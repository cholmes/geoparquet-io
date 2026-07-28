"""PMTiles pyramid generation: banded multi-level archives via tile-join.

Builds one tippecanoe run per aggregate level, each pinned to the zoom band
selected by the shared overview level-selection core, then merges the per-band
archives into a single PMTiles file with ``tile-join`` and records the bands
under a ``gpio:pyramid`` key in the archive's metadata JSON.
"""

from __future__ import annotations

import gc
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

from geoparquet_io.core.duckdb_utils import get_duckdb_connection
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.file_utils import safe_file_url
from geoparquet_io.core.logging_config import configure_verbose, debug, success
from geoparquet_io.core.pmtiles import (
    TippecanoeNotFoundError,
    _check_tippecanoe,
    _validate_path,
    create_pmtiles_from_geoparquet,
)
from geoparquet_io.core.process.overview.detect import AggregateInfo, detect_aggregate_info
from geoparquet_io.core.process.overview.levels import Band
from geoparquet_io.core.process.overview.run import (
    create_overviews,
    overview_output_path,
    parse_levels,
    plan_bands,
)

VALID_LAYER_MODES = ("single", "grouped", "per-level")


class TileJoinNotFoundError(Exception):
    """Raised when tile-join is not found in PATH."""

    def __init__(self):
        super().__init__(
            "tile-join not found in PATH.\n\n"
            "tile-join ships with tippecanoe; gpio pmtiles pyramid needs it to\n"
            "merge the per-level archives. Install tippecanoe:\n"
            "  macOS:  brew install tippecanoe\n"
            "  Ubuntu: sudo apt install tippecanoe\n"
            "  Source: https://github.com/felt/tippecanoe#installation"
        )


def _check_tile_join() -> bool:
    """Check if tile-join is available in PATH."""
    return shutil.which("tile-join") is not None


def _layer_name(layer_mode: str, scheme: str, level: int | str, stem: str) -> str:
    """Layer name for an aggregate band under the given --layer-mode."""
    if layer_mode == "single":
        return stem
    if layer_mode == "grouped":
        return "aggregate"
    return str(level) if scheme == "admin" else f"r{level}"


def _feature_layer_name(layer_mode: str, stem: str) -> str:
    """Layer name for the raw-features band under the given --layer-mode."""
    return stem if layer_mode == "single" else "features"


def _band_zoom_args(band: Band, base_max: int | None) -> tuple[int, int | None]:
    """(min_zoom, max_zoom) tippecanoe pinning for a band; the open-ended final
    band inherits the archive-level max zoom (None -> tippecanoe guesses)."""
    return band.minzoom, band.maxzoom if band.maxzoom is not None else base_max


def _resolve_feature_zooms(
    max_zoom: int | None, features_min_zoom: int | None, include_features: bool
) -> tuple[int | None, int | None]:
    """Resolve (base_band_max_zoom, features_min_zoom).

    The features band starts one past the base band, so with features enabled
    one of the two zoom anchors must be given; each derives the other.
    """
    if not include_features:
        return max_zoom, None
    if features_min_zoom is None:
        if max_zoom is None:
            raise InvalidParameterError(
                "features_min_zoom",
                "--include-features needs --features-min-zoom or --max-zoom "
                "to know where the features band starts",
            )
        return max_zoom, max_zoom + 1
    if features_min_zoom < 1:
        raise InvalidParameterError(
            "features_min_zoom",
            f"got {features_min_zoom}; must be >= 1 so the aggregate bands keep at least zoom 0",
        )
    if max_zoom is None:
        return features_min_zoom - 1, features_min_zoom
    if features_min_zoom <= max_zoom:
        raise InvalidParameterError(
            "features_min_zoom",
            f"got {features_min_zoom}; must be greater than --max-zoom "
            f"({max_zoom}) so the features band does not overlap the "
            "aggregate bands",
        )
    return max_zoom, features_min_zoom


def _build_tile_join_command(
    output_path: str,
    band_files: list[str],
    *,
    name: str,
    attribution: str | None = None,
    force: bool = False,
) -> list[str]:
    """argv for merging per-band archives (never joined through a shell)."""
    cmd = ["tile-join", "-o", output_path, "-pk"]
    if force:
        cmd.append("--force")
    cmd.append(f"--name={name}")
    if attribution:
        cmd.append(f"--attribution={attribution}")
    cmd.extend(band_files)
    return cmd


def _run_tile_join(cmd: list[str], verbose: bool) -> None:
    if verbose:
        debug(f"Running: {' '.join(cmd)}")
    proc = subprocess.run(cmd, capture_output=True, text=True, check=False)
    if proc.returncode != 0:
        stderr = proc.stderr.strip()
        raise RuntimeError(
            f"tile-join failed with exit code {proc.returncode}"
            + (f"\nstderr:\n{stderr}" if stderr else "")
        )
    if verbose and proc.stderr.strip():
        debug(proc.stderr.strip())


def _merge_pyramid_metadata(pmtiles_path: str, pyramid: dict) -> None:
    """Rewrite the archive with ``gpio:pyramid`` merged into its metadata JSON.

    Full read/write round-trip via the pmtiles package (tile data is copied
    verbatim; the writer recomputes header offsets and zoom bounds).

    The archive is read fully into memory first so that no handle on either
    file remains open when ``os.replace`` runs. In particular, pmtiles'
    ``MmapSource`` must NOT be used here: it wraps the file in an ``mmap``
    that lives on inside the reader's ``get_bytes`` closure with no way to
    close it, and on Windows an open memory map keeps the file locked (even
    after the file object itself is closed), failing the replace with
    WinError 5/32.
    """
    from pmtiles.reader import MemorySource, Reader, all_tiles
    from pmtiles.tile import zxy_to_tileid
    from pmtiles.writer import Writer

    with open(pmtiles_path, "rb") as source_file:
        archive_bytes = source_file.read()
    # No handles on the original archive remain past this point.

    source = MemorySource(archive_bytes)
    reader = Reader(source)
    header = reader.header()
    metadata = reader.metadata()
    metadata["gpio:pyramid"] = pyramid

    # Unpredictable temp name in the output dir (same filesystem for the
    # atomic replace; not symlink-followable like a fixed "<output>.meta.tmp").
    tmp = tempfile.NamedTemporaryFile(
        dir=os.path.dirname(os.path.abspath(pmtiles_path)),
        prefix=Path(pmtiles_path).name + ".",
        suffix=".meta.tmp",
        delete=False,
    )
    try:
        with tmp:
            writer = Writer(tmp)
            for zxy, data in all_tiles(source):
                writer.write_tile(zxy_to_tileid(*zxy), data)
            writer.finalize(header, metadata)
        os.replace(tmp.name, pmtiles_path)
    except BaseException:
        Path(tmp.name).unlink(missing_ok=True)
        raise


def _resolve_band_sources(
    input_path: str,
    info: AggregateInfo,
    bands: list[Band],
    tmpdir: str,
    verbose: bool,
) -> dict[int | str, str]:
    """Map each band level to a GeoParquet source.

    The base band uses the input itself. Overview levels use existing sibling
    files from `gpio process overview` when present; missing ones are built
    into ``tmpdir``.
    """
    sources: dict[int | str, str] = {info.base_level: input_path}
    missing: list[int | str] = []
    for band in bands:
        if band.level == info.base_level:
            continue
        sibling = overview_output_path(input_path, info.scheme, band.level)
        if Path(sibling).exists():
            debug(f"Using existing overview for level {band.level}: {sibling}")
            sources[band.level] = sibling
        else:
            missing.append(band.level)
    if missing:
        built = create_overviews(input_path, levels=missing, output_dir=tmpdir, verbose=verbose)
        sources.update(dict(built))
    return sources


def _pyramid_metadata(
    info: AggregateInfo,
    bands: list[Band],
    base_max: int | None,
    layer_mode: str,
    stem: str,
    features_min_zoom: int | None,
) -> dict:
    entries = []
    for band in bands:
        minzoom, maxzoom = _band_zoom_args(band, base_max)
        entries.append(
            {
                "level": band.level,
                "layer": _layer_name(layer_mode, info.scheme, band.level, stem),
                "minzoom": minzoom,
                "maxzoom": maxzoom,
            }
        )
    if features_min_zoom is not None:
        entries.append(
            {
                "level": "features",
                "layer": _feature_layer_name(layer_mode, stem),
                "minzoom": features_min_zoom,
                "maxzoom": None,
            }
        )
    return {"scheme": info.scheme, "bands": entries}


def _detect_and_plan(
    input_path: str,
    levels: str | list[int | str] | None,
    max_tile_kb: int,
    bytes_per_cell: float | None,
    base_max_zoom: int | None,
    verbose: bool,
) -> tuple[AggregateInfo, list[Band]]:
    """Detect the aggregate's shape and select zoom bands for the pyramid."""
    input_url = safe_file_url(input_path, verbose)
    con = get_duckdb_connection(load_spatial=True, load_httpfs=True)
    try:
        con.execute("SET geometry_always_xy = true")
        relation = f"read_parquet('{input_url}', hive_partitioning=false, union_by_name=true)"
        info = detect_aggregate_info(con, relation)
        if info.out_geometry == "none":
            raise InvalidParameterError(
                "input",
                "aggregate has no geometry column; re-run gpio process aggregate "
                "with --out-geometry polygon or centroid before tiling",
            )
        parsed_levels = parse_levels(levels, info) if levels is not None else None
        # Cap band transitions one below the base band's max zoom so the base
        # band starts by that zoom (and never inverts into minzoom > maxzoom).
        max_probe = base_max_zoom - 1 if base_max_zoom is not None else None
        bands = plan_bands(
            con,
            f"SELECT * FROM {relation}",
            info,
            levels=parsed_levels,
            max_tile_kb=max_tile_kb,
            bytes_per_cell=bytes_per_cell,
            verbose=verbose,
            max_probe_zoom=max_probe,
        )
        return info, bands
    finally:
        con.close()
        # Release GDAL/spatial native handles before the next spatial connection
        # opens; leaked native state can segfault sibling xdist tests.
        gc.collect()


def create_pmtiles_pyramid(
    input_path: str,
    output_path: str,
    *,
    levels=None,
    max_tile_kb: int = 500,
    bytes_per_cell: float | None = None,
    layer_mode: str = "grouped",
    include_features: bool = False,
    features_source: str | None = None,
    features_min_zoom: int | None = None,
    max_zoom: int | None = None,
    attribution: str | None = None,
    force: bool = False,
    verbose: bool = False,
) -> None:
    """Create a banded multi-level PMTiles archive from an aggregate file.

    Detects the aggregate's scheme and base level, selects zoom bands (shared
    core with `gpio process overview`), runs tippecanoe once per level pinned
    to its band, merges the results with tile-join, and records the bands in
    the archive metadata under ``gpio:pyramid``. Existing overview siblings
    (``cells_r5.parquet``) are reused; missing levels are built into a temp
    directory.

    Args:
        input_path: Path to a `gpio process aggregate` output (GeoParquet).
        output_path: Path for the output PMTiles archive.
        levels: Explicit overview levels (comma string or list); default
            auto-selects against ``max_tile_kb``.
        max_tile_kb: Tile-size budget in KB for band selection (default 500).
        bytes_per_cell: Override the estimated compressed bytes per cell.
        layer_mode: ``single`` (one layer named after the output), ``grouped``
            (``aggregate`` + ``features``), or ``per-level`` (``r5``/``r10`` or
            ``country``/``region`` + ``features``).
        include_features: Append the original features as the final band.
        features_source: GeoParquet source for the features band.
        features_min_zoom: First zoom of the features band (default: base band
            max zoom + 1).
        max_zoom: Max zoom of the base aggregate band (tippecanoe guesses when
            omitted and no features band is requested).
        attribution: Attribution HTML for the tiles.
        force: Overwrite the output archive if it exists.
        verbose: Enable verbose output.

    Raises:
        TippecanoeNotFoundError: tippecanoe missing from PATH.
        TileJoinNotFoundError: tile-join missing from PATH.
        InvalidParameterError: invalid parameter combinations or input shape.
        RuntimeError: a tippecanoe or tile-join run failed.
    """
    configure_verbose(verbose)
    _validate_path(input_path)
    _validate_path(output_path)
    if layer_mode not in VALID_LAYER_MODES:
        raise InvalidParameterError(
            "layer_mode",
            f"invalid value '{layer_mode}'. Valid: {', '.join(VALID_LAYER_MODES)}",
        )
    if include_features and not features_source:
        raise InvalidParameterError(
            "features_source", "--include-features requires --features-source"
        )
    if features_source:
        _validate_path(features_source)
    base_max, feat_min = _resolve_feature_zooms(max_zoom, features_min_zoom, include_features)
    if not _check_tippecanoe():
        raise TippecanoeNotFoundError()
    if not _check_tile_join():
        raise TileJoinNotFoundError()

    info, bands = _detect_and_plan(
        input_path, levels, max_tile_kb, bytes_per_cell, base_max, verbose
    )
    stem = Path(output_path).stem

    with tempfile.TemporaryDirectory() as tmpdir:
        sources = _resolve_band_sources(input_path, info, bands, tmpdir, verbose)
        band_files: list[str] = []
        for i, band in enumerate(bands):
            min_zoom, band_max = _band_zoom_args(band, base_max)
            band_file = os.path.join(tmpdir, f"band_{i}.pmtiles")
            debug(
                f"Tiling level {band.level} for zooms {min_zoom}.."
                f"{band_max if band_max is not None else 'auto'}"
            )
            create_pmtiles_from_geoparquet(
                sources[band.level],
                band_file,
                layer=_layer_name(layer_mode, info.scheme, band.level, stem),
                min_zoom=min_zoom,
                max_zoom=band_max,
                no_tile_size_limit=True,
                attribution=attribution,
                force=True,
                verbose=verbose,
            )
            band_files.append(band_file)

        if include_features:
            assert features_source is not None and feat_min is not None
            features_file = os.path.join(tmpdir, "band_features.pmtiles")
            debug(f"Tiling raw features from zoom {feat_min}")
            create_pmtiles_from_geoparquet(
                features_source,
                features_file,
                layer=_feature_layer_name(layer_mode, stem),
                min_zoom=feat_min,
                max_zoom=None,
                # Real feature data can be arbitrarily dense: keep tippecanoe's
                # size cap so --drop-densest-as-needed has a limit to enforce.
                no_tile_size_limit=False,
                drop_densest_as_needed=True,
                attribution=attribution,
                force=True,
                verbose=verbose,
            )
            band_files.append(features_file)

        _run_tile_join(
            _build_tile_join_command(
                output_path, band_files, name=stem, attribution=attribution, force=force
            ),
            verbose,
        )

    _merge_pyramid_metadata(
        output_path,
        _pyramid_metadata(info, bands, base_max, layer_mode, stem, feat_min),
    )
    success(f"Created {output_path} with {len(bands)} aggregate band(s)")
