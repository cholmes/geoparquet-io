"""Tests for `gpio pmtiles pyramid` (banded multi-level PMTiles archives)."""

import shutil
import sys

import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.pmtiles_pyramid import (
    TileJoinNotFoundError,
    _band_zoom_args,
    _build_tile_join_command,
    _feature_layer_name,
    _layer_name,
    _resolve_feature_zooms,
)
from geoparquet_io.core.process.overview.levels import Band


def has_tippecanoe():
    return shutil.which("tippecanoe") is not None


def has_tile_join():
    return shutil.which("tile-join") is not None


def has_gpio():
    return shutil.which("gpio") is not None


# Skip integration tests on Windows (tippecanoe has no native Windows build)
skip_windows = pytest.mark.skipif(
    sys.platform == "win32",
    reason="tippecanoe not available on Windows",
)


class TestTileJoinNotFoundError:
    def test_error_message_content(self):
        error = TileJoinNotFoundError()
        message = str(error)
        assert "tile-join not found" in message
        assert "tippecanoe" in message
        assert "brew install tippecanoe" in message


class TestLayerNaming:
    def test_single_mode_uses_output_stem(self):
        assert _layer_name("single", "a5", 6, "buildings") == "buildings"
        assert _feature_layer_name("single", "buildings") == "buildings"

    def test_grouped_mode(self):
        assert _layer_name("grouped", "a5", 6, "buildings") == "aggregate"
        assert _layer_name("grouped", "h3", 4, "buildings") == "aggregate"
        assert _feature_layer_name("grouped", "buildings") == "features"

    def test_per_level_mode_grid(self):
        assert _layer_name("per-level", "a5", 6, "buildings") == "r6"
        assert _layer_name("per-level", "h3", 10, "x") == "r10"

    def test_per_level_mode_admin(self):
        assert _layer_name("per-level", "admin", "country", "x") == "country"
        assert _layer_name("per-level", "admin", "region", "x") == "region"

    def test_per_level_features(self):
        assert _feature_layer_name("per-level", "x") == "features"


class TestBandZoomArgs:
    def test_bounded_band_passes_through(self):
        assert _band_zoom_args(Band(5, 2, 4), base_max=10) == (2, 4)

    def test_final_band_uses_base_max(self):
        assert _band_zoom_args(Band(9, 5, None), base_max=10) == (5, 10)

    def test_final_band_without_base_max(self):
        assert _band_zoom_args(Band(9, 5, None), base_max=None) == (5, None)


class TestFeatureZoomResolution:
    def test_features_min_defaults_to_base_max_plus_one(self):
        base_max, feat_min = _resolve_feature_zooms(
            max_zoom=8, features_min_zoom=None, include_features=True
        )
        assert (base_max, feat_min) == (8, 9)

    def test_base_max_derived_from_features_min(self):
        base_max, feat_min = _resolve_feature_zooms(
            max_zoom=None, features_min_zoom=9, include_features=True
        )
        assert (base_max, feat_min) == (8, 9)

    def test_no_features_passes_max_zoom_through(self):
        assert _resolve_feature_zooms(None, None, False) == (None, None)
        assert _resolve_feature_zooms(7, None, False) == (7, None)

    def test_features_without_any_zoom_errors(self):
        with pytest.raises(InvalidParameterError, match="features"):
            _resolve_feature_zooms(max_zoom=None, features_min_zoom=None, include_features=True)


class TestTileJoinCommand:
    def test_basic_command(self):
        cmd = _build_tile_join_command("out.pmtiles", ["a.pmtiles", "b.pmtiles"], name="out")
        assert cmd[0] == "tile-join"
        assert cmd[1:3] == ["-o", "out.pmtiles"]
        assert "-pk" in cmd
        assert "--name=out" in cmd
        assert cmd[-2:] == ["a.pmtiles", "b.pmtiles"]
        assert "--force" not in cmd

    def test_force_and_attribution(self):
        cmd = _build_tile_join_command(
            "out.pmtiles",
            ["a.pmtiles"],
            name="out",
            attribution="<a>me</a>",
            force=True,
        )
        assert "--force" in cmd
        assert "--attribution=<a>me</a>" in cmd


class TestParamValidation:
    def test_include_features_requires_source(self):
        from geoparquet_io.core.pmtiles_pyramid import create_pmtiles_pyramid

        with pytest.raises(InvalidParameterError, match="features.source|features_source"):
            create_pmtiles_pyramid("in.parquet", "out.pmtiles", include_features=True, max_zoom=8)

    def test_invalid_layer_mode(self):
        from geoparquet_io.core.pmtiles_pyramid import create_pmtiles_pyramid

        with pytest.raises(InvalidParameterError, match="layer_mode"):
            create_pmtiles_pyramid("in.parquet", "out.pmtiles", layer_mode="bogus")

    def test_dangerous_paths_rejected(self):
        from geoparquet_io.core.pmtiles_pyramid import create_pmtiles_pyramid

        with pytest.raises(ValueError, match="dangerous character"):
            create_pmtiles_pyramid("in.parquet", "out.pmtiles | cat")


class TestCli:
    def test_help(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["pmtiles", "pyramid", "--help"])
        assert result.exit_code == 0
        for opt in (
            "--levels",
            "--max-tile-kb",
            "--layer-mode",
            "--include-features",
            "--features-source",
            "--features-min-zoom",
            "--max-zoom",
        ):
            assert opt in result.output


# ---------------------------------------------------------------------------
# Integration (tippecanoe + tile-join + DuckDB community extensions)
# ---------------------------------------------------------------------------

_POINTS = [
    (2.35, 48.85, 4.0),
    (2.36, 48.86, 2.0),
    (13.40, 52.52, 1.5),
    (13.41, 52.53, 3.5),
    (-3.70, 40.42, 7.25),
    (30.0, -10.0, 5.0),
    (100.5, 13.75, 8.0),
    (-71.0, -35.0, 6.0),
]


def _write_points(path):
    import duckdb

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    values = ", ".join(f"({lon}, {lat}, {v})" for lon, lat, v in _POINTS)
    con.execute(
        f"""
        COPY (
            SELECT ST_Point(lon, lat) AS geometry, v
            FROM (VALUES {values}) AS t(lon, lat, v)
        ) TO '{path}' (FORMAT PARQUET)
        """
    )
    con.close()


def _read_pyramid_metadata(path):
    from pmtiles.reader import MmapSource, Reader

    with open(path, "rb") as f:
        reader = Reader(MmapSource(f))
        return reader.header(), reader.metadata()


integration = pytest.mark.skipif(
    not (has_gpio() and has_tippecanoe() and has_tile_join()),
    reason="requires gpio, tippecanoe, and tile-join on PATH",
)


@skip_windows
@integration
@pytest.mark.slow
@pytest.mark.network
def test_pyramid_end_to_end_grouped(tmp_path):
    """Aggregate -> pyramid: bands are pinned, joined, and recorded in metadata."""
    from geoparquet_io.core.pmtiles_pyramid import create_pmtiles_pyramid
    from geoparquet_io.core.process.aggregate.by_a5 import aggregate_by_a5

    src = tmp_path / "points.parquet"
    cells = tmp_path / "cells.parquet"
    out = tmp_path / "cells.pmtiles"
    _write_points(src)
    aggregate_by_a5(str(src), str(cells), resolution=7, metric="sum:v")

    # Absurd bytes-per-cell => nothing fits inside the probed range, so the
    # coarse level takes z0..max_zoom-1 and the base starts at max_zoom.
    create_pmtiles_pyramid(
        str(cells),
        str(out),
        levels=[5],
        bytes_per_cell=1e9,
        max_zoom=3,
    )

    assert out.exists() and out.stat().st_size > 0
    header, metadata = _read_pyramid_metadata(out)
    assert header["min_zoom"] == 0
    assert header["max_zoom"] == 3
    pyramid = metadata["gpio:pyramid"]
    assert pyramid["scheme"] == "a5"
    assert [band["level"] for band in pyramid["bands"]] == [5, 7]
    assert pyramid["bands"][0]["minzoom"] == 0
    assert pyramid["bands"][-1]["maxzoom"] == 3
    # Grouped (default) mode: all aggregate bands share one layer.
    layers = {layer["id"] for layer in metadata["vector_layers"]}
    assert layers == {"aggregate"}
    assert all(band["layer"] == "aggregate" for band in pyramid["bands"])


@skip_windows
@integration
@pytest.mark.slow
@pytest.mark.network
def test_pyramid_per_level_with_features(tmp_path):
    from geoparquet_io.core.pmtiles_pyramid import create_pmtiles_pyramid
    from geoparquet_io.core.process.aggregate.by_a5 import aggregate_by_a5

    src = tmp_path / "points.parquet"
    cells = tmp_path / "cells.parquet"
    out = tmp_path / "pyramid.pmtiles"
    _write_points(src)
    aggregate_by_a5(str(src), str(cells), resolution=7, metric="sum:v")

    create_pmtiles_pyramid(
        str(cells),
        str(out),
        levels=[5],
        bytes_per_cell=1e9,
        max_zoom=3,
        layer_mode="per-level",
        include_features=True,
        features_source=str(src),
    )

    header, metadata = _read_pyramid_metadata(out)
    pyramid = metadata["gpio:pyramid"]
    assert [band["layer"] for band in pyramid["bands"]] == ["r5", "r7", "features"]
    # Features band starts right after the base band.
    assert pyramid["bands"][-1]["minzoom"] == 4
    layers = {layer["id"] for layer in metadata["vector_layers"]}
    assert {"r5", "r7", "features"} <= layers
    assert header["min_zoom"] == 0
    assert header["max_zoom"] >= 4
