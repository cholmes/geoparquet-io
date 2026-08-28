#!/usr/bin/env python3
"""
Regenerate the canonical sample dataset used by docs, examples, and e2e tests.

Run from the repository root:

    uv run python tests/data/canonical/generate_canonical.py

Creates, in ``tests/data/canonical/``:

- ``places.parquet``   - 766 POINT features (from ``tests/data/places_test.parquet``)
- ``buildings.parquet`` - 42 POLYGON features (from ``tests/data/buildings_test.parquet``)
- ``places.geojson``   - GeoJSON FeatureCollection derived from ``places.parquet``
- ``places.csv``       - the same points as attributes plus ``lon``/``lat`` columns

and mirrors all four into ``examples/data/`` alongside the pre-existing
``sample.parquet`` (which this script never touches).

Everything is produced by gpio itself - the parquet files and the GeoJSON go
through the real CLI, always as ``python -m geoparquet_io.cli.main`` under the
interpreter running this script - so regenerating the dataset also smoke-tests
the gpio in this checkout (see ``_gpio_command`` for why never a PATH lookup). Only the lon/lat CSV needs a direct DuckDB query, because
``gpio convert csv`` emits WKT rather than coordinate columns.

The output is byte-reproducible: the CLI is deterministic for these inputs and
the DuckDB query runs single-threaded with insertion order preserved. Rerunning
this script on unchanged sources produces identical files.

``--output-dir DIR`` writes the dataset somewhere else and skips the
``examples/data/`` mirror. That is how ``tests/test_canonical_dataset.py``
regenerates into a temp directory and compares checksums against the committed
files, which turns "gpio changed and the dataset is no longer reproducible"
from a silent problem into a failing test.
"""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from pathlib import Path

CANONICAL_DIR = Path(__file__).resolve().parent
TEST_DATA_DIR = CANONICAL_DIR.parent
REPO_ROOT = TEST_DATA_DIR.parent.parent
EXAMPLES_DATA_DIR = REPO_ROOT / "examples" / "data"

# Source fixtures. These stay untouched; the canonical files are derived copies.
PLACES_SOURCE = TEST_DATA_DIR / "places_test.parquet"
BUILDINGS_SOURCE = TEST_DATA_DIR / "buildings_test.parquet"

PLACES_PARQUET = "places.parquet"
BUILDINGS_PARQUET = "buildings.parquet"
PLACES_GEOJSON = "places.geojson"
PLACES_CSV = "places.csv"

OUTPUT_FILENAMES = (PLACES_PARQUET, BUILDINGS_PARQUET, PLACES_GEOJSON, PLACES_CSV)

# GeoParquet 1.1 with a bbox covering and ZSTD is what `gpio check all` calls a
# clean file, and 1.1 (rather than 2.0) keeps the dataset readable by the widest
# range of clients the docs examples might be run against.
GEOPARQUET_VERSION = "1.1"
COMPRESSION = "zstd"


def _gpio_command() -> list[str]:
    """Always the module entry point of THIS interpreter, never a PATH lookup.

    Preferring an installed console script looks harmless but breaks the
    script's one guarantee. `uv tool install geoparquet-io` leaves a global
    executable that does not track the working tree, so a PATH lookup
    regenerates the dataset with whatever version happens to be installed —
    and the reproducibility test in tests/test_canonical_dataset.py, which
    compares regenerated SHA-256s against the committed files, would then pass
    while the repo's own output had drifted (stale global matching the
    committed bytes) or fail while the repo was fine. Pinning to
    sys.executable makes "regenerating also smoke-tests gpio" mean the gpio in
    this checkout, in every environment, including CI where no console script
    exists at all.
    """
    return [sys.executable, "-m", "geoparquet_io.cli.main"]


def run_gpio(*args: str) -> None:
    """Build the dataset with the real CLI, so regenerating it smoke-tests gpio."""
    print(f"  $ gpio {' '.join(args)}")
    subprocess.run([*_gpio_command(), *args], check=True, cwd=REPO_ROOT)


def output_paths(output_dir: Path) -> dict[str, Path]:
    """Map each canonical filename to its destination under ``output_dir``."""
    return {name: output_dir / name for name in OUTPUT_FILENAMES}


def _hilbert_sort(source: Path, destination: Path) -> None:
    """Sort into a spatially ordered, bbox-covered, ZSTD GeoParquet 1.1 file."""
    run_gpio(
        "sort",
        "hilbert",
        str(source),
        str(destination),
        "--add-bbox",
        "--geoparquet-version",
        GEOPARQUET_VERSION,
        "--compression",
        COMPRESSION,
        "--overwrite",
    )


def generate_points(paths: dict[str, Path]) -> None:
    """Hilbert-sort the places extract into the canonical point file."""
    print("Generating places.parquet (766 POINT features)...")
    _hilbert_sort(PLACES_SOURCE, paths[PLACES_PARQUET])


def generate_polygons(paths: dict[str, Path]) -> None:
    """Same treatment for the buildings sample."""
    print("Generating buildings.parquet (42 POLYGON features)...")
    _hilbert_sort(BUILDINGS_SOURCE, paths[BUILDINGS_PARQUET])


def generate_geojson(paths: dict[str, Path]) -> None:
    """Derive the GeoJSON FeatureCollection from the canonical point file."""
    print("Generating places.geojson...")
    run_gpio(
        "convert",
        "geojson",
        str(paths[PLACES_PARQUET]),
        str(paths[PLACES_GEOJSON]),
        "--overwrite",
    )


def generate_csv(paths: dict[str, Path]) -> None:
    """Derive a lon/lat CSV - the shape `gpio convert geoparquet` auto-detects.

    ``gpio convert csv`` writes WKT plus the bbox struct, which is not what a
    "plain CSV of points" looks like in the docs, so this goes straight to
    DuckDB. Full double precision is kept so the CSV round-trips exactly.
    """
    print("Generating places.csv (lon/lat columns)...")
    from geoparquet_io.core.duckdb_utils import _escape_sql_string, get_duckdb_connection

    source = paths[PLACES_PARQUET]
    destination = paths[PLACES_CSV]

    con = get_duckdb_connection(threads=1)
    try:
        con.execute("SET preserve_insertion_order = true")
        # COPY ... TO does not accept a prepared parameter for its destination,
        # so both paths are inlined. The destination derives from --output-dir,
        # which makes it user input; escape both like any SQL string literal.
        con.execute(f"""
            COPY (
                SELECT
                    fsq_place_id,
                    name,
                    address,
                    placemaker_url,
                    ST_X(geometry) AS lon,
                    ST_Y(geometry) AS lat
                FROM read_parquet('{_escape_sql_string(str(source))}')
            ) TO '{_escape_sql_string(str(destination))}' (FORMAT CSV, HEADER)
        """)
    finally:
        con.close()
    print(f"  ✓ Created {destination.name}")


def mirror_to_examples(paths: dict[str, Path]) -> None:
    """Copy the canonical files into examples/data/ for notebooks and docs.

    ``examples/data/sample.parquet`` is deliberately left in place: notebooks
    still reference it, and migrating them is a separate change.
    """
    print("Mirroring into examples/data/...")
    EXAMPLES_DATA_DIR.mkdir(parents=True, exist_ok=True)
    for name, source in paths.items():
        shutil.copyfile(source, EXAMPLES_DATA_DIR / name)
        print(f"  ✓ {name}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments.

    The ``examples/data/`` mirror is only refreshed when the dataset is written
    to its committed home. Regenerating elsewhere - which is what the drift
    guard in ``tests/test_canonical_dataset.py`` does - must not touch the
    repository, so ``--output-dir`` implies no mirroring.
    """
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[1])
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=CANONICAL_DIR,
        help=(
            "Directory to write the dataset into "
            "(default: tests/data/canonical/). Any other directory also skips "
            "the examples/data/ mirror."
        ),
    )
    args = parser.parse_args(argv)
    args.output_dir = args.output_dir.resolve()
    args.mirror = args.output_dir == CANONICAL_DIR
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)

    missing = [p for p in (PLACES_SOURCE, BUILDINGS_SOURCE) if not p.exists()]
    if missing:
        for path in missing:
            print(f"ERROR: source fixture not found: {path}", file=sys.stderr)
        return 1

    args.output_dir.mkdir(parents=True, exist_ok=True)
    paths = output_paths(args.output_dir)

    generate_points(paths)
    generate_polygons(paths)
    generate_geojson(paths)
    generate_csv(paths)

    if args.mirror:
        mirror_to_examples(paths)
    else:
        print(f"Wrote to {args.output_dir}; skipping the examples/data/ mirror.")

    total = sum(path.stat().st_size for path in paths.values())
    print(f"\nDone. Canonical dataset is {total / 1024:.1f} KB across 4 files.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
