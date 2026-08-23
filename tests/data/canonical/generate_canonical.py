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
through the real CLI (``python -m geoparquet_io.cli.main``), so regenerating the
dataset also smoke-tests the tool. Only the lon/lat CSV needs a direct DuckDB
query, because ``gpio convert csv`` emits WKT rather than coordinate columns.

The output is byte-reproducible: the CLI is deterministic for these inputs and
the DuckDB query runs single-threaded with insertion order preserved. Rerunning
this script on unchanged sources produces identical files.
"""

from __future__ import annotations

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

PLACES_PARQUET = CANONICAL_DIR / "places.parquet"
BUILDINGS_PARQUET = CANONICAL_DIR / "buildings.parquet"
PLACES_GEOJSON = CANONICAL_DIR / "places.geojson"
PLACES_CSV = CANONICAL_DIR / "places.csv"

MIRRORED_FILES = (PLACES_PARQUET, BUILDINGS_PARQUET, PLACES_GEOJSON, PLACES_CSV)

# GeoParquet 1.1 with a bbox covering and ZSTD is what `gpio check all` calls a
# clean file, and 1.1 (rather than 2.0) keeps the dataset readable by the widest
# range of clients the docs examples might be run against.
GEOPARQUET_VERSION = "1.1"
COMPRESSION = "zstd"


def _gpio_command() -> list[str]:
    """Prefer the installed console script; fall back to the module entry point."""
    executable = shutil.which("gpio")
    if executable:
        return [executable]
    return [sys.executable, "-m", "geoparquet_io.cli.main"]


def run_gpio(*args: str) -> None:
    """Build the dataset with the real CLI, so regenerating it smoke-tests gpio."""
    print(f"  $ gpio {' '.join(args)}")
    subprocess.run([*_gpio_command(), *args], check=True, cwd=REPO_ROOT)


def generate_points() -> None:
    """Hilbert-sort the places extract into a spatially ordered, bbox-covered file."""
    print("Generating places.parquet (766 POINT features)...")
    run_gpio(
        "sort",
        "hilbert",
        str(PLACES_SOURCE),
        str(PLACES_PARQUET),
        "--add-bbox",
        "--geoparquet-version",
        GEOPARQUET_VERSION,
        "--compression",
        COMPRESSION,
        "--overwrite",
    )


def generate_polygons() -> None:
    """Same treatment for the buildings sample."""
    print("Generating buildings.parquet (42 POLYGON features)...")
    run_gpio(
        "sort",
        "hilbert",
        str(BUILDINGS_SOURCE),
        str(BUILDINGS_PARQUET),
        "--add-bbox",
        "--geoparquet-version",
        GEOPARQUET_VERSION,
        "--compression",
        COMPRESSION,
        "--overwrite",
    )


def generate_geojson() -> None:
    """Derive the GeoJSON FeatureCollection from the canonical point file."""
    print("Generating places.geojson...")
    run_gpio(
        "convert",
        "geojson",
        str(PLACES_PARQUET),
        str(PLACES_GEOJSON),
        "--overwrite",
    )


def generate_csv() -> None:
    """Derive a lon/lat CSV - the shape `gpio convert geoparquet` auto-detects.

    ``gpio convert csv`` writes WKT plus the bbox struct, which is not what a
    "plain CSV of points" looks like in the docs, so this goes straight to
    DuckDB. Full double precision is kept so the CSV round-trips exactly.
    """
    print("Generating places.csv (lon/lat columns)...")
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection

    con = get_duckdb_connection(threads=1)
    try:
        con.execute("SET preserve_insertion_order = true")
        # COPY ... TO does not accept a prepared parameter for its destination,
        # so both paths are inlined. They are script constants, not user input.
        con.execute(f"""
            COPY (
                SELECT
                    fsq_place_id,
                    name,
                    address,
                    placemaker_url,
                    ST_X(geometry) AS lon,
                    ST_Y(geometry) AS lat
                FROM read_parquet('{PLACES_PARQUET}')
            ) TO '{PLACES_CSV}' (FORMAT CSV, HEADER)
        """)
    finally:
        con.close()
    print(f"  ✓ Created {PLACES_CSV.name}")


def mirror_to_examples() -> None:
    """Copy the canonical files into examples/data/ for notebooks and docs.

    ``examples/data/sample.parquet`` is deliberately left in place: notebooks
    still reference it, and migrating them is a separate change.
    """
    print("Mirroring into examples/data/...")
    EXAMPLES_DATA_DIR.mkdir(parents=True, exist_ok=True)
    for source in MIRRORED_FILES:
        shutil.copyfile(source, EXAMPLES_DATA_DIR / source.name)
        print(f"  ✓ {source.name}")


def main() -> int:
    missing = [p for p in (PLACES_SOURCE, BUILDINGS_SOURCE) if not p.exists()]
    if missing:
        for path in missing:
            print(f"ERROR: source fixture not found: {path}", file=sys.stderr)
        return 1

    CANONICAL_DIR.mkdir(parents=True, exist_ok=True)

    generate_points()
    generate_polygons()
    generate_geojson()
    generate_csv()
    mirror_to_examples()

    total = sum(path.stat().st_size for path in MIRRORED_FILES)
    print(f"\nDone. Canonical dataset is {total / 1024:.1f} KB across 4 files.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
