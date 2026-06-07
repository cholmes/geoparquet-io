"""
Pytest configuration and shared fixtures for geoparquet-io tests.

DuckDB Thread Limiting for Parallel Test Execution
--------------------------------------------------
Problem: DuckDB uses all CPU cores by default. With pytest-xdist running
multiple workers (e.g., -n 4), each worker creates multiple DuckDB connections,
leading to thread explosion: 4 workers × N connections × 16 threads = CPU saturation.

Solution: Monkeypatch duckdb.connect() BEFORE any modules import duckdb.
conftest.py is loaded before test collection, so we patch immediately at import.
With 4 workers and 2 threads per connection, max threads = 4 × 2 = 8.
"""

# ---------------------------------------------------------------------------
# CRITICAL: Patch duckdb.connect BEFORE any other imports
# This must happen before geoparquet_io modules are imported during collection
# ---------------------------------------------------------------------------
import duckdb

_DUCKDB_TEST_THREADS = 2  # Threads per DuckDB connection during tests
_original_duckdb_connect = duckdb.connect


def _thread_limited_connect(*args, **kwargs):
    """Wrapper around duckdb.connect that limits threads for test performance."""
    config = kwargs.pop("config", {}) or {}
    if "threads" not in config:
        config["threads"] = _DUCKDB_TEST_THREADS
    return _original_duckdb_connect(*args, config=config, **kwargs)


# Apply the monkeypatch globally at import time - BEFORE other imports
duckdb.connect = _thread_limited_connect

# ---------------------------------------------------------------------------
# Now import everything else (they'll get the patched duckdb.connect)
# noqa: E402 - Intentionally importing after duckdb patch
# ---------------------------------------------------------------------------
import json  # noqa: E402
import os  # noqa: E402
import shutil  # noqa: E402
import tempfile  # noqa: E402
import time  # noqa: E402
from contextlib import contextmanager  # noqa: E402
from pathlib import Path  # noqa: E402

import pyarrow.parquet as pq  # noqa: E402
import pytest  # noqa: E402

# Test data directory
TEST_DATA_DIR = Path(__file__).parent / "data"
PLACES_TEST_FILE = TEST_DATA_DIR / "places_test.parquet"
BUILDINGS_TEST_FILE = TEST_DATA_DIR / "buildings_test.parquet"
COUNTRY_PARTITION_DIR = TEST_DATA_DIR / "country_partition"


@pytest.fixture
def test_data_dir():
    """Return the path to the test data directory."""
    return TEST_DATA_DIR


@pytest.fixture
def places_test_file():
    """Return the path to the places test parquet file."""
    return str(PLACES_TEST_FILE)


@pytest.fixture
def buildings_test_file():
    """Return the path to the buildings test parquet file."""
    return str(BUILDINGS_TEST_FILE)


@pytest.fixture
def temp_output_dir():
    """Create a temporary directory for test outputs."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    # Cleanup after test
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def temp_output_file(temp_output_dir):
    """Create a temporary output file path."""
    return os.path.join(temp_output_dir, "output.parquet")


def _write_with_crs_state(source_file, dest_file, crs_state):
    """Write a copy of source_file with the geometry column's crs key adjusted.

    crs_state: "null" sets crs to None (explicit unknown CRS); "absent" removes
    the crs key entirely (defaults to OGC:CRS84 per the GeoParquet spec).
    """
    table = pq.read_table(source_file)
    metadata = dict(table.schema.metadata or {})
    geo = json.loads(metadata[b"geo"].decode("utf-8"))
    primary = geo.get("primary_column", "geometry")
    col_meta = geo["columns"][primary]
    if crs_state == "null":
        col_meta["crs"] = None
    elif crs_state == "absent":
        col_meta.pop("crs", None)
    metadata[b"geo"] = json.dumps(geo).encode("utf-8")
    table = table.replace_schema_metadata(metadata)
    pq.write_table(table, dest_file)
    return dest_file


@pytest.fixture
def null_crs_parquet(tmp_path):
    """A GeoParquet file with an explicit ``"crs": null`` (unknown CRS)."""
    return _write_with_crs_state(
        str(BUILDINGS_TEST_FILE), str(tmp_path / "null_crs.parquet"), "null"
    )


@pytest.fixture
def absent_crs_parquet(tmp_path):
    """A GeoParquet file with the crs key omitted (defaults to OGC:CRS84)."""
    return _write_with_crs_state(
        str(BUILDINGS_TEST_FILE), str(tmp_path / "absent_crs.parquet"), "absent"
    )


@contextmanager
def duckdb_connection():
    """
    Context manager for DuckDB connections that ensures proper cleanup.

    Useful for tests to avoid Windows file locking issues.
    """
    con = duckdb.connect()
    try:
        con.execute("INSTALL spatial;")
        con.execute("LOAD spatial;")
        yield con
    finally:
        con.close()


# Windows-safe cleanup helpers


def safe_unlink(file_path, retries=5, delay=0.1):
    """
    Safely unlink a file with retries for Windows compatibility.

    On Windows, file handles may not be released immediately, causing
    PermissionError. This function retries the unlink operation.

    Args:
        file_path: Path to the file (str or Path)
        retries: Number of retry attempts
        delay: Delay between retries in seconds
    """
    path = Path(file_path) if not isinstance(file_path, Path) else file_path
    if not path.exists():
        return

    for attempt in range(retries):
        try:
            path.unlink()
            return
        except (PermissionError, FileNotFoundError):
            if attempt < retries - 1:
                time.sleep(delay)
            # Ignore final failure - cleanup is best effort


def safe_rmtree(dir_path, retries=5, delay=0.1):
    """
    Safely remove a directory tree with retries for Windows compatibility.

    On Windows, file handles may not be released immediately, causing
    PermissionError or OSError. This function retries the rmtree operation.

    Args:
        dir_path: Path to the directory (str or Path)
        retries: Number of retry attempts
        delay: Delay between retries in seconds
    """
    path = Path(dir_path) if not isinstance(dir_path, Path) else dir_path
    if not path.exists():
        return

    for attempt in range(retries):
        try:
            shutil.rmtree(path)
            return
        except (PermissionError, OSError):
            if attempt < retries - 1:
                time.sleep(delay)
            # Ignore final failure - cleanup is best effort


# Helper functions for GeoParquet version testing


def get_geoparquet_version(parquet_file):
    """
    Extract GeoParquet version from file metadata.

    Args:
        parquet_file: Path to the parquet file

    Returns:
        str: GeoParquet version string (e.g., "1.0.0", "1.1.0", "2.0.0") or None
    """
    pf = pq.ParquetFile(parquet_file)
    metadata = pf.schema_arrow.metadata
    if metadata and b"geo" in metadata:
        geo_meta = json.loads(metadata[b"geo"].decode("utf-8"))
        return geo_meta.get("version")
    return None


def has_native_geo_types(parquet_file):
    """
    Check if file uses Parquet GEOMETRY/GEOGRAPHY logical types.

    Args:
        parquet_file: Path to the parquet file

    Returns:
        bool: True if file has native Parquet geo types
    """
    pf = pq.ParquetFile(parquet_file)
    schema_str = str(pf.metadata.schema)
    return "Geometry" in schema_str or "Geography" in schema_str


def has_geoparquet_metadata(parquet_file):
    """
    Check if file has 'geo' metadata key (GeoParquet metadata).

    Args:
        parquet_file: Path to the parquet file

    Returns:
        bool: True if file has GeoParquet metadata
    """
    pf = pq.ParquetFile(parquet_file)
    metadata = pf.schema_arrow.metadata
    return metadata is not None and b"geo" in metadata


def get_geo_metadata(parquet_file):
    """
    Get the full GeoParquet metadata from a file.

    Args:
        parquet_file: Path to the parquet file

    Returns:
        dict: GeoParquet metadata or None
    """
    pf = pq.ParquetFile(parquet_file)
    metadata = pf.schema_arrow.metadata
    if metadata and b"geo" in metadata:
        return json.loads(metadata[b"geo"].decode("utf-8"))
    return None


# Test data file fixtures
@pytest.fixture
def fields_v2_file(test_data_dir):
    """Return path to the GeoParquet 2.0 test file (CRS84, ZSTD)."""
    return str(test_data_dir / "fields_gpq2_crs84_zstd.parquet")


@pytest.fixture
def fields_geom_type_only_file(test_data_dir):
    """Return path to the Parquet Geo Only test file (CRS84, with bbox, SNAPPY)."""
    return str(test_data_dir / "fields_pgo_crs84_bbox_snappy.parquet")


@pytest.fixture
def fields_geom_type_only_5070_file(test_data_dir):
    """Return path to the Parquet Geo Only test file (EPSG:5070, SNAPPY)."""
    return str(test_data_dir / "fields_pgo_5070_snappy.parquet")


@pytest.fixture
def austria_bbox_covering_file(test_data_dir):
    """Return path to the austria_bbox_covering.parquet test file.

    This file has a non-standard bbox column name ('geometry_bbox')
    that is properly registered in the GeoParquet covering metadata.
    """
    return str(test_data_dir / "austria_bbox_covering.parquet")


@pytest.fixture
def geojson_input(test_data_dir):
    """Return path to the buildings_test.geojson test file."""
    return str(test_data_dir / "buildings_test.geojson")


@pytest.fixture
def gpkg_buildings(test_data_dir):
    """Return path to the buildings_test.gpkg test file."""
    return str(test_data_dir / "buildings_test.gpkg")


@pytest.fixture
def buildings_gpkg_6933(test_data_dir):
    """Return path to the buildings_test_6933.gpkg test file (EPSG:6933)."""
    return str(test_data_dir / "buildings_test_6933.gpkg")


@pytest.fixture
def shapefile_buildings(test_data_dir):
    """Return path to the buildings_test.shp test file."""
    return str(test_data_dir / "buildings_test.shp")


@pytest.fixture
def csv_points_wkt(test_data_dir):
    """Return path to the points_wkt.csv test file."""
    return str(test_data_dir / "points_wkt.csv")


@pytest.fixture
def fields_5070_file(test_data_dir):
    """Return path to the Parquet Geo Only test file (EPSG:5070, SNAPPY)."""
    return str(test_data_dir / "fields_pgo_5070_snappy.parquet")


@pytest.fixture
def unsorted_test_file(test_data_dir):
    """Return path to the unsorted.parquet test file (poor spatial ordering)."""
    return str(test_data_dir / "unsorted.parquet")


@pytest.fixture
def country_partition_dir():
    """Return path to the country partition test directory.

    This directory contains 4 parquet files representing a flat partition:
    - El_Salvador.parquet
    - Guatemala.parquet
    - Honduras.parquet
    - Nicaragua.parquet

    All files have the same schema and GeoParquet 1.1.0 metadata.
    Total: ~5000 rows across 4 files.
    """
    return str(COUNTRY_PARTITION_DIR)


# Helper functions for CLI output parsing


def _extract_json_from_output(output: str) -> str:
    """Extract JSON from output that may contain warnings or other text.

    Some commands (e.g., deprecated ones) output warning lines before JSON.
    This helper finds and returns just the JSON part.

    Handles JSON that starts with '{', '[', or is the literal 'null'.
    """
    lines = output.strip().split("\n")
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("{") or stripped.startswith("[") or stripped == "null":
            return "\n".join(lines[i:])
    # If no JSON found, return original output
    return output


# CRS reference format test files
@pytest.fixture
def crs_projjson_file(test_data_dir):
    """Return path to parquet file with projjson: CRS reference format.

    This file has a GEOMETRY column with CRS specified as 'projjson:projjson_epsg_5070',
    referencing a PROJJSON stored in file-level metadata.
    """
    return str(test_data_dir / "crs-projjson.parquet")


@pytest.fixture
def crs_srid_file(test_data_dir):
    """Return path to parquet file with srid: CRS format.

    This file has a GEOMETRY column with CRS specified as 'srid:5070',
    indicating EPSG:5070.
    """
    return str(test_data_dir / "crs-srid.parquet")
