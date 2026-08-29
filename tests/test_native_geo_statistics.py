"""Native Parquet geospatial statistics must describe the data they ship with.

Issue #721 asked which side zeroes the geospatial statistics of a native
GEOMETRY column on Windows: pyarrow writing them, or DuckDB's
``parquet_metadata()`` reading them. CI answered it — run 33193858561, Python
3.11/3.12/3.13, both pyarrow write paths, 6 of 6 consistent:

- ``..._via_pyarrow``         XPASS — pyarrow reads back the real bounds
- ``..._via_duckdb``          XFAIL — DuckDB reports [0, 0, 0, 0] for that same file
- ``..._both_readers_agree``  XFAIL

So the files gpio writes on Windows are correct on disk; gpio's *reader* is not.
Everything gpio reports and validates from these statistics goes through DuckDB
``parquet_metadata()`` — ``get_native_geo_statistics``,
``get_aggregated_native_geo_stats`` and ``get_per_row_group_native_geo_stats``
in ``core/duckdb_metadata.py`` — so on Windows those misreport bounds that are
fine in the file. ``native_geo_stats_contains_data_*`` was flagging a real
discrepancy all along; it pointed at the reader, not at the file. No rewrite of
Windows-written files is needed, and routing Windows native-geo writes through
DuckDB ``COPY`` would have fixed nothing.

``test_duckdb_reads_a_file_it_did_not_write`` closes the one branch the pair
above leaves open: a writer and reader that are wrong *symmetrically* would also
agree. It reads a committed corpus fixture, written by neither gpio nor the
Windows runner, so a failure there is the reader's alone.

The pyarrow assertion is enforced on every platform, Windows included: now that
the write path is known good there, it is the guard that keeps it good. The
DuckDB-dependent assertions stay xfail on Windows until the reader is fixed
(#721). They are strict, so the day DuckDB stops zeroing them CI turns red and
names the markers — and this docstring — to delete.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import shapely

from geoparquet_io.core.common import get_duckdb_connection, write_geoparquet_table

# Far from the origin in every direction, so all-zero statistics cannot contain
# the data by accident, and a zeroed *single* bound is still caught. The
# assertions below assume plain min/max bounds; the Parquet geospatial spec also
# permits xmin > xmax for antimeridian-wrapping bounds, which these points would
# qualify for if a writer ever chose to emit them.
POINTS = [(-122.4, 37.8), (151.2, -33.9), (2.35, 48.85)]
EXPECTED = {"xmin": -122.4, "ymin": -33.9, "xmax": 151.2, "ymax": 48.85}

# A native-geo file written by the geoparquet-testing corpus, not by gpio and not
# on the runner reading it. Bounds are the file's own, verified from the fixture.
CORPUS_NATIVE_GEO = Path(__file__).parent / "data" / "geoparquet-testing" / "data" / "encodings"
CORPUS_EXPECTED = {"xmin": 30.0, "ymin": 10.0, "xmax": 40.0, "ymax": 40.0}

duckdb_windows_xfail = pytest.mark.xfail(
    sys.platform == "win32",
    reason=(
        "#721: DuckDB parquet_metadata() reports [0, 0, 0, 0] on Windows for "
        "native GEOMETRY columns whose statistics pyarrow reads back correctly "
        "from the very same file"
    ),
    strict=True,
)


def _points_table():
    return pa.table(
        {
            "id": pa.array(range(len(POINTS))),
            "geometry": pa.array(
                [shapely.to_wkb(shapely.Point(x, y)) for x, y in POINTS], pa.binary()
            ),
        }
    )


def _write_via_table_writer(tmp_path):
    """`write_geoparquet_table` -> pyarrow `write_table` (the #702 path)."""
    out = tmp_path / "table_writer.parquet"
    write_geoparquet_table(
        _points_table(), str(out), geometry_column="geometry", geoparquet_version="2.0"
    )
    return str(out)


def _write_via_streaming_strategy(tmp_path):
    """The arrow-streaming write strategy -> pyarrow `ParquetWriter` (the #707 path)."""
    from click.testing import CliRunner

    from geoparquet_io.cli.main import cli

    src = tmp_path / "src.parquet"
    write_geoparquet_table(
        _points_table(), str(src), geometry_column="geometry", geoparquet_version="1.1"
    )
    out = tmp_path / "streamed.parquet"
    result = CliRunner().invoke(
        cli,
        [
            "extract",
            "geoparquet",
            str(src),
            str(out),
            "--geoparquet-version",
            "2.0",
            "--write-strategy",
            "streaming",
        ],
    )
    assert result.exit_code == 0, result.output
    return str(out)


@pytest.fixture(
    params=[_write_via_table_writer, _write_via_streaming_strategy],
    ids=["table-writer", "streaming-strategy"],
)
def native_geo_file(request, tmp_path):
    """A native-geo file at known coordinates, from each implicated pyarrow path."""
    return request.param(tmp_path)


def _contains_data(xmin, ymin, xmax, ymax) -> bool:
    return (
        xmin <= EXPECTED["xmin"]
        and ymin <= EXPECTED["ymin"]
        and xmax >= EXPECTED["xmax"]
        and ymax >= EXPECTED["ymax"]
    )


def test_all_zero_statistics_are_recognised_as_not_containing_the_data():
    """Pin the guard's own logic: [0,0,0,0] is exactly the reported failure."""
    assert not _contains_data(0.0, 0.0, 0.0, 0.0)
    assert _contains_data(EXPECTED["xmin"], EXPECTED["ymin"], EXPECTED["xmax"], EXPECTED["ymax"])
    # A single zeroed bound is caught too.
    assert not _contains_data(0.0, EXPECTED["ymin"], EXPECTED["xmax"], EXPECTED["ymax"])


def test_native_geo_file_uses_the_geometry_logical_type(native_geo_file):
    """Guard the premise: without the native type there are no statistics to check."""
    schema = str(pq.ParquetFile(native_geo_file).metadata.schema)
    assert "Geometry" in schema or "Geography" in schema


def test_native_geo_statistics_contain_the_data_via_pyarrow(native_geo_file):
    """The writer's own reader: what pyarrow believes it wrote."""
    pf = pq.ParquetFile(native_geo_file)
    index = pf.schema_arrow.names.index("geometry")

    for rg in range(pf.metadata.num_row_groups):
        column = pf.metadata.row_group(rg).column(index)
        assert column.is_geo_stats_set, f"row group {rg} has no geospatial statistics"
        stats = column.geo_statistics
        assert _contains_data(stats.xmin, stats.ymin, stats.xmax, stats.ymax), (
            f"row group {rg} statistics "
            f"[{stats.xmin}, {stats.ymin}, {stats.xmax}, {stats.ymax}] "
            f"do not contain the data {EXPECTED}"
        )


@duckdb_windows_xfail
def test_native_geo_statistics_contain_the_data_via_duckdb(native_geo_file):
    """The reader gpio itself uses when it reports and validates these statistics."""
    con = get_duckdb_connection()
    try:
        rows = con.execute(
            "SELECT geo_bbox FROM parquet_metadata(?) WHERE path_in_schema = 'geometry'",
            [native_geo_file],
        ).fetchall()
    finally:
        con.close()

    assert rows, "DuckDB reported no geometry column chunk"
    for (bbox,) in rows:
        assert bbox is not None, "DuckDB reported no geospatial statistics"
        assert _contains_data(bbox["xmin"], bbox["ymin"], bbox["xmax"], bbox["ymax"]), (
            f"statistics {bbox} do not contain the data {EXPECTED}"
        )


@duckdb_windows_xfail
def test_both_readers_agree_on_the_statistics(native_geo_file):
    """Whichever side is wrong, the two readers disagreeing is itself the signal."""
    pf = pq.ParquetFile(native_geo_file)
    index = pf.schema_arrow.names.index("geometry")

    con = get_duckdb_connection()
    try:
        # One row per column chunk, so order it: the comparison below is
        # positional, and parquet_metadata() promises no ordering of its own.
        rows = con.execute(
            "SELECT geo_bbox FROM parquet_metadata(?) "
            "WHERE path_in_schema = 'geometry' ORDER BY row_group_id",
            [native_geo_file],
        ).fetchall()
    finally:
        con.close()

    assert len(rows) == pf.metadata.num_row_groups, (
        f"DuckDB reported {len(rows)} geometry chunks for {pf.metadata.num_row_groups} row groups"
    )

    for rg, (bbox,) in enumerate(rows):
        stats = pf.metadata.row_group(rg).column(index).geo_statistics
        # Exact equality is the property under test, not an approximation of it:
        # both readers decode the same IEEE-754 doubles out of the same thrift
        # footer, with no arithmetic in between. Do not relax this to approx().
        assert (stats.xmin, stats.ymin, stats.xmax, stats.ymax) == (
            bbox["xmin"],
            bbox["ymin"],
            bbox["xmax"],
            bbox["ymax"],
        ), f"row group {rg}: pyarrow and DuckDB disagree"


@pytest.mark.corpus
@duckdb_windows_xfail
def test_duckdb_reads_a_file_it_did_not_write():
    """Isolate the reader from the writer, on a file neither gpio nor CI wrote.

    The tests above compare pyarrow against DuckDB on files pyarrow just wrote
    on the same machine. A writer and reader wrong *symmetrically* would agree,
    which is the one reading under which the zeros would still be a write-side
    bug. This fixture is committed to the geoparquet-testing submodule and was
    produced elsewhere, so whatever DuckDB reports about it is the reader's
    doing alone.
    """
    fixture = CORPUS_NATIVE_GEO / "point-native-geometry.parquet"
    if not fixture.exists():
        pytest.skip("run: git submodule update --init")

    con = get_duckdb_connection()
    try:
        bbox = con.execute(
            "SELECT geo_bbox FROM parquet_metadata(?) WHERE path_in_schema = 'geometry'",
            [str(fixture)],
        ).fetchone()
    finally:
        con.close()

    assert bbox is not None, "DuckDB reported no geometry column chunk"
    bbox = bbox[0]
    assert bbox is not None, "DuckDB reported no geospatial statistics"
    assert (bbox["xmin"], bbox["ymin"], bbox["xmax"], bbox["ymax"]) == (
        CORPUS_EXPECTED["xmin"],
        CORPUS_EXPECTED["ymin"],
        CORPUS_EXPECTED["xmax"],
        CORPUS_EXPECTED["ymax"],
    ), f"DuckDB reports {bbox} for a fixture whose bounds are {CORPUS_EXPECTED}"
