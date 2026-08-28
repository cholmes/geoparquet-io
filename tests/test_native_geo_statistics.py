"""Native Parquet geospatial statistics must describe the data they ship with.

Issue #721: on Windows, a file gpio writes with a native GEOMETRY logical type
through the **pyarrow** writer comes back with geospatial statistics of
``[0, 0, 0, 0]`` while the geometries are elsewhere. The statistics are
*present* (``is_geo_stats_set`` is true) — they are just all zeros. Any reader
using them for predicate pushdown skips row groups that actually match, so the
result is silently wrong query results rather than an error. The same write path
produces correct statistics on macOS and Linux, and the DuckDB ``COPY`` write
strategy is correct on Windows too, which is why only the pyarrow path is
implicated.

The open question the issue poses is **which side the zeros come from**: pyarrow
writing them, or DuckDB's ``parquet_metadata()`` reading them. Neither can be
settled on a machine where the bug does not reproduce, so these tests settle it
in CI instead. They read the *same file* through both readers and assert each
one separately. On Windows they are non-strict xfail, so CI stays green while
recording which of the two fails:

- only ``via_duckdb`` xfails  -> pyarrow wrote correct statistics and DuckDB
  misreads them; the fix belongs in gpio's stats-reading path
- both xfail                  -> pyarrow wrote zeros; the fix (or workaround)
  belongs upstream, or in routing Windows native-geo writes through DuckDB COPY
- both xpass                  -> the platform bug is gone; delete the markers
  and this docstring's question with them
"""

from __future__ import annotations

import sys

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import shapely

from geoparquet_io.core.common import write_geoparquet_table

# Far from the origin in every direction, so all-zero statistics cannot contain
# the data by accident, and a zeroed *single* bound is still caught.
POINTS = [(-122.4, 37.8), (151.2, -33.9), (2.35, 48.85)]
EXPECTED = {"xmin": -122.4, "ymin": -33.9, "xmax": 151.2, "ymax": 48.85}

windows_xfail = pytest.mark.xfail(
    sys.platform == "win32",
    reason="#721: pyarrow-written native GEOMETRY carries all-zero geospatial stats on Windows",
    strict=False,
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


@windows_xfail
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


@windows_xfail
def test_native_geo_statistics_contain_the_data_via_duckdb(native_geo_file):
    """The reader gpio itself uses when it reports and validates these statistics."""
    con = duckdb.connect()
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


@windows_xfail
def test_both_readers_agree_on_the_statistics(native_geo_file):
    """Whichever side is wrong, the two readers disagreeing is itself the signal."""
    pf = pq.ParquetFile(native_geo_file)
    index = pf.schema_arrow.names.index("geometry")
    stats = pf.metadata.row_group(0).column(index).geo_statistics

    con = duckdb.connect()
    try:
        bbox = con.execute(
            "SELECT geo_bbox FROM parquet_metadata(?) WHERE path_in_schema = 'geometry'",
            [native_geo_file],
        ).fetchone()[0]
    finally:
        con.close()

    assert (stats.xmin, stats.ymin, stats.xmax, stats.ymax) == (
        bbox["xmin"],
        bbox["ymin"],
        bbox["xmax"],
        bbox["ymax"],
    )
