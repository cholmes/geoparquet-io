"""Tests for --where row filtering on `gpio process aggregate` (issue #568).

The clause must apply to the source scan feeding keying, metrics, breakdowns,
and --auto resolution sizing, mirroring `gpio extract --where` semantics.
"""

import duckdb
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.exceptions import ValidationError
from geoparquet_io.core.process.aggregate.by_a5 import aggregate_by_a5
from geoparquet_io.core.process.aggregate.by_h3 import aggregate_by_h3


def _write_points_geoparquet(path, rows):
    """rows: list of (lon, lat, crop, area). Writes a tiny GeoParquet of points."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    values = ", ".join(f"({lon}, {lat}, '{crop}', {area})" for lon, lat, crop, area in rows)
    con.execute(
        f"""
        COPY (
            SELECT ST_Point(lon, lat) AS geometry, crop, area
            FROM (VALUES {values}) AS t(lon, lat, crop, area)
        ) TO '{path}' (FORMAT PARQUET)
        """
    )
    con.close()


FOUR_POINTS = [
    (10.00, 50.00, "wheat", 4.0),
    (10.001, 50.001, "wheat", 6.0),
    (10.002, 50.002, "corn", 2.0),
    (-120.0, 40.0, "wheat", 1.0),
]


# ---------------------------------------------------------------------------
# SQL construction units (no grid extension, no network)
# ---------------------------------------------------------------------------


def test_read_grid_source_sql_appends_where(tmp_path):
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection
    from geoparquet_io.core.process.aggregate.grid_common import read_grid_source_sql

    src = tmp_path / "f.parquet"
    _write_points_geoparquet(src, FOUR_POINTS)
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        sql = read_grid_source_sql(con, str(src), "geometry", where="area > 3")
        assert "WHERE (area > 3)" in sql
        count = con.execute(f"SELECT COUNT(*) FROM ({sql})").fetchone()[0]
        assert count == 2  # areas 4.0 and 6.0
        # No clause -> unchanged behavior
        sql_all = read_grid_source_sql(con, str(src), "geometry")
        assert "WHERE" not in sql_all
    finally:
        con.close()


def test_build_joined_sql_injects_where_into_inner_scan():
    from geoparquet_io.core.process.aggregate.by_admin import _build_joined_sql

    sql = _build_joined_sql(
        "in.parquet",
        "geometry",
        "read_parquet('admin.parquet')",
        "country",
        "country",
        "geometry",
        where="\"crop:name\" = 'wheat'",
    )
    inner = sql.split(") s")[0]  # the input-scan subquery
    assert "WHERE (\"crop:name\" = 'wheat')" in inner


def test_where_validation_rejects_dangerous_keywords(tmp_path):
    src = tmp_path / "f.parquet"
    _write_points_geoparquet(src, FOUR_POINTS[:1])
    with pytest.raises(ValidationError, match="dangerous"):
        aggregate_by_a5(
            str(src), str(tmp_path / "o.parquet"), resolution=5, where="1=1; DROP TABLE x"
        )


def test_cli_where_dangerous_keywords_fail_cleanly(tmp_path):
    src = tmp_path / "f.parquet"
    _write_points_geoparquet(src, FOUR_POINTS[:1])
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "process",
            "aggregate",
            "a5",
            str(src),
            str(tmp_path / "o.parquet"),
            "--resolution",
            "5",
            "--where",
            "DROP TABLE x",
        ],
    )
    assert result.exit_code != 0
    assert "dangerous" in result.output.lower()


# ---------------------------------------------------------------------------
# --auto integration: the filter must drive resolution sizing
# ---------------------------------------------------------------------------


def test_auto_row_count_respects_where(tmp_path):
    from geoparquet_io.core.partition.auto_resolution import _get_total_row_count

    src = tmp_path / "f.parquet"
    _write_points_geoparquet(src, FOUR_POINTS)
    assert _get_total_row_count(str(src)) == 4
    assert _get_total_row_count(str(src), where="area > 3") == 2


def test_aggregate_auto_threads_where_to_auto_resolution(tmp_path, monkeypatch):
    """aggregate --auto must size the grid on the *filtered* row count (#568)."""
    import geoparquet_io.core.partition.auto_resolution as auto_res

    src = tmp_path / "f.parquet"
    _write_points_geoparquet(src, FOUR_POINTS)
    captured = {}

    def fake_calc(input_parquet, spatial_index_type, **kwargs):
        captured.update(kwargs)
        raise RuntimeError("stop-after-capture")

    monkeypatch.setattr(auto_res, "calculate_auto_resolution", fake_calc)
    with pytest.raises(RuntimeError, match="stop-after-capture"):
        aggregate_by_a5(str(src), str(tmp_path / "o.parquet"), auto=True, where="area > 3")
    assert captured.get("where") == "area > 3"


def test_probe_distinct_cell_counts_respects_where(tmp_path):
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection
    from geoparquet_io.core.partition.auto_resolution import (
        _probe_distinct_cell_counts,
        _register_quadkey_udf,
    )

    src = tmp_path / "f.parquet"
    # Two far-apart points -> 2 distinct quadkeys; filter selects one.
    _write_points_geoparquet(src, [(10.0, 50.0, "wheat", 4.0), (-120.0, 40.0, "corn", 1.0)])
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        _register_quadkey_udf(con)
        counts_all = _probe_distinct_cell_counts(con, str(src), "quadkey", "geometry", "", [5])
        counts_filtered = _probe_distinct_cell_counts(
            con, str(src), "quadkey", "geometry", "", [5], where="crop = 'wheat'"
        )
        assert counts_all == [2]
        assert counts_filtered == [1]
    finally:
        con.close()


# ---------------------------------------------------------------------------
# End-to-end per scheme (needs grid community extensions)
# ---------------------------------------------------------------------------


@pytest.mark.slow
@pytest.mark.network
def test_aggregate_a5_where_filters_rows(tmp_path):
    src = tmp_path / "f.parquet"
    _write_points_geoparquet(src, FOUR_POINTS)
    out_all = tmp_path / "all.parquet"
    out_filtered = tmp_path / "filtered.parquet"
    aggregate_by_a5(str(src), str(out_all), resolution=5)
    aggregate_by_a5(str(src), str(out_filtered), resolution=5, where="crop = 'wheat'")
    total_all = sum(pq.read_table(out_all).column("count").to_pylist())
    total_filtered = sum(pq.read_table(out_filtered).column("count").to_pylist())
    assert total_all == 4
    assert total_filtered == 3  # three wheat rows


@pytest.mark.slow
@pytest.mark.network
def test_aggregate_a5_where_with_breakdown_and_metric(tmp_path):
    src = tmp_path / "f.parquet"
    _write_points_geoparquet(src, FOUR_POINTS)
    out = tmp_path / "o.parquet"
    aggregate_by_a5(
        str(src),
        str(out),
        resolution=5,
        metric="sum:area",
        breakdown="crop",
        where="area > 1",
    )
    df = pq.read_table(out).to_pandas()
    # The (-120, 40) wheat row (area=1) is filtered out entirely.
    assert int(df["count"].sum()) == 3
    assert float(df["sum_area"].sum()) == 12.0
    assert int(df["count_wheat"].sum()) == 2
    assert int(df["count_corn"].sum()) == 1


@pytest.mark.slow
@pytest.mark.network
def test_aggregate_h3_where_filters_rows(tmp_path):
    src = tmp_path / "f.parquet"
    _write_points_geoparquet(src, FOUR_POINTS)
    out = tmp_path / "o.parquet"
    aggregate_by_h3(str(src), str(out), resolution=5, where="crop = 'corn'")
    assert sum(pq.read_table(out).column("count").to_pylist()) == 1


@pytest.mark.slow
@pytest.mark.network
def test_cli_aggregate_a5_where_special_char_column(tmp_path):
    src = tmp_path / "f.parquet"
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    con.execute(
        f"""
        COPY (
            SELECT ST_Point(lon, lat) AS geometry, crop AS "crop:name"
            FROM (VALUES (10.0, 50.0, 'wheat'), (10.001, 50.001, 'corn')) AS t(lon, lat, crop)
        ) TO '{src}' (FORMAT PARQUET)
        """
    )
    con.close()
    out = tmp_path / "o.parquet"
    runner = CliRunner()
    result = runner.invoke(
        cli,
        [
            "process",
            "aggregate",
            "a5",
            str(src),
            str(out),
            "--resolution",
            "5",
            "--where",
            "\"crop:name\" = 'wheat'",
        ],
    )
    assert result.exit_code == 0, result.output
    assert sum(pq.read_table(out).column("count").to_pylist()) == 1


@pytest.mark.slow
@pytest.mark.network
def test_aggregate_admin_where_filters_rows(tmp_path):
    from geoparquet_io.core.process.aggregate.by_admin import aggregate_by_admin

    src = tmp_path / "pts.parquet"
    out = tmp_path / "by_country.parquet"
    # Two points in France (one wheat, one corn), one ocean wheat point.
    _write_points_geoparquet(
        src,
        [(2.35, 48.85, "wheat", 1.0), (4.85, 45.75, "corn", 2.0), (-30.0, 0.0, "wheat", 3.0)],
    )
    aggregate_by_admin(str(src), str(out), level="country", where="crop = 'wheat'")
    df = pq.read_table(out).to_pandas()
    assert int(df["count"].sum()) == 2  # corn row filtered out


@pytest.mark.slow
@pytest.mark.network
def test_table_aggregate_a5_where():
    from geoparquet_io.api.table import Table

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    tbl = (
        con.execute(
            """
            SELECT ST_AsWKB(ST_Point(lon, lat)) AS geometry, crop FROM (VALUES
                (10.0, 50.0, 'wheat'), (10.001, 50.001, 'corn')
            ) AS t(lon, lat, crop)
            """
        )
        .arrow()
        .read_all()
    )
    con.close()
    result = Table(tbl).aggregate_a5(resolution=5, where="crop = 'wheat'")
    assert sum(result.table.column("count").to_pylist()) == 1


def test_cli_help_has_where_option():
    runner = CliRunner()
    for sub in ("a5", "h3", "admin"):
        result = runner.invoke(cli, ["process", "aggregate", sub, "--help"])
        assert result.exit_code == 0
        assert "--where" in result.output, f"--where missing from {sub} --help"
