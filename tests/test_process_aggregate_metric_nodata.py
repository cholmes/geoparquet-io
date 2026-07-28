"""Tests for --metric-nodata sentinel handling in `gpio process aggregate` (issue #566).

Sentinel values (e.g. -999) are mapped to NULL for metric computation only, so
sum/avg/min/max ignore them while `count` still counts every feature.
"""

import duckdb
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.process.aggregate.by_a5 import aggregate_by_a5
from geoparquet_io.core.process.aggregate.common import (
    build_metric_select,
    parse_metric_nodata,
    parse_metrics,
)


def _write_points_geoparquet(path, rows):
    """rows: list of (lon, lat, height). Writes a tiny GeoParquet of points."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    values = ", ".join(f"({lon}, {lat}, {height})" for lon, lat, height in rows)
    con.execute(
        f"""
        COPY (
            SELECT ST_Point(lon, lat) AS geometry, height
            FROM (VALUES {values}) AS t(lon, lat, height)
        ) TO '{path}' (FORMAT PARQUET)
        """
    )
    con.close()


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


def test_parse_metric_nodata_single():
    assert parse_metric_nodata("-999") == ["-999"]


def test_parse_metric_nodata_multiple_with_whitespace():
    assert parse_metric_nodata(" -999, -9999 ,9999") == ["-999", "-9999", "9999"]


def test_parse_metric_nodata_float():
    assert parse_metric_nodata("-999.0") == ["-999.0"]


def test_parse_metric_nodata_none_passthrough():
    assert parse_metric_nodata(None) == []


def test_parse_metric_nodata_rejects_non_numeric():
    with pytest.raises(InvalidParameterError):
        parse_metric_nodata("NA")
    with pytest.raises(InvalidParameterError):
        parse_metric_nodata("-999,oops")
    with pytest.raises(InvalidParameterError):
        parse_metric_nodata("")


# ---------------------------------------------------------------------------
# SQL generation (shared by grid + admin paths)
# ---------------------------------------------------------------------------


def test_build_metric_select_single_sentinel():
    metrics = parse_metrics("avg:height,max:height")
    sql = build_metric_select(metrics, nodata_values=["-999"])
    assert sql == (
        'AVG(NULLIF("height", -999)) AS "avg_height", MAX(NULLIF("height", -999)) AS "max_height"'
    )


def test_build_metric_select_multiple_sentinels():
    metrics = parse_metrics("min:var")
    sql = build_metric_select(metrics, nodata_values=["-999", "-9999"])
    assert sql == ('MIN(CASE WHEN "var" IN (-999, -9999) THEN NULL ELSE "var" END) AS "min_var"')


def test_build_metric_select_without_nodata_unchanged():
    metrics = parse_metrics("sum:area_ha,avg:yield")
    assert build_metric_select(metrics) == (
        'SUM("area_ha") AS "sum_area_ha", AVG("yield") AS "avg_yield"'
    )


def test_metric_nodata_verified_against_duckdb():
    """The generated SQL computes metrics over non-sentinel values only."""
    con = duckdb.connect()
    con.execute("CREATE TABLE t AS SELECT * FROM (VALUES (1.0), (-999.0), (2.0)) v(height)")
    metrics = parse_metrics("avg:height,min:height")
    sql = build_metric_select(metrics, nodata_values=["-999"])
    row = con.execute(f"SELECT COUNT(*) AS count, {sql} FROM t").fetchone()
    assert row[0] == 3  # count unaffected
    assert row[1] == 1.5  # avg of 1.0, 2.0
    assert row[2] == 1.0  # min ignores -999
    con.close()


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def test_metric_nodata_requires_metric(tmp_path):
    src = tmp_path / "f.parquet"
    _write_points_geoparquet(src, [(10.0, 50.0, 5.0)])
    with pytest.raises(InvalidParameterError, match="metric"):
        aggregate_by_a5(str(src), str(tmp_path / "o.parquet"), resolution=5, metric_nodata="-999")


def test_cli_metric_nodata_requires_metric(tmp_path):
    src = tmp_path / "f.parquet"
    _write_points_geoparquet(src, [(10.0, 50.0, 5.0)])
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
            "--metric-nodata",
            "-999",
        ],
    )
    assert result.exit_code != 0
    assert "metric" in result.output.lower()


def test_cli_help_has_metric_nodata_option():
    runner = CliRunner()
    for sub in ("a5", "h3", "admin"):
        result = runner.invoke(cli, ["process", "aggregate", sub, "--help"])
        assert result.exit_code == 0
        assert "--metric-nodata" in result.output, f"--metric-nodata missing from {sub} --help"


# ---------------------------------------------------------------------------
# End-to-end (needs grid community extensions)
# ---------------------------------------------------------------------------


@pytest.mark.slow
@pytest.mark.network
def test_aggregate_a5_metric_nodata(tmp_path):
    src = tmp_path / "f.parquet"
    out = tmp_path / "o.parquet"
    # One cluster: heights 4.0, 6.0, and a -999 NoData sentinel.
    _write_points_geoparquet(
        src, [(10.0, 50.0, 4.0), (10.001, 50.001, 6.0), (10.002, 50.002, -999.0)]
    )
    aggregate_by_a5(
        str(src),
        str(out),
        resolution=5,
        metric="avg:height,min:height",
        metric_nodata="-999",
    )
    df = pq.read_table(out).to_pandas()
    assert int(df["count"].sum()) == 3  # count includes the sentinel row
    assert float(df["avg_height"].iloc[0]) == 5.0  # avg of 4.0, 6.0
    assert float(df["min_height"].iloc[0]) == 4.0  # min ignores -999


@pytest.mark.slow
@pytest.mark.network
def test_cli_aggregate_a5_metric_nodata(tmp_path):
    src = tmp_path / "f.parquet"
    out = tmp_path / "o.parquet"
    _write_points_geoparquet(src, [(10.0, 50.0, 4.0), (10.001, 50.001, -999.0)])
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
            "--metric",
            "avg:height",
            "--metric-nodata",
            "-999",
        ],
    )
    assert result.exit_code == 0, result.output
    df = pq.read_table(out).to_pandas()
    assert float(df["avg_height"].iloc[0]) == 4.0


@pytest.mark.slow
@pytest.mark.network
def test_table_aggregate_a5_metric_nodata():
    from geoparquet_io.api.table import Table

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    tbl = (
        con.execute(
            """
            SELECT ST_AsWKB(ST_Point(lon, lat)) AS geometry, height FROM (VALUES
                (10.0, 50.0, 2.0), (10.001, 50.001, -999.0)
            ) AS t(lon, lat, height)
            """
        )
        .arrow()
        .read_all()
    )
    con.close()
    result = Table(tbl).aggregate_a5(resolution=5, metric="avg:height", metric_nodata="-999")
    df = result.table.to_pandas()
    assert int(df["count"].sum()) == 2
    assert float(df["avg_height"].iloc[0]) == 2.0
