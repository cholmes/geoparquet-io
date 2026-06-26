import duckdb
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.process.aggregate.by_a5 import aggregate_by_a5


def test_process_aggregate_group_exists():
    runner = CliRunner()
    result = runner.invoke(cli, ["process", "aggregate", "--help"])
    assert result.exit_code == 0
    assert "aggregate" in result.output.lower()


def _write_points_geoparquet(path, rows):
    """rows: list of (lon, lat, crop, area). Writes a tiny GeoParquet of points."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    values = ", ".join(f"({lon}, {lat}, '{crop}', {area})" for lon, lat, crop, area in rows)
    con.execute(
        f"""
        COPY (
            SELECT ST_AsWKB(ST_Point(lon, lat)) AS geometry, crop, area
            FROM (VALUES {values}) AS t(lon, lat, crop, area)
        ) TO '{path}' (FORMAT PARQUET)
        """
    )
    con.close()


@pytest.mark.slow
def test_aggregate_a5_count_metric_breakdown(tmp_path):
    src = tmp_path / "fields.parquet"
    out = tmp_path / "agg.parquet"
    # Two clusters far apart so they land in different res-5 cells.
    _write_points_geoparquet(
        src,
        [
            (10.00, 50.00, "wheat", 4.0),
            (10.001, 50.001, "wheat", 6.0),
            (10.002, 50.002, "corn", 2.0),
            (-120.0, 40.0, "wheat", 1.0),
        ],
    )
    aggregate_by_a5(
        str(src),
        str(out),
        resolution=5,
        metric="sum:area",
        breakdown="crop",
        out_geometry="polygon",
    )
    table = pq.read_table(out)
    cols = table.column_names
    assert "a5_cell" in cols
    assert "count" in cols
    assert "sum_area" in cols
    assert "count_wheat" in cols and "count_corn" in cols
    assert "geometry" in cols
    # Two output cells (two clusters)
    assert table.num_rows == 2
    # The 3-feature cell totals area 12 and has 2 wheat + 1 corn
    df = table.to_pandas().sort_values("count", ascending=False).reset_index(drop=True)
    assert int(df.loc[0, "count"]) == 3
    assert float(df.loc[0, "sum_area"]) == 12.0
    assert int(df.loc[0, "count_wheat"]) == 2
    assert int(df.loc[0, "count_corn"]) == 1


@pytest.mark.slow
def test_aggregate_a5_out_geometry_none_is_plain_table(tmp_path):
    src = tmp_path / "fields.parquet"
    out = tmp_path / "agg_none.parquet"
    _write_points_geoparquet(src, [(10.0, 50.0, "wheat", 4.0)])
    aggregate_by_a5(str(src), str(out), resolution=5, out_geometry="none")
    table = pq.read_table(out)
    assert "a5_cell" in table.column_names
    assert "count" in table.column_names
    assert "geometry" not in table.column_names
    assert b"geo" not in (table.schema.metadata or {})


def test_aggregate_a5_requires_resolution_or_auto(tmp_path):
    src = tmp_path / "f.parquet"
    _write_points_geoparquet(src, [(10.0, 50.0, "wheat", 1.0)])
    with pytest.raises(InvalidParameterError):
        aggregate_by_a5(str(src), str(tmp_path / "o.parquet"))  # neither given


def test_aggregate_a5_rejects_resolution_and_auto(tmp_path):
    src = tmp_path / "f.parquet"
    _write_points_geoparquet(src, [(10.0, 50.0, "wheat", 1.0)])
    with pytest.raises(InvalidParameterError):
        aggregate_by_a5(str(src), str(tmp_path / "o.parquet"), resolution=5, auto=True)


@pytest.mark.slow
def test_aggregate_a5_auto_runs(tmp_path):
    src = tmp_path / "f.parquet"
    out = tmp_path / "o.parquet"
    _write_points_geoparquet(src, [(10.0, 50.0, "wheat", 1.0), (10.001, 50.001, "corn", 2.0)])
    aggregate_by_a5(str(src), str(out), auto=True, target_per_cell=1)
    assert out.exists()


@pytest.mark.slow
def test_cli_process_aggregate_a5(tmp_path):
    src = tmp_path / "f.parquet"
    out = tmp_path / "o.parquet"
    _write_points_geoparquet(src, [(10.0, 50.0, "wheat", 4.0), (10.001, 50.001, "corn", 2.0)])
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
            "sum:area",
            "--breakdown",
            "crop",
        ],
    )
    assert result.exit_code == 0, result.output
    assert out.exists()


def _points_table():
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    return (
        con.execute(
            """
        SELECT ST_AsWKB(ST_Point(lon, lat)) AS geometry, crop, area FROM (VALUES
            (10.0, 50.0, 'wheat', 4.0),
            (10.001, 50.001, 'corn', 2.0)
        ) AS t(lon, lat, crop, area)
        """
        )
        .arrow()
        .read_all()
    )


@pytest.mark.slow
def test_table_aggregate_a5_api():
    from geoparquet_io.api.table import Table

    result = Table(_points_table()).aggregate_a5(resolution=5, metric="sum:area")
    assert "a5_cell" in result.column_names
    assert "count" in result.column_names
    assert "sum_area" in result.column_names
    assert "geometry" in result.column_names


def test_cli_process_aggregate_a5_bad_metric(tmp_path):
    src = tmp_path / "f.parquet"
    _write_points_geoparquet(src, [(10.0, 50.0, "wheat", 4.0)])
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
            "--metric",
            "median:area",
        ],
    )
    assert result.exit_code != 0
    assert "median" in result.output.lower() or "metric" in result.output.lower()
