import duckdb
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
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
