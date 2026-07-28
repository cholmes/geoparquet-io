"""Tests for --bucket-point on `gpio process aggregate` (issue #567).

Keying can derive its point from a bbox covering column (or an existing point
column) instead of the full geometry, so the (usually huge) geometry column is
never scanned for the common cell-polygon output.
"""

import duckdb
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.exceptions import InvalidParameterError
from geoparquet_io.core.process.aggregate.by_a5 import aggregate_by_a5
from geoparquet_io.core.process.aggregate.by_h3 import aggregate_by_h3


def _write_points_with_bbox(path, rows, bbox_col="bbox"):
    """rows: list of (lon, lat, height). Writes points plus a bbox struct column."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    values = ", ".join(f"({lon}, {lat}, {height})" for lon, lat, height in rows)
    con.execute(
        f"""
        COPY (
            SELECT ST_Point(lon, lat) AS geometry,
                   {{'xmin': lon - 0.0004, 'ymin': lat - 0.0004,
                     'xmax': lon + 0.0004, 'ymax': lat + 0.0004}} AS "{bbox_col}",
                   height
            FROM (VALUES {values}) AS t(lon, lat, height)
        ) TO '{path}' (FORMAT PARQUET)
        """
    )
    con.close()


def _write_plain_points(path, rows):
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


POINTS = [
    (10.00, 50.00, 4.0),
    (10.001, 50.001, 6.0),
    (-120.0, 40.0, 1.0),
]


# ---------------------------------------------------------------------------
# Source-SQL construction (fast, no grid extension)
# ---------------------------------------------------------------------------


def test_read_grid_source_sql_bbox_mode(tmp_path):
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection
    from geoparquet_io.core.process.aggregate.grid_common import read_grid_source_sql

    src = tmp_path / "f.parquet"
    _write_points_with_bbox(src, POINTS)
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        sql = read_grid_source_sql(
            con, str(src), "geometry", bucket_point="bbox", bbox_column="bbox"
        )
        # Keying point from the bbox center, geometry column excluded from the scan.
        assert "AS __pt" in sql
        assert "ST_Point" in sql
        assert 'EXCLUDE ("geometry")' in sql
        cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM ({sql})").fetchall()}
        assert "geometry" not in cols
        assert "__pt" in cols
        # bbox center == the original point (symmetric box)
        row = con.execute(f"SELECT ST_X(__pt), ST_Y(__pt) FROM ({sql}) LIMIT 1").fetchone()
        assert row[0] == pytest.approx(10.0)
        assert row[1] == pytest.approx(50.0)
    finally:
        con.close()


def test_read_grid_source_sql_geometry_mode_default(tmp_path):
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection
    from geoparquet_io.core.process.aggregate.grid_common import read_grid_source_sql

    src = tmp_path / "f.parquet"
    _write_plain_points(src, POINTS)
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        sql = read_grid_source_sql(con, str(src), "geometry")
        assert "ST_Centroid" in sql
        assert "AS __pt" in sql
        cols = {r[0] for r in con.execute(f"DESCRIBE SELECT * FROM ({sql})").fetchall()}
        assert "geometry" in cols  # geometry passthrough kept in default mode
    finally:
        con.close()


def test_read_grid_source_sql_point_column_mode(tmp_path):
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection
    from geoparquet_io.core.process.aggregate.grid_common import read_grid_source_sql

    src = tmp_path / "f.parquet"
    con0 = duckdb.connect()
    con0.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    con0.execute(
        f"""
        COPY (
            SELECT ST_Point(0.0, 0.0) AS geometry, ST_Point(10.0, 50.0) AS anchor, 1.0 AS height
        ) TO '{src}' (FORMAT PARQUET)
        """
    )
    con0.close()
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        sql = read_grid_source_sql(con, str(src), "geometry", bucket_point="anchor")
        assert 'EXCLUDE ("geometry")' in sql
        row = con.execute(f"SELECT ST_X(__pt), ST_Y(__pt) FROM ({sql})").fetchone()
        assert row == (10.0, 50.0)
    finally:
        con.close()


def test_read_grid_source_sql_bbox_mode_transforms_non_crs84(tmp_path):
    from geoparquet_io.core.duckdb_utils import get_duckdb_connection
    from geoparquet_io.core.process.aggregate.grid_common import read_grid_source_sql

    src = tmp_path / "f.parquet"
    _write_points_with_bbox(src, POINTS)
    con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
    try:
        sql = read_grid_source_sql(
            con,
            str(src),
            "geometry",
            source_crs="EPSG:5070",
            bucket_point="bbox",
            bbox_column="bbox",
        )
        assert "ST_Transform" in sql  # bbox coords are in the file's CRS
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Validation / detection
# ---------------------------------------------------------------------------


def test_bbox_mode_errors_when_no_bbox_column(tmp_path):
    src = tmp_path / "f.parquet"
    _write_plain_points(src, POINTS)
    with pytest.raises(InvalidParameterError, match="bbox"):
        aggregate_by_a5(str(src), str(tmp_path / "o.parquet"), resolution=5, bucket_point="bbox")


def test_bbox_column_requires_bbox_mode(tmp_path):
    src = tmp_path / "f.parquet"
    _write_points_with_bbox(src, POINTS)
    with pytest.raises(InvalidParameterError, match="bucket-point"):
        aggregate_by_a5(str(src), str(tmp_path / "o.parquet"), resolution=5, bbox_column="bbox")


def test_h3_bbox_mode_errors_when_no_bbox_column(tmp_path):
    src = tmp_path / "f.parquet"
    _write_plain_points(src, POINTS)
    with pytest.raises(InvalidParameterError, match="bbox"):
        aggregate_by_h3(str(src), str(tmp_path / "o.parquet"), resolution=5, bucket_point="bbox")


def test_admin_bbox_mode_errors_when_no_bbox_column(tmp_path):
    """Admin path resolves the bbox column before any dataset setup or download."""
    from geoparquet_io.core.process.aggregate.by_admin import aggregate_by_admin

    src = tmp_path / "f.parquet"
    _write_plain_points(src, POINTS)
    with pytest.raises(InvalidParameterError, match="bbox"):
        aggregate_by_admin(
            str(src), str(tmp_path / "o.parquet"), level="country", bucket_point="bbox"
        )


def test_table_bbox_mode_errors_when_no_bbox_column():
    from geoparquet_io.core.process.aggregate.by_a5 import aggregate_a5_table

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    tbl = (
        con.execute("SELECT ST_AsWKB(ST_Point(10.0, 50.0)) AS geometry, 1.0 AS height")
        .arrow()
        .read_all()
    )
    con.close()
    with pytest.raises(InvalidParameterError, match="bbox"):
        aggregate_a5_table(tbl, resolution=5, bucket_point="bbox")


def test_table_bbox_mode_autodetects_from_schema():
    """Table-path detection finds a conventional bbox struct in the Arrow schema."""
    from geoparquet_io.core.process.aggregate.grid_common import (
        _resolve_bbox_column_for_table,
    )

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    tbl = (
        con.execute(
            "SELECT ST_AsWKB(ST_Point(10.0, 50.0)) AS geometry, "
            "{'xmin': 9.9, 'ymin': 49.9, 'xmax': 10.1, 'ymax': 50.1} AS bbox"
        )
        .arrow()
        .read_all()
    )
    con.close()
    assert _resolve_bbox_column_for_table(tbl, None) == "bbox"
    assert _resolve_bbox_column_for_table(tbl, "custom") == "custom"


def test_cli_help_has_bucket_point_options():
    runner = CliRunner()
    for sub in ("a5", "h3", "admin"):
        result = runner.invoke(cli, ["process", "aggregate", sub, "--help"])
        assert result.exit_code == 0
        assert "--bucket-point" in result.output, f"--bucket-point missing from {sub} --help"
        assert "--bbox-column" in result.output, f"--bbox-column missing from {sub} --help"


def test_admin_joined_sql_uses_point_expr_and_exclude():
    from geoparquet_io.core.process.aggregate.by_admin import _build_joined_sql

    sql = _build_joined_sql(
        "in.parquet",
        "ST_Point((bbox.xmin + bbox.xmax) / 2.0, (bbox.ymin + bbox.ymax) / 2.0)",
        "read_parquet('admin.parquet')",
        "country",
        "country",
        "geometry",
        exclude_input_columns=("geometry",),
    )
    inner = sql.split(") s")[0]
    assert "ST_Point((bbox.xmin" in inner
    assert 'EXCLUDE ("geometry")' in inner


# ---------------------------------------------------------------------------
# End-to-end (grid community extensions)
# ---------------------------------------------------------------------------


@pytest.mark.slow
@pytest.mark.network
def test_aggregate_a5_bbox_matches_geometry_keying(tmp_path):
    """For symmetric boxes around points, bbox keying == centroid keying."""
    src = tmp_path / "f.parquet"
    _write_points_with_bbox(src, POINTS)
    out_geom = tmp_path / "geom.parquet"
    out_bbox = tmp_path / "bbox.parquet"
    aggregate_by_a5(str(src), str(out_geom), resolution=5, metric="avg:height")
    aggregate_by_a5(str(src), str(out_bbox), resolution=5, metric="avg:height", bucket_point="bbox")
    df_g = pq.read_table(out_geom).to_pandas().sort_values("a5_cell").reset_index(drop=True)
    df_b = pq.read_table(out_bbox).to_pandas().sort_values("a5_cell").reset_index(drop=True)
    assert list(df_g["a5_cell"]) == list(df_b["a5_cell"])
    assert list(df_g["count"]) == list(df_b["count"])
    assert list(df_g["avg_height"]) == list(df_b["avg_height"])


@pytest.mark.slow
@pytest.mark.network
def test_aggregate_a5_bbox_autodetects_column(tmp_path):
    src = tmp_path / "f.parquet"
    _write_points_with_bbox(src, POINTS)  # conventional name "bbox" -> auto-detected
    out = tmp_path / "o.parquet"
    aggregate_by_a5(str(src), str(out), resolution=5, bucket_point="bbox")
    assert sum(pq.read_table(out).column("count").to_pylist()) == 3


@pytest.mark.slow
@pytest.mark.network
def test_aggregate_a5_bbox_explicit_nonstandard_column(tmp_path):
    src = tmp_path / "f.parquet"
    _write_points_with_bbox(src, POINTS, bbox_col="my_box")  # convention can't find this
    out = tmp_path / "o.parquet"
    aggregate_by_a5(str(src), str(out), resolution=5, bucket_point="bbox", bbox_column="my_box")
    assert sum(pq.read_table(out).column("count").to_pylist()) == 3


@pytest.mark.slow
@pytest.mark.network
def test_aggregate_h3_bbox_mode(tmp_path):
    src = tmp_path / "f.parquet"
    _write_points_with_bbox(src, POINTS)
    out = tmp_path / "o.parquet"
    aggregate_by_h3(str(src), str(out), resolution=5, bucket_point="bbox")
    assert sum(pq.read_table(out).column("count").to_pylist()) == 3


@pytest.mark.slow
@pytest.mark.network
def test_cli_aggregate_a5_bucket_point_bbox(tmp_path):
    src = tmp_path / "f.parquet"
    _write_points_with_bbox(src, POINTS)
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
            "--bucket-point",
            "bbox",
        ],
    )
    assert result.exit_code == 0, result.output
    assert out.exists()


@pytest.mark.slow
@pytest.mark.network
def test_table_aggregate_a5_bucket_point_bbox():
    from geoparquet_io.api.table import Table

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy = true")
    tbl = (
        con.execute(
            """
            SELECT ST_AsWKB(ST_Point(lon, lat)) AS geometry,
                   {'xmin': lon - 0.001, 'ymin': lat - 0.001,
                    'xmax': lon + 0.001, 'ymax': lat + 0.001} AS bbox,
                   height
            FROM (VALUES (10.0, 50.0, 2.0), (-120.0, 40.0, 4.0)) AS t(lon, lat, height)
            """
        )
        .arrow()
        .read_all()
    )
    con.close()
    result = Table(tbl).aggregate_a5(resolution=5, bucket_point="bbox")
    assert sum(result.table.column("count").to_pylist()) == 2


@pytest.mark.slow
@pytest.mark.network
def test_aggregate_admin_bucket_point_bbox(tmp_path):
    from geoparquet_io.core.process.aggregate.by_admin import aggregate_by_admin

    src = tmp_path / "pts.parquet"
    out = tmp_path / "by_country.parquet"
    # Paris + Lyon (France) and one ocean point.
    _write_points_with_bbox(src, [(2.35, 48.85, 1.0), (4.85, 45.75, 2.0), (-30.0, 0.0, 3.0)])
    aggregate_by_admin(str(src), str(out), level="country", bucket_point="bbox")
    df = pq.read_table(out).to_pandas()
    assert int(df.loc[df["admin_code"] == "FR", "count"].iloc[0]) == 2
    assert int(df.loc[df["admin_code"] == "unassigned", "count"].iloc[0]) == 1
