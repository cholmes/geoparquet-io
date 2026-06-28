#!/usr/bin/env python3

"""CRS-awareness for `gpio process aggregate` (#525 follow-up).

Aggregation carried the same OGC:CRS84 assumption as the add/partition grid and
admin operations. These tests pin that projected input is reprojected before
grid keying and the admin spatial join, mirroring tests/test_crs_normalize.py.
"""

import pytest

from geoparquet_io.core.crs_utils import source_crs_string, transform_geom_sql
from geoparquet_io.core.duckdb_utils import get_duckdb_connection


def _read_column(parquet_file, column):
    con = get_duckdb_connection(load_spatial=True)
    try:
        rows = con.execute(f"SELECT \"{column}\" FROM '{parquet_file}' ORDER BY 1").fetchall()
    finally:
        con.close()
    return [r[0] for r in rows]


class TestGridAggregateIsCrsAware:
    """Aggregating a projected file must key the same cells as a CRS84 copy."""

    @pytest.fixture
    def reprojected_4326(self, fields_5070_file, tmp_path):
        from geoparquet_io.core.reproject import reproject

        out = tmp_path / "fields_4326.parquet"
        reproject(fields_5070_file, str(out), target_crs="EPSG:4326")
        return str(out)

    def test_a5_aggregate_cells_match_reprojected(
        self, fields_5070_file, reprojected_4326, tmp_path
    ):
        from geoparquet_io.core.process.aggregate.by_a5 import aggregate_by_a5

        proj_out = tmp_path / "proj.parquet"
        wgs_out = tmp_path / "wgs.parquet"
        aggregate_by_a5(fields_5070_file, str(proj_out), resolution=10, out_geometry="none")
        aggregate_by_a5(reprojected_4326, str(wgs_out), resolution=10, out_geometry="none")

        proj_cells = set(_read_column(str(proj_out), "a5_cell"))
        wgs_cells = set(_read_column(str(wgs_out), "a5_cell"))
        # Same geographic input -> same A5 cells once the projected file is
        # reprojected internally. (Cell-set equality is robust at this resolution.)
        assert proj_cells == wgs_cells
        assert proj_cells  # non-empty


def _con_with_admin():
    con = get_duckdb_connection(load_spatial=True)
    con.execute("SET geometry_always_xy = true;")
    con.execute(
        "CREATE TEMP TABLE _admin AS "
        "SELECT ST_GeomFromText('POLYGON((17 46, 19 46, 19 48, 17 48, 17 46))') AS geom, "
        "'R1' AS region"
    )
    return con


class TestAdminAggregateIsCrsAware:
    """The admin aggregate join must reproject projected input before ST_Intersects."""

    def test_joined_sql_assigns_with_reprojection(self, fields_5070_file):
        from geoparquet_io.core.process.aggregate.by_admin import _build_joined_sql

        con = _con_with_admin()
        try:
            with_crs = _build_joined_sql(
                fields_5070_file,
                transform_geom_sql('"geometry"', "EPSG:5070"),
                "_admin",
                "region",
                "region",
                "geom",
            )
            without = _build_joined_sql(
                fields_5070_file, '"geometry"', "_admin", "region", "region", "geom"
            )
            assigned = con.execute(f"SELECT COUNT(__admin_code) FROM ({with_crs})").fetchone()[0]
            not_assigned = con.execute(f"SELECT COUNT(__admin_code) FROM ({without})").fetchone()[0]
        finally:
            con.close()
        assert assigned > 0
        assert not_assigned == 0


def test_source_crs_detected_for_aggregate_input(fields_5070_file):
    assert source_crs_string(fields_5070_file) == "EPSG:5070"
