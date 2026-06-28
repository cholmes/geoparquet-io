#!/usr/bin/env python3

"""Tests for the shared CRS-normalization helpers (issue #525).

Spatial operations (admin joins, lon/lat grid keying) assume OGC:CRS84 input.
These helpers produce the SQL needed to reproject non-CRS84 input to the
operation's expected CRS before the spatial work happens.
"""

import pytest

from geoparquet_io.core.crs_utils import (
    crs_string_for_transform,
    source_crs_string,
    transform_geom_sql,
)
from geoparquet_io.core.duckdb_utils import get_duckdb_connection

EPSG_28992 = {"id": {"authority": "EPSG", "code": 28992}}


class TestCrsStringForTransform:
    def test_projected_dict_returns_auth_code(self):
        assert crs_string_for_transform(EPSG_28992) == "EPSG:28992"

    def test_projected_string_returns_auth_code(self):
        assert crs_string_for_transform("EPSG:28992") == "EPSG:28992"

    def test_none_returns_none(self):
        assert crs_string_for_transform(None) is None

    @pytest.mark.parametrize(
        "crs", ["EPSG:4326", "OGC:CRS84", {"id": {"authority": "OGC", "code": "CRS84"}}]
    )
    def test_default_crs_returns_none(self, crs):
        # The default CRS needs no transform when the target is lon/lat.
        assert crs_string_for_transform(crs) is None


class TestTransformGeomSql:
    def test_noop_for_none_source(self):
        assert transform_geom_sql('"geom"', None) == '"geom"'

    def test_noop_for_default_source(self):
        assert transform_geom_sql('"geom"', "EPSG:4326") == '"geom"'

    def test_projected_source_emits_st_transform(self):
        assert (
            transform_geom_sql('"geom"', EPSG_28992)
            == "ST_Transform(\"geom\", 'EPSG:28992', 'OGC:CRS84')"
        )

    def test_custom_target_crs(self):
        assert (
            transform_geom_sql("g", "EPSG:28992", target_crs="EPSG:3857")
            == "ST_Transform(g, 'EPSG:28992', 'EPSG:3857')"
        )

    def test_transform_runs_in_duckdb_and_yields_lonlat(self, fields_5070_file):
        """A projected (EPSG:5070) geometry transforms to lon/lat degrees."""
        src = source_crs_string(fields_5070_file)
        assert src == "EPSG:5070"
        expr = transform_geom_sql('"geometry"', src)
        con = get_duckdb_connection(load_spatial=True)
        try:
            con.execute("SET geometry_always_xy = true;")
            raw_x, lon, lat = con.execute(
                f'SELECT ST_X(ST_Centroid("geometry")), '
                f"ST_X(ST_Centroid({expr})), ST_Y(ST_Centroid({expr})) "
                f"FROM '{fields_5070_file}' LIMIT 1"
            ).fetchone()
        finally:
            con.close()
        # Raw coords are projected metres (millions); transformed are valid degrees.
        assert abs(raw_x) > 1000
        assert -180 <= lon <= 180
        assert -90 <= lat <= 90


class TestSourceCrsString:
    def test_detects_projected_fixture(self, fields_5070_file):
        assert source_crs_string(fields_5070_file) == "EPSG:5070"

    def test_default_crs_input_needs_no_transform(self, buildings_test_file):
        # The common case: a CRS84 input is not reprojected (hot path stays free).
        assert source_crs_string(buildings_test_file) is None


class TestHotPathUnchanged:
    """Regression guard (#525 perf): CRS84 input must not pay for reprojection.

    The reprojection is keyed off a detected source CRS. For default/CRS84 input
    the join SQL must keep its bbox pre-filter and contain no ST_Transform, so the
    common path is byte-for-byte what it was before this change.
    """

    def test_join_condition_keeps_bbox_prefilter_without_transform(self):
        from geoparquet_io.core.duckdb_utils import build_spatial_join_condition

        sql = build_spatial_join_condition("geom", "geom", "bbox", "bbox")
        assert "ST_Transform" not in sql
        assert ".xmin" in sql and ".xmax" in sql  # bbox pre-filter retained

    def test_join_condition_reprojects_and_drops_bbox_when_transforming(self):
        from geoparquet_io.core.duckdb_utils import build_spatial_join_condition

        sql = build_spatial_join_condition(
            "geom",
            "geom",
            "bbox",
            "bbox",
            input_geom_sql="ST_Transform(a.geom, 'EPSG:5070', 'OGC:CRS84')",
        )
        assert "ST_Transform" in sql
        assert ".xmin" not in sql  # bbox pre-filter skipped (source-CRS bbox)


def _read_column(parquet_file, column):
    con = get_duckdb_connection(load_spatial=True)
    try:
        rows = con.execute(f"SELECT \"{column}\" FROM '{parquet_file}' ORDER BY 1").fetchall()
    finally:
        con.close()
    return [r[0] for r in rows]


class TestGridKeyingIsCrsAware:
    """Class B (#525): lon/lat grid keying must reproject projected input.

    Keying a projected (EPSG:5070) file must yield the same cells as keying a
    reprojected-to-CRS84 copy of the same data. Before the fix the projected
    metres were fed to ``*_lonlat_to_cell`` as degrees, producing different
    (wrong) cells.
    """

    @pytest.fixture
    def reprojected_4326(self, fields_5070_file, tmp_path):
        from geoparquet_io.core.reproject import reproject

        out = tmp_path / "fields_4326.parquet"
        reproject(fields_5070_file, str(out), target_crs="EPSG:4326")
        return str(out)

    @pytest.mark.parametrize(
        "module,func_name,column,res_kw",
        [
            ("a5", "add_a5_column", "a5_cell", {"a5_resolution": 12}),
            ("h3", "add_h3_column", "h3_cell", {"h3_resolution": 9}),
            ("s2", "add_s2_column", "s2_cell", {"s2_level": 14}),
            ("quadkey", "add_quadkey_column", "quadkey", {"resolution": 16}),
        ],
    )
    def test_projected_cells_match_reprojected(
        self, fields_5070_file, reprojected_4326, tmp_path, module, func_name, column, res_kw
    ):
        import importlib

        mod = importlib.import_module(f"geoparquet_io.core.add.{module}")
        add_func = getattr(mod, func_name)

        proj_out = tmp_path / f"proj_{module}.parquet"
        wgs_out = tmp_path / f"wgs_{module}.parquet"
        add_func(fields_5070_file, str(proj_out), **res_kw)
        add_func(reprojected_4326, str(wgs_out), **res_kw)

        proj_cells = _read_column(str(proj_out), column)
        wgs_cells = _read_column(str(wgs_out), column)

        # Keying the projected file (now reprojected internally) must agree with
        # keying a reprojected copy. A few fine-resolution boundary cells can flip
        # due to the reproject-to-file coordinate rounding in the oracle, so allow
        # a tiny mismatch — without the fix, ~every cell differs (metres keyed as
        # degrees), so this still cleanly distinguishes fixed from broken.
        assert len(proj_cells) == len(wgs_cells)
        mismatches = sum(1 for a, b in zip(proj_cells, wgs_cells, strict=True) if a != b)
        assert mismatches <= max(1, len(proj_cells) // 100)
        # And the keying actually produced a cell for every row (not null).
        assert all(c is not None for c in proj_cells)


def _con_with_admin():
    """Connection with a CRS84 admin polygon covering the 5070 fixture's extent.

    The fixture's geometries reproject to roughly lon 18 / lat 47, so the admin
    polygon spans (17..19, 46..48).
    """
    con = get_duckdb_connection(load_spatial=True)
    con.execute("SET geometry_always_xy = true;")
    con.execute(
        "CREATE TEMP TABLE _admin AS "
        "SELECT ST_GeomFromText('POLYGON((17 46, 19 46, 19 48, 17 48, 17 46))') AS geom, "
        "'R1' AS region"
    )
    return con


class TestAdminJoinIsCrsAware:
    """Class A (#525): admin spatial joins must reproject projected input.

    Without reprojection a projected (EPSG:5070) input either errors on a CRS
    mismatch or silently matches nothing (metres compared against degrees).
    """

    def test_admin_divisions_join_assigns_with_reprojection(self, fields_5070_file):
        from geoparquet_io.core.add.admin_divisions import _build_spatial_join_query

        con = _con_with_admin()
        try:
            kwargs = {
                "input_url": fields_5070_file,
                "admin_subquery": "(SELECT geom, region FROM _admin)",
                "admin_select_clause": 'b."region" AS region',
                "input_bbox_col": None,
                "admin_bbox_col": None,
                "input_geom_col": "geometry",
                "admin_geom_col": "geom",
            }
            with_crs = _build_spatial_join_query(source_crs="EPSG:5070", **kwargs)
            without = _build_spatial_join_query(source_crs=None, **kwargs)
            assigned = con.execute(
                f"SELECT COUNT(*) FROM ({with_crs}) WHERE region IS NOT NULL"
            ).fetchone()[0]
            not_assigned = con.execute(
                f"SELECT COUNT(*) FROM ({without}) WHERE region IS NOT NULL"
            ).fetchone()[0]
        finally:
            con.close()
        assert assigned > 0  # reprojected input intersects the admin polygon
        assert not_assigned == 0  # raw projected metres match nothing

    def test_admin_hierarchical_enrichment_assigns_with_reprojection(self, fields_5070_file):
        from geoparquet_io.core.partition.admin_hierarchical import _build_enrichment_query

        con = _con_with_admin()
        try:
            sql = _build_enrichment_query(
                input_url=fields_5070_file,
                admin_table_ref="_admin",
                admin_where_clause="",
                admin_select_clause='b."region" AS region',
                admin_geom_col="geom",
                admin_bbox_col=None,
                boundary_columns=["region"],
                input_geom_col="geometry",
                input_bbox_col=None,
                enriched_table="_enriched",
                source_crs="EPSG:5070",
            )
            con.execute(sql)
            assigned = con.execute(
                "SELECT COUNT(*) FROM _enriched WHERE region IS NOT NULL"
            ).fetchone()[0]
        finally:
            con.close()
        assert assigned > 0
