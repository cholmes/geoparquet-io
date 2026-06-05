"""Tests for spatial join deduplication of border-straddling features."""

import duckdb
import pytest

from geoparquet_io.core.add.admin_divisions import (
    _build_spatial_join_query as admin_build_query,
)
from geoparquet_io.core.add.country_codes import (
    _build_spatial_join_query as country_build_query,
)


class TestBuildSpatialJoinQueryDedup:
    """Test that _build_spatial_join_query generates dedup SQL by default."""

    def _build_simple_query(self, deduplicate=True):
        return admin_build_query(
            input_url="input.parquet",
            admin_subquery="(SELECT * FROM admin) b_sub",
            admin_select_clause='b."country" as "gaul_country"',
            input_bbox_col=None,
            admin_bbox_col=None,
            input_geom_col="geometry",
            admin_geom_col="geometry",
            deduplicate=deduplicate,
        )

    def test_default_query_contains_dedup(self):
        """Default query should deduplicate border-straddling features."""
        query = self._build_simple_query(deduplicate=True)
        assert "ROW_NUMBER()" in query
        assert "QUALIFY" in query
        assert "ST_Area" in query
        assert "ST_Intersection" in query
        assert "__gpio_dedup_rownum__" in query

    def test_all_matches_query_has_no_dedup(self):
        """With deduplicate=False, query should not contain dedup logic."""
        query = self._build_simple_query(deduplicate=False)
        assert "ROW_NUMBER()" not in query
        assert "QUALIFY" not in query
        assert "__gpio_dedup_rownum__" not in query
        # Should still have the spatial join
        assert "ST_Intersects" in query

    def test_dedup_query_excludes_internal_columns(self):
        """Dedup query should not expose __gpio_dedup_rownum__ in final output."""
        query = self._build_simple_query(deduplicate=True)
        assert "EXCLUDE" in query
        assert "__gpio_dedup_rownum__" in query

    def test_dedup_with_bbox_optimization(self):
        """Dedup should work alongside bbox pre-filtering."""
        query = admin_build_query(
            input_url="input.parquet",
            admin_subquery="(SELECT * FROM admin) b_sub",
            admin_select_clause='b."country" as "gaul_country"',
            input_bbox_col="bbox",
            admin_bbox_col="geometry_bbox",
            input_geom_col="geometry",
            admin_geom_col="geometry",
            deduplicate=True,
        )
        assert "ROW_NUMBER()" in query
        assert "QUALIFY" in query
        assert "bbox" in query
        assert "ST_Intersects" in query


class TestCountryCodesBuildSpatialJoinQueryDedup:
    """Test that country_codes._build_spatial_join_query generates dedup SQL by default."""

    def _build_simple_query(self, deduplicate=True):
        return country_build_query(
            input_url="input.parquet",
            countries_source="filtered_countries",
            select_clause='b."country" as "admin:country_code"',
            input_geom_col="geometry",
            countries_geom_col="geometry",
            input_bbox_col=None,
            countries_bbox_col=None,
            deduplicate=deduplicate,
        )

    def test_default_query_contains_dedup(self):
        """Default query should deduplicate border-straddling features."""
        query = self._build_simple_query(deduplicate=True)
        assert "ROW_NUMBER()" in query
        assert "QUALIFY" in query
        assert "ST_Area" in query
        assert "__gpio_dedup_rownum__" in query

    def test_all_matches_query_has_no_dedup(self):
        """With deduplicate=False, query should not contain dedup logic."""
        query = self._build_simple_query(deduplicate=False)
        assert "ROW_NUMBER()" not in query
        assert "QUALIFY" not in query
        assert "__gpio_dedup_rownum__" not in query
        assert "ST_Intersects" in query

    def test_dedup_with_native_geo(self):
        """Dedup should work alongside native geometry bbox pre-filtering."""
        query = country_build_query(
            input_url="input.parquet",
            countries_source="filtered_countries",
            select_clause='b."country" as "admin:country_code"',
            input_geom_col="geometry",
            countries_geom_col="geometry",
            input_bbox_col=None,
            countries_bbox_col="bbox",
            input_has_native_geo=True,
            deduplicate=True,
        )
        assert "ROW_NUMBER()" in query
        assert "QUALIFY" in query
        assert "ST_XMin" in query
        assert "ST_Intersects" in query


class TestSpatialJoinDedupExecution:
    """Test actual dedup behavior with synthetic DuckDB data."""

    @pytest.fixture
    def con(self):
        """Create a DuckDB connection with spatial extension."""
        conn = duckdb.connect()
        conn.execute("INSTALL spatial; LOAD spatial;")
        return conn

    @pytest.fixture
    def setup_test_data(self, con):
        """Create two adjacent admin polygons and input features.

        Admin layout (two countries sharing a border at x=5):
          Country A: box (0,0)-(5,10)
          Country B: box (5,0)-(10,10)

        Input features:
          Feature 1: box (1,1)-(4,4) — fully inside Country A
          Feature 2: box (3,3)-(8,8) — straddles border, more in Country B
          Feature 3: box (6,6)-(9,9) — fully inside Country B
        """
        con.execute("""
            CREATE TABLE admin_boundaries AS
            SELECT 'Country A' as country, ST_GeomFromText('POLYGON((0 0, 5 0, 5 10, 0 10, 0 0))') as geometry
            UNION ALL
            SELECT 'Country B', ST_GeomFromText('POLYGON((5 0, 10 0, 10 10, 5 10, 5 0))')
        """)

        con.execute("""
            CREATE TABLE input_features AS
            SELECT 1 as id, ST_GeomFromText('POLYGON((1 1, 4 1, 4 4, 1 4, 1 1))') as geometry
            UNION ALL
            SELECT 2, ST_GeomFromText('POLYGON((3 3, 8 3, 8 8, 3 8, 3 3))')
            UNION ALL
            SELECT 3, ST_GeomFromText('POLYGON((6 6, 9 6, 9 9, 6 9, 6 6))')
        """)
        return con

    def test_without_dedup_produces_duplicates(self, setup_test_data):
        """Without dedup, border-straddling feature 2 matches both countries."""
        con = setup_test_data
        result = con.execute("""
            SELECT a.id, b.country
            FROM input_features a
            LEFT JOIN admin_boundaries b
            ON ST_Intersects(b.geometry, a.geometry)
            ORDER BY a.id, b.country
        """).fetchall()

        assert len(result) == 4  # Feature 2 matches both countries
        ids = [r[0] for r in result]
        assert ids.count(2) == 2  # Feature 2 appears twice

    def test_dedup_keeps_largest_overlap(self, setup_test_data):
        """Dedup should keep only the country with the largest overlap area.

        Feature 2 is box (3,3)-(8,8):
          Overlap with Country A (x=0..5): area of (3,3)-(5,8) = 2*5 = 10
          Overlap with Country B (x=5..10): area of (5,3)-(8,8) = 3*5 = 15
        So Country B should be kept.
        """
        con = setup_test_data
        result = con.execute("""
            WITH _gpio_input AS (
                SELECT *, ROW_NUMBER() OVER () AS __gpio_dedup_rownum__
                FROM input_features
            )
            SELECT * EXCLUDE (__gpio_dedup_rownum__) FROM (
                SELECT
                    a.*,
                    b.country
                FROM _gpio_input a
                LEFT JOIN admin_boundaries b
                ON ST_Intersects(b.geometry, a.geometry)
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY a.__gpio_dedup_rownum__
                    ORDER BY ST_Area(ST_Intersection(b.geometry, a.geometry)) DESC NULLS LAST
                ) = 1
            )
            ORDER BY id
        """).fetchall()

        assert len(result) == 3  # One row per input feature
        # Columns: id, geometry (WKB bytes), country
        countries = {r[0]: r[2] for r in result}
        assert countries[1] == "Country A"
        assert countries[2] == "Country B"  # Larger overlap
        assert countries[3] == "Country B"

    def test_dedup_preserves_unmatched_rows(self, setup_test_data):
        """Features with no admin match should still appear (LEFT JOIN)."""
        con = setup_test_data
        # Add a feature outside both admin boundaries
        con.execute("""
            INSERT INTO input_features
            VALUES (4, ST_GeomFromText('POLYGON((20 20, 21 20, 21 21, 20 21, 20 20))'))
        """)

        result = con.execute("""
            WITH _gpio_input AS (
                SELECT *, ROW_NUMBER() OVER () AS __gpio_dedup_rownum__
                FROM input_features
            )
            SELECT * EXCLUDE (__gpio_dedup_rownum__) FROM (
                SELECT
                    a.*,
                    b.country
                FROM _gpio_input a
                LEFT JOIN admin_boundaries b
                ON ST_Intersects(b.geometry, a.geometry)
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY a.__gpio_dedup_rownum__
                    ORDER BY ST_Area(ST_Intersection(b.geometry, a.geometry)) DESC NULLS LAST
                ) = 1
            )
            ORDER BY id
        """).fetchall()

        assert len(result) == 4
        # Columns: id, geometry (WKB bytes), country
        assert result[3][0] == 4  # Feature 4 present
        assert result[3][2] is None  # No country match


class TestDryRunDedup:
    """Test that CLI dry-run output reflects dedup behavior."""

    def test_dry_run_default_shows_dedup(self, buildings_test_file):
        """Default dry-run should show dedup SQL (QUALIFY, ROW_NUMBER)."""
        from click.testing import CliRunner

        from geoparquet_io.cli.main import add

        runner = CliRunner()
        result = runner.invoke(
            add,
            ["admin-divisions", buildings_test_file, "output.parquet", "--dry-run", "--no-cache"],
        )

        assert result.exit_code == 0
        assert "QUALIFY" in result.output
        assert "ROW_NUMBER" in result.output
        assert "ST_Area" in result.output

    def test_dry_run_all_matches_no_dedup(self, buildings_test_file):
        """With --all-matches, dry-run should not show dedup SQL."""
        from click.testing import CliRunner

        from geoparquet_io.cli.main import add

        runner = CliRunner()
        result = runner.invoke(
            add,
            [
                "admin-divisions",
                buildings_test_file,
                "output.parquet",
                "--dry-run",
                "--no-cache",
                "--all-matches",
            ],
        )

        assert result.exit_code == 0
        assert "QUALIFY" not in result.output
        assert "ROW_NUMBER" not in result.output
        assert "ST_Intersects" in result.output
