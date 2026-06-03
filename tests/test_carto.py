"""Tests for Carto SQL API extractor."""

import pytest

from geoparquet_io.core.carto import (
    CartoError,
    _build_carto_query,
    _validate_carto_url,
    carto_to_table,
)
from geoparquet_io.core.common import InvalidParameterError


class TestValidateCartoUrl:
    """Tests for URL validation."""

    def test_valid_full_url(self):
        """Full SQL API URL passes validation."""
        url = _validate_carto_url("https://phl.carto.com/api/v2/sql")
        assert url == "https://phl.carto.com/api/v2/sql"

    def test_valid_v1_url(self):
        """V1 API URL is also valid."""
        url = _validate_carto_url("https://example.carto.com/api/v1/sql")
        assert url == "https://example.carto.com/api/v1/sql"

    def test_base_domain_gets_api_path(self):
        """Base domain gets /api/v2/sql appended."""
        url = _validate_carto_url("https://phl.carto.com")
        assert url == "https://phl.carto.com/api/v2/sql"

    def test_trailing_slash_removed(self):
        """Trailing slashes are stripped."""
        url = _validate_carto_url("https://phl.carto.com/api/v2/sql/")
        assert url == "https://phl.carto.com/api/v2/sql"

    def test_missing_scheme_raises(self):
        """URL without scheme raises error."""
        with pytest.raises(InvalidParameterError, match="Must include scheme"):
            _validate_carto_url("phl.carto.com/api/v2/sql")

    def test_invalid_path_raises(self):
        """Invalid path raises error."""
        with pytest.raises(InvalidParameterError, match="Invalid Carto SQL API URL"):
            _validate_carto_url("https://phl.carto.com/some/other/path")


class TestBuildCartoQuery:
    """Tests for SQL query building."""

    def test_simple_query(self):
        """Basic query with just table name."""
        sql = _build_carto_query("my_table")
        assert sql == "SELECT * FROM my_table"

    def test_with_columns(self):
        """Query with column selection."""
        sql = _build_carto_query("my_table", columns=["id", "name"])
        assert sql == "SELECT id, name, the_geom FROM my_table"

    def test_columns_include_geom(self):
        """the_geom is not duplicated if already in columns."""
        sql = _build_carto_query("my_table", columns=["id", "the_geom"])
        assert sql == "SELECT id, the_geom FROM my_table"

    def test_with_where(self):
        """Query with WHERE clause."""
        sql = _build_carto_query("my_table", where="status = 'active'")
        assert sql == "SELECT * FROM my_table WHERE (status = 'active')"

    def test_with_bbox(self):
        """Query with bounding box filter."""
        sql = _build_carto_query("my_table", bbox=(-75.2, 39.9, -75.1, 40.0))
        assert "ST_Intersects" in sql
        assert "ST_MakeEnvelope(-75.2, 39.9, -75.1, 40.0, 4326)" in sql

    def test_with_limit(self):
        """Query with LIMIT clause."""
        sql = _build_carto_query("my_table", limit=100)
        assert sql == "SELECT * FROM my_table LIMIT 100"

    def test_combined_filters(self):
        """Query with WHERE, bbox, and limit."""
        sql = _build_carto_query(
            "my_table",
            where="status = 'active'",
            bbox=(-75.2, 39.9, -75.1, 40.0),
            limit=100,
        )
        assert "WHERE (status = 'active') AND ST_Intersects" in sql
        assert "LIMIT 100" in sql


@pytest.mark.network
class TestCartoToTable:
    """Integration tests for Carto extraction (requires network)."""

    def test_basic_extraction(self):
        """Extract a small sample from Philadelphia Carto."""
        table = carto_to_table(
            url="https://phl.carto.com/api/v2/sql",
            table_name="opa_properties_public",
            limit=10,
        )
        assert table.num_rows == 10
        assert "geometry" in table.column_names
        # Verify the_geom was renamed to geometry
        assert "the_geom" not in table.column_names

    def test_with_where_filter(self):
        """Extract with WHERE filter."""
        table = carto_to_table(
            url="https://phl.carto.com/api/v2/sql",
            table_name="opa_properties_public",
            where="category_code_description = 'SINGLE FAMILY'",
            limit=5,
        )
        assert table.num_rows == 5

    def test_with_bbox_filter(self):
        """Extract with bounding box filter."""
        table = carto_to_table(
            url="https://phl.carto.com/api/v2/sql",
            table_name="opa_properties_public",
            bbox=(-75.18, 39.95, -75.15, 39.97),
            limit=10,
        )
        assert table.num_rows <= 10
        assert "geometry" in table.column_names

    def test_include_cols(self):
        """Extract with column selection."""
        table = carto_to_table(
            url="https://phl.carto.com/api/v2/sql",
            table_name="opa_properties_public",
            include_cols="cartodb_id,parcel_number,market_value",
            limit=5,
        )
        # Should have requested columns plus geometry
        assert "cartodb_id" in table.column_names
        assert "parcel_number" in table.column_names
        assert "market_value" in table.column_names
        assert "geometry" in table.column_names

    def test_exclude_cols(self):
        """Extract with column exclusion."""
        table = carto_to_table(
            url="https://phl.carto.com/api/v2/sql",
            table_name="opa_properties_public",
            exclude_cols="cartodb_id",
            limit=5,
        )
        assert "cartodb_id" not in table.column_names
        assert "geometry" in table.column_names

    def test_base_domain_url(self):
        """URL without /api/v2/sql works."""
        table = carto_to_table(
            url="https://phl.carto.com",
            table_name="opa_properties_public",
            limit=5,
        )
        assert table.num_rows == 5

    def test_invalid_table_raises(self):
        """Non-existent table raises CartoError."""
        with pytest.raises(CartoError, match="Failed to fetch data from Carto"):
            carto_to_table(
                url="https://phl.carto.com/api/v2/sql",
                table_name="nonexistent_table_12345",
                limit=1,
            )
