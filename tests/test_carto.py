"""Tests for Carto SQL API extractor."""

import json
import tempfile
from pathlib import Path

import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.core.carto import (
    CartoError,
    _build_carto_query,
    _create_empty_geoparquet_table,
    _validate_carto_url,
    _validate_table_name,
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


class TestValidateTableName:
    """Tests for table name validation (SQL injection protection)."""

    def test_valid_simple_name(self):
        """Simple table name passes validation."""
        assert _validate_table_name("my_table") == "my_table"

    def test_valid_schema_qualified(self):
        """Schema-qualified name passes validation."""
        assert _validate_table_name("public.my_table") == "public.my_table"

    def test_valid_with_numbers(self):
        """Table name with numbers passes."""
        assert _validate_table_name("table_123") == "table_123"

    def test_invalid_sql_injection_semicolon(self):
        """Table name with semicolon is rejected."""
        with pytest.raises(InvalidParameterError, match="Invalid table name"):
            _validate_table_name("users; DROP TABLE users--")

    def test_invalid_sql_injection_quotes(self):
        """Table name with quotes is rejected."""
        with pytest.raises(InvalidParameterError, match="Invalid table name"):
            _validate_table_name("users' OR '1'='1")

    def test_invalid_spaces(self):
        """Table name with spaces is rejected."""
        with pytest.raises(InvalidParameterError, match="Invalid table name"):
            _validate_table_name("my table")

    def test_invalid_special_chars(self):
        """Table name with special characters is rejected."""
        with pytest.raises(InvalidParameterError, match="Invalid table name"):
            _validate_table_name("my-table")

    def test_invalid_starts_with_number(self):
        """Table name starting with number is rejected."""
        with pytest.raises(InvalidParameterError, match="Invalid table name"):
            _validate_table_name("123_table")


class TestBuildCartoQuery:
    """Tests for SQL query building."""

    def test_simple_query(self):
        """Basic query with just table name."""
        sql = _build_carto_query("my_table")
        # Table name should be quoted
        assert 'FROM "my_table"' in sql
        assert "SELECT *" in sql

    def test_with_columns(self):
        """Query with column selection."""
        sql = _build_carto_query("my_table", columns=["id", "name"])
        # Column names should be quoted
        assert '"id"' in sql
        assert '"name"' in sql
        assert '"the_geom"' in sql
        assert 'FROM "my_table"' in sql

    def test_columns_include_geom(self):
        """the_geom is not duplicated if already in columns."""
        sql = _build_carto_query("my_table", columns=["id", "the_geom"])
        # Should only have one the_geom
        assert sql.count('"the_geom"') == 1

    def test_with_where(self):
        """Query with WHERE clause."""
        sql = _build_carto_query("my_table", where="status = 'active'")
        assert "WHERE (status = 'active')" in sql

    def test_with_bbox(self):
        """Query with bounding box filter."""
        sql = _build_carto_query("my_table", bbox=(-75.2, 39.9, -75.1, 40.0))
        assert "ST_Intersects" in sql
        assert '"the_geom"' in sql
        assert "ST_MakeEnvelope(-75.2, 39.9, -75.1, 40.0, 4326)" in sql

    def test_with_limit(self):
        """Query with LIMIT clause."""
        sql = _build_carto_query("my_table", limit=100)
        assert "LIMIT 100" in sql

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


class TestEmptyGeoparquetTable:
    """Tests for empty table creation with proper metadata."""

    def test_empty_table_has_geometry_column(self):
        """Empty table has geometry column."""
        table = _create_empty_geoparquet_table()
        assert "geometry" in table.column_names
        assert table.num_rows == 0

    def test_empty_table_has_geo_metadata(self):
        """Empty table has valid GeoParquet metadata."""
        table = _create_empty_geoparquet_table()
        assert b"geo" in table.schema.metadata

        geo_meta = json.loads(table.schema.metadata[b"geo"])
        assert geo_meta["version"] == "1.1.0"
        assert geo_meta["primary_column"] == "geometry"
        assert "geometry" in geo_meta["columns"]

    def test_empty_table_has_crs(self):
        """Empty table has CRS metadata."""
        table = _create_empty_geoparquet_table()
        geo_meta = json.loads(table.schema.metadata[b"geo"])

        crs = geo_meta["columns"]["geometry"]["crs"]
        assert crs is not None
        # Should be OGC:CRS84
        assert crs["id"]["authority"] == "OGC"
        assert crs["id"]["code"] == "CRS84"

    def test_empty_table_respects_version(self):
        """Empty table uses specified GeoParquet version."""
        table = _create_empty_geoparquet_table(geoparquet_version="1.0.0")
        geo_meta = json.loads(table.schema.metadata[b"geo"])
        assert geo_meta["version"] == "1.0.0"


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

    def test_output_has_geoparquet_metadata(self):
        """Extracted table has valid GeoParquet metadata."""
        table = carto_to_table(
            url="https://phl.carto.com/api/v2/sql",
            table_name="opa_properties_public",
            limit=5,
        )
        assert b"geo" in table.schema.metadata

        geo_meta = json.loads(table.schema.metadata[b"geo"])
        assert geo_meta["version"] == "1.1.0"
        assert geo_meta["primary_column"] == "geometry"

    def test_output_has_correct_crs(self):
        """Extracted table has OGC:CRS84 CRS."""
        table = carto_to_table(
            url="https://phl.carto.com/api/v2/sql",
            table_name="opa_properties_public",
            limit=5,
        )
        geo_meta = json.loads(table.schema.metadata[b"geo"])
        crs = geo_meta["columns"]["geometry"]["crs"]

        assert crs["id"]["authority"] == "OGC"
        assert crs["id"]["code"] == "CRS84"

    def test_output_has_wkb_encoding(self):
        """Extracted table uses WKB encoding."""
        table = carto_to_table(
            url="https://phl.carto.com/api/v2/sql",
            table_name="opa_properties_public",
            limit=5,
        )
        geo_meta = json.loads(table.schema.metadata[b"geo"])
        assert geo_meta["columns"]["geometry"]["encoding"] == "WKB"

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
        with pytest.raises(CartoError):
            carto_to_table(
                url="https://phl.carto.com/api/v2/sql",
                table_name="nonexistent_table_12345",
                limit=1,
            )

    def test_sql_injection_table_name_rejected(self):
        """SQL injection in table name is rejected before request."""
        with pytest.raises(InvalidParameterError, match="Invalid table name"):
            carto_to_table(
                url="https://phl.carto.com/api/v2/sql",
                table_name="users; DROP TABLE opa_properties_public--",
                limit=1,
            )


@pytest.mark.network
class TestCartoCli:
    """CLI integration tests for Carto extraction."""

    def test_cli_basic_extraction(self):
        """Basic CLI extraction works."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            output_file = Path(tmpdir) / "output.parquet"
            result = runner.invoke(
                cli,
                [
                    "extract",
                    "carto",
                    "https://phl.carto.com/api/v2/sql",
                    "opa_properties_public",
                    str(output_file),
                    "--limit",
                    "5",
                    "--skip-hilbert",
                    "--skip-bbox",
                ],
            )
            assert result.exit_code == 0, result.output
            assert output_file.exists()

    def test_cli_with_filters(self):
        """CLI with --where and --bbox filters."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            output_file = Path(tmpdir) / "output.parquet"
            result = runner.invoke(
                cli,
                [
                    "extract",
                    "carto",
                    "https://phl.carto.com/api/v2/sql",
                    "opa_properties_public",
                    str(output_file),
                    "--where",
                    "category_code_description = 'SINGLE FAMILY'",
                    "--bbox",
                    "-75.2,39.9,-75.1,40.0",
                    "--limit",
                    "5",
                    "--skip-hilbert",
                    "--skip-bbox",
                ],
            )
            assert result.exit_code == 0, result.output

    def test_cli_invalid_bbox_format(self):
        """CLI rejects invalid bbox format."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            output_file = Path(tmpdir) / "output.parquet"
            result = runner.invoke(
                cli,
                [
                    "extract",
                    "carto",
                    "https://phl.carto.com/api/v2/sql",
                    "opa_properties_public",
                    str(output_file),
                    "--bbox",
                    "invalid,bbox",
                ],
            )
            assert result.exit_code != 0
            assert "Invalid bbox format" in result.output

    def test_cli_timeout_option(self):
        """CLI accepts --timeout option."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            output_file = Path(tmpdir) / "output.parquet"
            result = runner.invoke(
                cli,
                [
                    "extract",
                    "carto",
                    "https://phl.carto.com/api/v2/sql",
                    "opa_properties_public",
                    str(output_file),
                    "--limit",
                    "5",
                    "--timeout",
                    "60",
                    "--skip-hilbert",
                    "--skip-bbox",
                ],
            )
            assert result.exit_code == 0, result.output

    def test_cli_help(self):
        """CLI help shows all options."""
        runner = CliRunner()
        result = runner.invoke(cli, ["extract", "carto", "--help"])
        assert result.exit_code == 0
        assert "--where" in result.output
        assert "--bbox" in result.output
        assert "--limit" in result.output
        assert "--timeout" in result.output
        assert "--include-cols" in result.output
        assert "--exclude-cols" in result.output
        assert "CARTO_API_KEY" in result.output

    def test_cli_invalid_table_name(self):
        """CLI rejects invalid table names."""
        runner = CliRunner()
        with tempfile.TemporaryDirectory() as tmpdir:
            output_file = Path(tmpdir) / "output.parquet"
            result = runner.invoke(
                cli,
                [
                    "extract",
                    "carto",
                    "https://phl.carto.com/api/v2/sql",
                    "users; DROP TABLE x--",
                    str(output_file),
                ],
            )
            assert result.exit_code != 0
            assert "Invalid table name" in result.output
