"""
Tests for the extract bigquery command and API.

Tests are organized into:
- Unit tests (no BigQuery access needed): CLI structure, dry-run, backwards compatibility
- Integration tests (marked @pytest.mark.network): require BQ_TEST_TABLE env var
"""

from __future__ import annotations

import os
import tempfile
import uuid
from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import extract
from tests.conftest import safe_unlink


class TestExtractCommandGroup:
    """Test that extract command group works correctly."""

    def test_extract_help_shows_subcommands(self):
        """Test that extract --help shows both subcommands."""
        runner = CliRunner()
        result = runner.invoke(extract, ["--help"])
        assert result.exit_code == 0
        assert "geoparquet" in result.output
        assert "bigquery" in result.output

    def test_extract_bigquery_help(self):
        """Test that extract bigquery --help works."""
        runner = CliRunner()
        result = runner.invoke(extract, ["bigquery", "--help"])
        assert result.exit_code == 0
        assert "TABLE_ID" in result.output
        assert "--project" in result.output
        assert "--credentials-file" in result.output
        assert "--where" in result.output
        assert "--limit" in result.output


class TestBackwardsCompatibility:
    """Test that 'gpio extract input.parquet output.parquet' still works."""

    @pytest.fixture
    def input_file(self):
        """Create a test parquet file."""
        # Create a simple test parquet file
        schema = pa.schema(
            [
                pa.field("id", pa.int32()),
                pa.field("name", pa.string()),
            ]
        )
        table = pa.table({"id": [1, 2, 3], "name": ["a", "b", "c"]}, schema=schema)
        tmp_path = Path(tempfile.gettempdir()) / f"test_input_{uuid.uuid4()}.parquet"
        pq.write_table(table, str(tmp_path))
        yield str(tmp_path)
        safe_unlink(tmp_path)

    @pytest.fixture
    def output_file(self):
        """Create a temporary output file path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_output_{uuid.uuid4()}.parquet"
        yield str(tmp_path)
        safe_unlink(tmp_path)

    def test_extract_without_subcommand(self, input_file, output_file):
        """Test that extract without explicit subcommand defaults to geoparquet."""
        runner = CliRunner()
        result = runner.invoke(extract, [input_file, output_file])
        # Should work (exit 0) or give appropriate error
        # The key is it shouldn't fail with "Unknown subcommand"
        assert "Unknown command" not in result.output


class TestDetectGeometryColumn:
    """Test geometry column detection function."""

    def test_detect_common_names(self):
        """Test detection of common geometry column names."""
        from geoparquet_io.core.extract_bigquery import _detect_geometry_column

        # Create table with "geometry" column
        table = pa.table(
            {
                "id": [1, 2],
                "geometry": [b"test", b"test2"],
            }
        )
        assert _detect_geometry_column(table) == "geometry"

        # Create table with "geography" column
        table = pa.table(
            {
                "id": [1, 2],
                "geography": [b"test", b"test2"],
            }
        )
        assert _detect_geometry_column(table) == "geography"

        # Create table with "geom" column
        table = pa.table(
            {
                "id": [1, 2],
                "geom": [b"test", b"test2"],
            }
        )
        assert _detect_geometry_column(table) == "geom"

    def test_detect_fallback_to_geo_in_name(self):
        """Test fallback to columns containing 'geo' in name."""
        from geoparquet_io.core.extract_bigquery import _detect_geometry_column

        table = pa.table(
            {
                "id": [1, 2],
                "building_geom_wkb": [b"test", b"test2"],
            }
        )
        assert _detect_geometry_column(table) == "building_geom_wkb"

    def test_detect_returns_none_when_not_found(self):
        """Test that None is returned when no geometry column found."""
        from geoparquet_io.core.extract_bigquery import _detect_geometry_column

        table = pa.table(
            {
                "id": [1, 2],
                "name": ["a", "b"],
            }
        )
        assert _detect_geometry_column(table) is None


class TestExtractBigQueryTable:
    """Test the extract_bigquery_table function for in-memory tables."""

    def test_extract_with_limit(self):
        """Test extracting with a row limit."""
        from geoparquet_io.core.extract_bigquery import extract_bigquery_table

        table = pa.table(
            {
                "id": [1, 2, 3, 4, 5],
                "name": ["a", "b", "c", "d", "e"],
            }
        )
        result = extract_bigquery_table(table, limit=3)
        assert result.num_rows == 3

    def test_extract_with_columns(self):
        """Test extracting specific columns."""
        from geoparquet_io.core.extract_bigquery import extract_bigquery_table

        table = pa.table(
            {
                "id": [1, 2, 3],
                "name": ["a", "b", "c"],
                "value": [10, 20, 30],
            }
        )
        result = extract_bigquery_table(table, columns=["id", "name"])
        assert result.column_names == ["id", "name"]

    def test_extract_with_exclude_columns(self):
        """Test excluding specific columns."""
        from geoparquet_io.core.extract_bigquery import extract_bigquery_table

        table = pa.table(
            {
                "id": [1, 2, 3],
                "name": ["a", "b", "c"],
                "value": [10, 20, 30],
            }
        )
        result = extract_bigquery_table(table, exclude_columns=["value"])
        assert "value" not in result.column_names
        assert "id" in result.column_names
        assert "name" in result.column_names


class TestBuildGeometrySelectExpr:
    """Test _build_geometry_select_expr handles different column types."""

    def test_varchar_type_uses_geomfromtext(self):
        """Test that VARCHAR columns get ST_GeomFromText wrapping."""
        from geoparquet_io.core.extract_bigquery import _build_geometry_select_expr

        expr = _build_geometry_select_expr("geometry", "VARCHAR")
        assert "ST_GeomFromText" in expr
        assert "ST_AsWKB" in expr
        assert '"geometry"' in expr

    def test_geometry_type_uses_direct_aswkb(self):
        """Test that GEOMETRY-typed columns use ST_AsWKB directly."""
        from geoparquet_io.core.extract_bigquery import _build_geometry_select_expr

        expr = _build_geometry_select_expr("geometry", "GEOMETRY")
        assert "ST_AsWKB" in expr
        assert "ST_GeomFromText" not in expr

    def test_string_type_uses_geomfromtext(self):
        """Test that STRING type (another non-GEOMETRY type) uses ST_GeomFromText."""
        from geoparquet_io.core.extract_bigquery import _build_geometry_select_expr

        expr = _build_geometry_select_expr("geom", "STRING")
        assert "ST_GeomFromText" in expr
        assert "ST_AsWKB" in expr

    def test_varchar_geometry_query_executes(self):
        """Test that the generated SQL for VARCHAR geometry actually works."""
        from geoparquet_io.core.extract_bigquery import _build_geometry_select_expr

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial")
        con.execute("CREATE TABLE test_exec AS SELECT 1 AS id, 'POINT(1.5 2.5)' AS geometry")

        expr = _build_geometry_select_expr("geometry", "VARCHAR")
        result = con.execute(f"SELECT {expr} FROM test_exec").fetchall()
        assert len(result) == 1
        # geometry column should be BLOB (WKB bytes)
        assert isinstance(result[0][0], bytes)
        con.close()

    def test_geojson_format(self):
        """Test that GeoJSON format uses ST_GeomFromGeoJSON."""
        from geoparquet_io.core.extract_bigquery import _build_geometry_select_expr

        expr = _build_geometry_select_expr("geom", "VARCHAR", geometry_format="geojson")
        assert "ST_GeomFromGeoJSON" in expr
        assert "ST_AsWKB" in expr
        assert "ST_GeomFromText" not in expr

    def test_geojson_format_query_executes(self):
        """Test that GeoJSON format SQL actually works."""
        from geoparquet_io.core.extract_bigquery import _build_geometry_select_expr

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial")
        con.execute(
            """CREATE TABLE test_geojson AS
            SELECT 1 AS id,
            '{"type":"Point","coordinates":[1.5,2.5]}' AS geom"""
        )

        expr = _build_geometry_select_expr("geom", "VARCHAR", geometry_format="geojson")
        result = con.execute(f"SELECT {expr} FROM test_geojson").fetchall()
        assert len(result) == 1
        assert isinstance(result[0][0], bytes)
        con.close()


class TestNoGeometryPlainParquet:
    """Test that tables without geometry are written as plain Parquet."""

    def test_no_geometry_detected_returns_none(self):
        """Test that _detect_geometry_column_from_schema returns None for non-GEOMETRY columns."""
        from geoparquet_io.core.extract_bigquery import _detect_geometry_column_from_schema

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial")
        # Table with a "geometry" column that is VARCHAR, not GEOMETRY type
        con.execute("CREATE TABLE test_no_geom AS SELECT 1 AS id, 'POINT(0 0)' AS geometry")

        # Should NOT detect "geometry" by name - only native GEOMETRY type
        result = _detect_geometry_column_from_schema(con, "test_no_geom", table_source="local")
        assert result is None
        con.close()

    def test_detects_native_geometry_type(self):
        """Test that native GEOMETRY columns are still detected."""
        from geoparquet_io.core.extract_bigquery import _detect_geometry_column_from_schema

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial")
        con.execute("CREATE TABLE test_native AS SELECT 1 AS id, ST_Point(0, 0) AS geometry")

        result = _detect_geometry_column_from_schema(con, "test_native", table_source="local")
        assert result == "geometry"
        con.close()

    def test_explicit_varchar_column_returns_none(self):
        """Test that specifying a VARCHAR column returns None (not error)."""
        from geoparquet_io.core.extract_bigquery import _detect_geometry_column_from_schema

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial")
        con.execute("CREATE TABLE test_varchar AS SELECT 1 AS id, 'POINT(0 0)' AS geom")

        # Should return None - column exists but isn't GEOMETRY type
        result = _detect_geometry_column_from_schema(
            con, "test_varchar", geography_column="geom", table_source="local"
        )
        assert result is None
        con.close()

    def test_nonexistent_column_raises_error(self):
        """Test that specifying a nonexistent column raises ValueError."""
        from geoparquet_io.core.extract_bigquery import _detect_geometry_column_from_schema

        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial")
        con.execute("CREATE TABLE test_noexist AS SELECT 1 AS id")

        with pytest.raises(ValueError, match="not found"):
            _detect_geometry_column_from_schema(
                con, "test_noexist", geography_column="geom", table_source="local"
            )
        con.close()


class TestEdgesParameter:
    """Test edge interpretation logic for native vs VARCHAR geometry columns."""

    def test_python_api_has_edges_parameter(self):
        """Test that Python API exposes edges parameter."""
        import inspect

        from geoparquet_io.api import Table, ops

        # Check Table.from_bigquery
        sig = inspect.signature(Table.from_bigquery)
        assert "edges" in sig.parameters
        assert "geography_column" in sig.parameters
        assert "geometry_format" in sig.parameters

        # Check ops.read_bigquery
        sig = inspect.signature(ops.read_bigquery)
        assert "edges" in sig.parameters
        assert "geography_column" in sig.parameters
        assert "geometry_format" in sig.parameters

    def test_python_api_parameter_defaults(self):
        """Test that Python API has correct default values."""
        import inspect

        from geoparquet_io.api import Table, ops

        # Check Table.from_bigquery defaults
        sig = inspect.signature(Table.from_bigquery)
        assert sig.parameters["edges"].default is None
        assert sig.parameters["geography_column"].default is None
        assert sig.parameters["geometry_format"].default == "wkt"

        # Check ops.read_bigquery defaults
        sig = inspect.signature(ops.read_bigquery)
        assert sig.parameters["edges"].default is None
        assert sig.parameters["geography_column"].default is None
        assert sig.parameters["geometry_format"].default == "wkt"

    def test_core_function_has_edges_parameter(self):
        """Test that core extract_bigquery function has edges parameter."""
        import inspect

        from geoparquet_io.core.extract_bigquery import extract_bigquery

        sig = inspect.signature(extract_bigquery)
        assert "edges" in sig.parameters
        assert sig.parameters["edges"].default is None


class TestColumnNameResolution:
    """Test column name case resolution for VARCHAR columns."""

    def test_resolve_column_name_case_insensitive(self):
        """Test that _resolve_column_name finds the actual schema spelling."""
        from geoparquet_io.core.extract_bigquery import _resolve_column_name

        con = duckdb.connect()
        con.execute("CREATE TABLE test_case AS SELECT 1 AS id, 'POINT(0 0)' AS Geometry")

        # Should resolve "geometry" to "Geometry" (actual schema spelling)
        resolved = _resolve_column_name(con, "test_case", "geometry", table_source="local")
        assert resolved == "Geometry"

        # Should resolve "GEOMETRY" to "Geometry"
        resolved = _resolve_column_name(con, "test_case", "GEOMETRY", table_source="local")
        assert resolved == "Geometry"

        con.close()

    def test_resolve_column_name_returns_input_if_not_found(self):
        """Test that _resolve_column_name returns input if column not found."""
        from geoparquet_io.core.extract_bigquery import _resolve_column_name

        con = duckdb.connect()
        con.execute("CREATE TABLE test_missing AS SELECT 1 AS id")

        resolved = _resolve_column_name(con, "test_missing", "nonexistent", table_source="local")
        assert resolved == "nonexistent"

        con.close()


class TestBboxFiltersWithVarchar:
    """Test bbox filter generation for VARCHAR WKT/GeoJSON columns."""

    def test_bbox_filter_native_geometry(self):
        """Test that native GEOMETRY columns use direct ST_INTERSECTS."""
        from geoparquet_io.core.extract_bigquery import _build_bbox_filters

        bq_filters, local_filters = _build_bbox_filters(
            "0,0,10,10", "geom", use_server_side=True, is_native_geometry=True
        )
        assert len(bq_filters) == 1
        assert "ST_INTERSECTS(geom," in bq_filters[0]
        assert "ST_GEOGFROMTEXT(geom" not in bq_filters[0]

    def test_bbox_filter_varchar_wkt(self):
        """Test that VARCHAR WKT columns wrap with ST_GEOGFROMTEXT."""
        from geoparquet_io.core.extract_bigquery import _build_bbox_filters

        bq_filters, local_filters = _build_bbox_filters(
            "0,0,10,10",
            "geom",
            use_server_side=True,
            is_native_geometry=False,
            geometry_format="wkt",
        )
        assert len(bq_filters) == 1
        assert "ST_GEOGFROMTEXT(geom)" in bq_filters[0]

    def test_bbox_filter_varchar_geojson(self):
        """Test that VARCHAR GeoJSON columns wrap with ST_GEOGFROMGEOJSON."""
        from geoparquet_io.core.extract_bigquery import _build_bbox_filters

        bq_filters, local_filters = _build_bbox_filters(
            "0,0,10,10",
            "geom",
            use_server_side=True,
            is_native_geometry=False,
            geometry_format="geojson",
        )
        assert len(bq_filters) == 1
        assert "ST_GEOGFROMGEOJSON(geom)" in bq_filters[0]

    def test_local_filter_varchar_wkt(self):
        """Test that local filtering wraps VARCHAR WKT with ST_GeomFromText."""
        from geoparquet_io.core.extract_bigquery import _build_bbox_filters

        bq_filters, local_filters = _build_bbox_filters(
            "0,0,10,10",
            "geom",
            use_server_side=False,
            is_native_geometry=False,
            geometry_format="wkt",
        )
        assert len(local_filters) == 1
        assert 'ST_GeomFromText("geom")' in local_filters[0]


class TestDryRun:
    """Test dry-run functionality."""

    def test_dry_run_shows_sql(self):
        """Test that dry-run shows SQL without executing."""
        from geoparquet_io.core.extract_bigquery import extract_bigquery

        # Dry-run should not raise an error even without credentials
        result = extract_bigquery(
            table_id="project.dataset.table",
            dry_run=True,
        )
        assert result is None


class TestPythonAPI:
    """Test the Python API for BigQuery."""

    def test_table_from_bigquery_exists(self):
        """Test that Table.from_bigquery method exists."""
        from geoparquet_io.api import Table

        assert hasattr(Table, "from_bigquery")
        assert callable(Table.from_bigquery)

    def test_ops_read_bigquery_exists(self):
        """Test that ops.read_bigquery function exists."""
        from geoparquet_io.api import ops

        assert hasattr(ops, "read_bigquery")
        assert callable(ops.read_bigquery)

    def test_read_bigquery_top_level_export(self):
        """Test that read_bigquery is exported at top level."""
        import geoparquet_io as gpio

        assert hasattr(gpio, "read_bigquery")
        assert callable(gpio.read_bigquery)


class TestBigQueryConnection:
    """Test BigQuery connection setup."""

    @patch("geoparquet_io.core.extract_bigquery.get_duckdb_connection")
    def test_connection_loads_extensions_in_order(self, mock_get_con):
        """Test that spatial is loaded before bigquery, and bigquery install uses try/except."""
        from geoparquet_io.core.extract_bigquery import get_bigquery_connection

        mock_con = MagicMock()
        mock_get_con.return_value = mock_con

        get_bigquery_connection()

        # Verify get_duckdb_connection was called with spatial=True
        mock_get_con.assert_called_once_with(load_spatial=True, load_httpfs=False)

        # Verify bigquery extension is loaded
        calls = [call[0][0] for call in mock_con.execute.call_args_list]
        load_bq_calls = [c for c in calls if "LOAD bigquery" in c]
        assert load_bq_calls, "LOAD bigquery should be called"

    @patch("geoparquet_io.core.extract_bigquery.get_duckdb_connection")
    def test_connection_no_deprecated_geography_setting(self, mock_get_con):
        """Test that deprecated bq_geography_as_geometry is NOT set (v1.5+)."""
        from geoparquet_io.core.extract_bigquery import get_bigquery_connection

        mock_con = MagicMock()
        mock_get_con.return_value = mock_con

        get_bigquery_connection()

        calls = [call[0][0] for call in mock_con.execute.call_args_list]
        geom_setting_calls = [c for c in calls if "geography_as_geometry" in c.lower()]
        assert not geom_setting_calls, (
            "bq_geography_as_geometry is deprecated in DuckDB 1.5 — should not be set"
        )

    @patch("geoparquet_io.core.extract_bigquery.get_duckdb_connection")
    def test_connection_sets_arrow_compression(self, mock_get_con):
        """Test that bq_arrow_compression is set for efficient data transfer."""
        from geoparquet_io.core.extract_bigquery import get_bigquery_connection

        mock_con = MagicMock()
        mock_get_con.return_value = mock_con

        get_bigquery_connection()

        calls = [call[0][0] for call in mock_con.execute.call_args_list]
        compression_calls = [c for c in calls if "bq_arrow_compression" in c.lower()]
        assert compression_calls, "bq_arrow_compression should be set for network efficiency"

    @patch("geoparquet_io.core.extract_bigquery.get_duckdb_connection")
    def test_connection_handles_install_race(self, mock_get_con):
        """Test that bigquery INSTALL failure (race condition) is handled gracefully."""
        from geoparquet_io.core.extract_bigquery import get_bigquery_connection

        mock_con = MagicMock()
        mock_get_con.return_value = mock_con

        # Simulate INSTALL failing (already installed by another worker)
        def execute_side_effect(sql):
            if "INSTALL" in sql and "bigquery" in sql.lower():
                raise duckdb.IOException("Extension already installed")
            return mock_con

        mock_con.execute.side_effect = execute_side_effect

        # Should not raise — INSTALL failure is caught internally
        get_bigquery_connection()

    @patch("geoparquet_io.core.extract_bigquery._setup_bigquery_connection")
    def test_credentials_file_validation(self, mock_setup):
        """Test that non-existent credentials file raises error."""
        from geoparquet_io.core.extract_bigquery import get_bigquery_connection

        mock_setup.return_value = MagicMock()

        with pytest.raises(FileNotFoundError, match="Credentials file not found"):
            get_bigquery_connection(credentials_file="/nonexistent/path/credentials.json")

    @patch("geoparquet_io.core.extract_bigquery._setup_bigquery_connection")
    def test_context_manager_no_deprecated_geography_setting(self, mock_setup):
        """Test BigQueryConnection context manager doesn't set deprecated setting."""
        from geoparquet_io.core.extract_bigquery import BigQueryConnection

        mock_con = MagicMock()
        mock_setup.return_value = mock_con

        with BigQueryConnection() as _con:
            pass

        # _setup_bigquery_connection handles all DuckDB config;
        # BigQueryConnection should not add deprecated settings on top
        calls = [call[0][0] for call in mock_con.execute.call_args_list]
        geom_setting_calls = [c for c in calls if "geography_as_geometry" in c.lower()]
        assert not geom_setting_calls, (
            "BigQueryConnection should not set deprecated bq_geography_as_geometry in v1.5"
        )


# Integration tests that require BigQuery access
@pytest.mark.network
class TestBigQueryIntegration:
    """Integration tests requiring BigQuery access.

    Set BQ_TEST_TABLE environment variable to run these tests:
    export BQ_TEST_TABLE=project.dataset.table
    """

    @pytest.fixture
    def bq_table_id(self):
        """Get BigQuery test table from environment."""
        table_id = os.environ.get("BQ_TEST_TABLE")
        if not table_id:
            pytest.skip("BQ_TEST_TABLE environment variable not set")
        return table_id

    @pytest.fixture
    def output_file(self):
        """Create a temporary output file path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_bq_{uuid.uuid4()}.parquet"
        yield str(tmp_path)
        safe_unlink(tmp_path)

    def test_extract_from_bigquery(self, bq_table_id, output_file):
        """Test extracting data from BigQuery to GeoParquet."""
        from geoparquet_io.core.extract_bigquery import extract_bigquery

        extract_bigquery(
            table_id=bq_table_id,
            output_parquet=output_file,
            limit=10,
        )

        # Verify output file exists and is valid GeoParquet
        assert Path(output_file).exists()
        table = pq.read_table(output_file)
        assert table.num_rows == 10

    def test_extract_with_spherical_edges(self, bq_table_id, output_file):
        """Test that BigQuery output has spherical edges in metadata."""
        import json

        from geoparquet_io.core.extract_bigquery import extract_bigquery

        extract_bigquery(
            table_id=bq_table_id,
            output_parquet=output_file,
            limit=5,
        )

        # Check geo metadata for spherical edges
        pf = pq.ParquetFile(output_file)
        metadata = pf.schema_arrow.metadata
        assert b"geo" in metadata, "Expected geo metadata in BigQuery-extracted Parquet"

        geo_meta = json.loads(metadata[b"geo"].decode("utf-8"))
        columns = geo_meta.get("columns", {})
        assert columns, "Expected at least one geometry column in geo metadata"

        # Verify all geometry columns have spherical edges
        for col_name, col_meta in columns.items():
            if "edges" in col_meta:
                assert col_meta["edges"] == "spherical", (
                    f"Expected spherical edges for column {col_name}, got {col_meta['edges']}"
                )

    def test_python_api_from_bigquery(self, bq_table_id):
        """Test Table.from_bigquery() method."""
        from geoparquet_io.api import Table

        table = Table.from_bigquery(bq_table_id, limit=5)
        assert isinstance(table, Table)
        assert table.num_rows == 5
