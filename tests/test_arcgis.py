"""
Tests for ArcGIS Feature Service conversion.

Tests use mocked HTTP responses to avoid network dependencies.
Network tests are marked separately for optional integration testing.
"""

import tempfile
import uuid
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from tests.conftest import safe_unlink

# --- Mock Data Fixtures ---

MOCK_LAYER_INFO = {
    "name": "Test Layer",
    "geometryType": "esriGeometryPoint",
    "spatialReference": {"wkid": 4326, "latestWkid": 4326},
    "fields": [
        {"name": "OBJECTID", "type": "esriFieldTypeOID"},
        {"name": "name", "type": "esriFieldTypeString"},
    ],
    "maxRecordCount": 1000,
}

MOCK_FEATURE_COUNT = {"count": 3}

MOCK_FEATURES_PAGE = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-122.4, 37.8]},
            "properties": {"OBJECTID": 1, "name": "Point 1"},
        },
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-122.5, 37.9]},
            "properties": {"OBJECTID": 2, "name": "Point 2"},
        },
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-122.6, 38.0]},
            "properties": {"OBJECTID": 3, "name": "Point 3"},
        },
    ],
}


class TestResolveToken:
    """Tests for token resolution."""

    def test_direct_token(self):
        """Test direct token is used as-is."""
        from geoparquet_io.core.arcgis import ArcGISAuth, resolve_token

        auth = ArcGISAuth(token="direct_token")
        result = resolve_token(auth, "https://example.com")
        assert result == "direct_token"

    def test_token_file(self, tmp_path):
        """Test token is read from file."""
        from geoparquet_io.core.arcgis import ArcGISAuth, resolve_token

        token_file = tmp_path / "token.txt"
        token_file.write_text("file_token\n")

        auth = ArcGISAuth(token_file=str(token_file))
        result = resolve_token(auth, "https://example.com")
        assert result == "file_token"

    @patch("geoparquet_io.core.arcgis.generate_token")
    def test_username_password(self, mock_generate):
        """Test token generation from username/password."""
        from geoparquet_io.core.arcgis import ArcGISAuth, resolve_token

        mock_generate.return_value = "generated_token"

        auth = ArcGISAuth(username="user", password="pass")
        result = resolve_token(auth, "https://example.com")

        assert result == "generated_token"
        mock_generate.assert_called_once()

    def test_priority_token_over_file(self, tmp_path):
        """Test direct token takes priority over token file."""
        from geoparquet_io.core.arcgis import ArcGISAuth, resolve_token

        token_file = tmp_path / "token.txt"
        token_file.write_text("file_token")

        auth = ArcGISAuth(token="direct_token", token_file=str(token_file))
        result = resolve_token(auth, "https://example.com")
        assert result == "direct_token"

    def test_no_auth(self):
        """Test None returned when no auth provided."""
        from geoparquet_io.core.arcgis import ArcGISAuth, resolve_token

        auth = ArcGISAuth()
        result = resolve_token(auth, "https://example.com")
        assert result is None


class TestValidateArcgisUrl:
    """Tests for URL validation."""

    def test_valid_feature_server_url(self):
        """Test valid FeatureServer URL."""
        from geoparquet_io.core.arcgis import validate_arcgis_url

        url, layer_id = validate_arcgis_url(
            "https://services.arcgis.com/org/arcgis/rest/services/Test/FeatureServer/0"
        )
        assert "/FeatureServer/0" in url
        assert layer_id == 0

    def test_valid_map_server_url(self):
        """Test valid MapServer URL."""
        from geoparquet_io.core.arcgis import validate_arcgis_url

        url, layer_id = validate_arcgis_url(
            "https://example.com/arcgis/rest/services/Test/MapServer/5"
        )
        assert "/MapServer/5" in url
        assert layer_id == 5

    def test_url_with_trailing_slash(self):
        """Test URL with trailing slash is handled."""
        from geoparquet_io.core.arcgis import validate_arcgis_url

        url, layer_id = validate_arcgis_url(
            "https://services.arcgis.com/org/rest/services/Test/FeatureServer/0/"
        )
        assert layer_id == 0

    def test_invalid_url_no_server_type(self):
        """Test invalid URL without FeatureServer/MapServer."""
        import click

        from geoparquet_io.core.arcgis import validate_arcgis_url

        with pytest.raises(click.ClickException, match="Invalid ArcGIS URL"):
            validate_arcgis_url("https://example.com/rest/services/Test/0")

    def test_invalid_url_no_layer_id(self):
        """Test invalid URL without layer ID."""
        import click

        from geoparquet_io.core.arcgis import validate_arcgis_url

        with pytest.raises(click.ClickException, match="Missing layer ID in URL"):
            validate_arcgis_url("https://example.com/rest/services/Test/FeatureServer")


class TestGenerateToken:
    """Tests for token generation."""

    @patch("geoparquet_io.core.arcgis._make_request")
    def test_successful_generation(self, mock_request):
        """Test successful token generation."""
        from geoparquet_io.core.arcgis import generate_token

        mock_request.return_value = {"token": "new_token", "expires": 3600}

        result = generate_token("user", "pass")

        assert result == "new_token"
        mock_request.assert_called_once()

    @patch("geoparquet_io.core.arcgis._make_request")
    def test_invalid_credentials(self, mock_request):
        """Test error on invalid credentials."""
        import click

        from geoparquet_io.core.arcgis import generate_token

        mock_request.return_value = {
            "error": {"code": 400, "message": "Invalid credentials", "details": []}
        }

        with pytest.raises(click.ClickException, match="Invalid credentials"):
            generate_token("user", "wrong_pass")


class TestGetLayerInfo:
    """Tests for layer info retrieval."""

    @patch("geoparquet_io.core.arcgis._make_request")
    @patch("geoparquet_io.core.arcgis.get_feature_count")
    def test_successful_info(self, mock_count, mock_request):
        """Test successful layer info retrieval."""
        from geoparquet_io.core.arcgis import get_layer_info

        mock_request.return_value = MOCK_LAYER_INFO
        mock_count.return_value = 100

        result = get_layer_info("https://example.com/FeatureServer/0")

        assert result.name == "Test Layer"
        assert result.geometry_type == "esriGeometryPoint"
        assert result.max_record_count == 1000
        assert result.total_count == 100


class TestFetchFeaturesPage:
    """Tests for feature fetching."""

    @patch("geoparquet_io.core.arcgis._make_request")
    def test_fetch_page(self, mock_request):
        """Test fetching a single page of features."""
        from geoparquet_io.core.arcgis import fetch_features_page

        mock_request.return_value = MOCK_FEATURES_PAGE

        result = fetch_features_page(
            "https://example.com/FeatureServer/0",
            offset=0,
            limit=1000,
        )

        assert result["type"] == "FeatureCollection"
        assert len(result["features"]) == 3


class TestCrsExtraction:
    """Tests for CRS handling."""

    def test_wkid_to_epsg(self):
        """Test WKID conversion to EPSG."""
        from geoparquet_io.core.arcgis import _extract_crs_from_spatial_reference

        # Standard EPSG
        result = _extract_crs_from_spatial_reference({"wkid": 4326})
        assert result is not None

        # Web Mercator special case
        result = _extract_crs_from_spatial_reference({"wkid": 102100})
        assert result is not None

    def test_default_crs(self):
        """Test default CRS when no spatial reference."""
        from geoparquet_io.core.arcgis import _extract_crs_from_spatial_reference

        result = _extract_crs_from_spatial_reference({})
        assert result is not None  # Should default to WGS84


class TestSchemaBuilding:
    """Tests for schema building from layer metadata (issue #290)."""

    def test_basic_types(self):
        """Test mapping of common ArcGIS types to PyArrow types."""
        from geoparquet_io.core.arcgis import ArcGISLayerInfo, _build_schema_from_layer_info

        layer_info = ArcGISLayerInfo(
            name="test",
            geometry_type="esriGeometryPoint",
            spatial_reference={"wkid": 4326},
            fields=[
                {"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False},
                {"name": "int_field", "type": "esriFieldTypeInteger", "nullable": True},
                {"name": "double_field", "type": "esriFieldTypeDouble", "nullable": True},
                {"name": "str_field", "type": "esriFieldTypeString", "nullable": True},
                {"name": "date_field", "type": "esriFieldTypeDate", "nullable": True},
            ],
            max_record_count=2000,
            total_count=100,
        )

        schema = _build_schema_from_layer_info(layer_info)

        # Check geometry column
        assert schema.names[0] == "geometry"
        assert schema.field("geometry").type == pa.binary()
        assert schema.field("geometry").nullable  # Can be null for features without spatial data

        # Check attribute columns
        assert schema.field("OBJECTID").type == pa.int64()
        assert not schema.field("OBJECTID").nullable

        assert schema.field("int_field").type == pa.int32()
        assert schema.field("int_field").nullable

        assert schema.field("double_field").type == pa.float64()
        assert schema.field("double_field").nullable

        assert schema.field("str_field").type == pa.string()
        assert schema.field("str_field").nullable

        assert schema.field("date_field").type == pa.timestamp("ms")
        assert schema.field("date_field").nullable

    def test_all_numeric_types(self):
        """Test all numeric type mappings."""
        from geoparquet_io.core.arcgis import ArcGISLayerInfo, _build_schema_from_layer_info

        layer_info = ArcGISLayerInfo(
            name="test",
            geometry_type="esriGeometryPoint",
            spatial_reference={"wkid": 4326},
            fields=[
                {"name": "small_int", "type": "esriFieldTypeSmallInteger", "nullable": True},
                {"name": "int", "type": "esriFieldTypeInteger", "nullable": True},
                {"name": "single", "type": "esriFieldTypeSingle", "nullable": True},
                {"name": "double", "type": "esriFieldTypeDouble", "nullable": True},
            ],
            max_record_count=2000,
            total_count=100,
        )

        schema = _build_schema_from_layer_info(layer_info)

        assert schema.field("small_int").type == pa.int16()
        assert schema.field("int").type == pa.int32()
        assert schema.field("single").type == pa.float32()
        assert schema.field("double").type == pa.float64()

    def test_special_types(self):
        """Test GUID, GlobalID, Blob, XML types."""
        from geoparquet_io.core.arcgis import ArcGISLayerInfo, _build_schema_from_layer_info

        layer_info = ArcGISLayerInfo(
            name="test",
            geometry_type="esriGeometryPoint",
            spatial_reference={"wkid": 4326},
            fields=[
                {"name": "guid_field", "type": "esriFieldTypeGUID", "nullable": True},
                {"name": "globalid", "type": "esriFieldTypeGlobalID", "nullable": False},
                {"name": "blob_field", "type": "esriFieldTypeBlob", "nullable": True},
                {"name": "xml_field", "type": "esriFieldTypeXML", "nullable": True},
            ],
            max_record_count=2000,
            total_count=100,
        )

        schema = _build_schema_from_layer_info(layer_info)

        # GUIDs map to string
        assert schema.field("guid_field").type == pa.string()
        assert schema.field("guid_field").nullable

        # GlobalID is non-nullable string
        assert schema.field("globalid").type == pa.string()
        assert not schema.field("globalid").nullable

        # Blob maps to binary
        assert schema.field("blob_field").type == pa.binary()
        assert schema.field("blob_field").nullable

        # XML maps to string
        assert schema.field("xml_field").type == pa.string()
        assert schema.field("xml_field").nullable

    def test_unknown_type_fallback(self):
        """Test that unknown types fall back to string with warning."""
        from geoparquet_io.core.arcgis import ArcGISLayerInfo, _build_schema_from_layer_info

        layer_info = ArcGISLayerInfo(
            name="test",
            geometry_type="esriGeometryPoint",
            spatial_reference={"wkid": 4326},
            fields=[
                {"name": "unknown_field", "type": "esriFieldTypeUnknown", "nullable": True},
            ],
            max_record_count=2000,
            total_count=100,
        )

        # Should not raise, should warn and fallback to string
        schema = _build_schema_from_layer_info(layer_info)
        assert schema.field("unknown_field").type == pa.string()
        assert schema.field("unknown_field").nullable

    def test_schema_field_count(self):
        """Test that schema has correct number of fields (geometry + attributes)."""
        from geoparquet_io.core.arcgis import ArcGISLayerInfo, _build_schema_from_layer_info

        layer_info = ArcGISLayerInfo(
            name="test",
            geometry_type="esriGeometryPoint",
            spatial_reference={"wkid": 4326},
            fields=[
                {"name": "field1", "type": "esriFieldTypeString", "nullable": True},
                {"name": "field2", "type": "esriFieldTypeInteger", "nullable": True},
                {"name": "field3", "type": "esriFieldTypeDouble", "nullable": True},
            ],
            max_record_count=2000,
            total_count=100,
        )

        schema = _build_schema_from_layer_info(layer_info)

        # Should have geometry + 3 fields = 4 total
        assert len(schema) == 4
        assert schema.names == ["geometry", "field1", "field2", "field3"]


class TestCLI:
    """CLI integration tests."""

    @pytest.fixture
    def output_file(self):
        """Create temp output file path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_arcgis_{uuid.uuid4()}.parquet"
        yield str(tmp_path)
        safe_unlink(tmp_path)

    @patch("geoparquet_io.core.arcgis.convert_arcgis_to_geoparquet")
    def test_basic_command(self, mock_convert, output_file):
        """Test basic CLI command."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "extract",
                "arcgis",
                "https://example.com/FeatureServer/0",
                output_file,
            ],
        )

        assert result.exit_code == 0
        mock_convert.assert_called_once()

    def test_missing_output(self):
        """Test error when output file missing."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "extract",
                "arcgis",
                "https://example.com/FeatureServer/0",
            ],
        )

        assert result.exit_code != 0

    def test_username_without_password(self, output_file):
        """Test error when username provided without password."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "extract",
                "arcgis",
                "https://example.com/FeatureServer/0",
                output_file,
                "--username",
                "user",
            ],
        )

        assert result.exit_code != 0
        assert "password" in result.output.lower() or "password" in str(result.exception).lower()


class TestPythonAPI:
    """Tests for Python API functions."""

    @patch("geoparquet_io.core.arcgis.arcgis_to_table")
    def test_extract_arcgis_function(self, mock_arcgis_to_table):
        """Test extract_arcgis API function."""
        from geoparquet_io.api.table import extract_arcgis

        # Create mock table
        mock_table = pa.table({"geometry": [b"test"], "name": ["Point 1"]})
        mock_arcgis_to_table.return_value = mock_table

        result = extract_arcgis("https://example.com/FeatureServer/0")

        assert result.num_rows == 1
        mock_arcgis_to_table.assert_called_once()

    @patch("geoparquet_io.core.arcgis.arcgis_to_table")
    def test_ops_from_arcgis_function(self, mock_arcgis_to_table):
        """Test ops.from_arcgis function."""
        from geoparquet_io.api import ops

        # Create mock table
        mock_table = pa.table({"geometry": [b"test"], "name": ["Point 1"]})
        mock_arcgis_to_table.return_value = mock_table

        result = ops.from_arcgis("https://example.com/FeatureServer/0")

        assert isinstance(result, pa.Table)
        assert result.num_rows == 1


class TestStreamingConversion:
    """Tests for memory-efficient streaming conversion."""

    @pytest.fixture
    def output_file(self):
        """Create temp output file path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_arcgis_stream_{uuid.uuid4()}.parquet"
        yield str(tmp_path)
        safe_unlink(tmp_path)

    def test_geojson_page_to_table(self):
        """Test converting a single page of GeoJSON features to Arrow table."""
        from geoparquet_io.core.arcgis import _geojson_page_to_table

        features = MOCK_FEATURES_PAGE["features"]
        table = _geojson_page_to_table(features)

        assert table is not None
        assert table.num_rows == 3
        assert "geometry" in table.column_names

    def test_geojson_page_to_table_empty(self):
        """Test empty features returns None."""
        from geoparquet_io.core.arcgis import _geojson_page_to_table

        result = _geojson_page_to_table([])
        assert result is None

    @patch("geoparquet_io.core.arcgis.fetch_all_features")
    def test_stream_features_to_parquet_single_page(self, mock_fetch, output_file):
        """Test streaming a single page to parquet."""
        from geoparquet_io.core.arcgis import ArcGISLayerInfo, _stream_features_to_parquet

        # Mock single page
        mock_fetch.return_value = iter([MOCK_FEATURES_PAGE])

        layer_info = ArcGISLayerInfo(
            name="Test",
            geometry_type="esriGeometryPoint",
            spatial_reference={"wkid": 4326},
            fields=[
                {"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False},
                {"name": "name", "type": "esriFieldTypeString", "nullable": True},
            ],
            max_record_count=1000,
            total_count=3,
        )

        total = _stream_features_to_parquet(
            service_url="https://example.com/FeatureServer/0",
            layer_info=layer_info,
            output_path=output_file,
        )

        assert total == 3
        assert Path(output_file).exists()

        # Verify parquet content
        table = pq.read_table(output_file)
        assert table.num_rows == 3
        assert "geometry" in table.column_names

    @patch("geoparquet_io.core.arcgis.fetch_all_features")
    def test_stream_features_to_parquet_multi_page(self, mock_fetch, output_file):
        """Test streaming multiple pages to parquet."""
        from geoparquet_io.core.arcgis import ArcGISLayerInfo, _stream_features_to_parquet

        # Create two pages of features
        page1 = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [-122.4, 37.8]},
                    "properties": {"OBJECTID": 1, "name": "Point 1"},
                },
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [-122.5, 37.9]},
                    "properties": {"OBJECTID": 2, "name": "Point 2"},
                },
            ],
        }
        page2 = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [-122.6, 38.0]},
                    "properties": {"OBJECTID": 3, "name": "Point 3"},
                },
            ],
        }

        mock_fetch.return_value = iter([page1, page2])

        layer_info = ArcGISLayerInfo(
            name="Test",
            geometry_type="esriGeometryPoint",
            spatial_reference={"wkid": 4326},
            fields=[
                {"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False},
                {"name": "name", "type": "esriFieldTypeString", "nullable": True},
            ],
            max_record_count=2,
            total_count=3,
        )

        total = _stream_features_to_parquet(
            service_url="https://example.com/FeatureServer/0",
            layer_info=layer_info,
            output_path=output_file,
        )

        assert total == 3

        # Verify all rows are present
        table = pq.read_table(output_file)
        assert table.num_rows == 3

    @patch("geoparquet_io.core.arcgis.fetch_all_features")
    def test_stream_features_handles_empty_pages(self, mock_fetch, output_file):
        """Test streaming handles pages with no features."""
        from geoparquet_io.core.arcgis import ArcGISLayerInfo, _stream_features_to_parquet

        # First page has features, second is empty
        page1 = MOCK_FEATURES_PAGE
        page2 = {"type": "FeatureCollection", "features": []}

        mock_fetch.return_value = iter([page1, page2])

        layer_info = ArcGISLayerInfo(
            name="Test",
            geometry_type="esriGeometryPoint",
            spatial_reference={"wkid": 4326},
            fields=[
                {"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False},
                {"name": "name", "type": "esriFieldTypeString", "nullable": True},
            ],
            max_record_count=1000,
            total_count=3,
        )

        total = _stream_features_to_parquet(
            service_url="https://example.com/FeatureServer/0",
            layer_info=layer_info,
            output_path=output_file,
        )

        assert total == 3  # Only features from page1

    @patch("geoparquet_io.core.arcgis._stream_features_to_parquet")
    @patch("geoparquet_io.core.arcgis.get_layer_info")
    @patch("geoparquet_io.core.arcgis.validate_arcgis_url")
    def test_arcgis_to_table_cleans_temp_file(self, mock_validate, mock_layer_info, mock_stream):
        """Test that temp file is cleaned up after conversion."""
        import glob
        import os

        from geoparquet_io.core.arcgis import ArcGISLayerInfo, arcgis_to_table

        mock_validate.return_value = ("https://example.com/FeatureServer/0", 0)
        mock_layer_info.return_value = ArcGISLayerInfo(
            name="Test",
            geometry_type="esriGeometryPoint",
            spatial_reference={"wkid": 4326},
            fields=[],
            max_record_count=1000,
            total_count=3,
        )

        # Create a real temp file that stream would create
        temp_dir = tempfile.gettempdir()

        def mock_stream_impl(service_url, layer_info, output_path, **kwargs):
            # Write a minimal parquet file
            table = pa.table({"geometry": [b"test1", b"test2", b"test3"], "name": ["a", "b", "c"]})
            pq.write_table(table, output_path)
            return 3

        mock_stream.side_effect = mock_stream_impl

        # Count temp files before
        temp_files_before = set(glob.glob(os.path.join(temp_dir, "arcgis_stream_*.parquet")))

        # Run conversion
        result = arcgis_to_table("https://example.com/FeatureServer/0")

        # Count temp files after
        temp_files_after = set(glob.glob(os.path.join(temp_dir, "arcgis_stream_*.parquet")))

        # Should have same number (temp file cleaned up)
        assert temp_files_before == temp_files_after
        assert result.num_rows == 3


@pytest.mark.network
class TestNetworkIntegration:
    """Network integration tests (require actual ArcGIS service)."""

    # Small public service for testing
    SMALL_SERVICE = "https://services7.arcgis.com/n1YM8pTrFmm7L4hs/ArcGIS/rest/services/Current_Ice_Jams/FeatureServer/0"

    def test_fetch_layer_info(self):
        """Test fetching real layer info."""
        from geoparquet_io.core.arcgis import get_layer_info

        info = get_layer_info(self.SMALL_SERVICE)
        assert info.name is not None
        assert info.total_count >= 0

    def test_fetch_feature_count(self):
        """Test fetching real feature count."""
        from geoparquet_io.core.arcgis import get_feature_count

        count = get_feature_count(self.SMALL_SERVICE)
        assert isinstance(count, int)
        assert count >= 0

    @pytest.fixture
    def output_file(self):
        """Create temp output file path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_arcgis_network_{uuid.uuid4()}.parquet"
        yield str(tmp_path)
        safe_unlink(tmp_path)

    def test_full_conversion(self, output_file):
        """Test full conversion of small public service."""
        from geoparquet_io.core.arcgis import convert_arcgis_to_geoparquet

        convert_arcgis_to_geoparquet(
            self.SMALL_SERVICE,
            output_file,
            verbose=True,
            skip_hilbert=True,  # Skip for speed
        )

        assert Path(output_file).exists()
        pf = pq.ParquetFile(output_file)
        assert pf.metadata.num_rows >= 0

    def test_cli_full_conversion(self, output_file):
        """Test CLI full extraction from public service."""
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "extract",
                "arcgis",
                self.SMALL_SERVICE,
                output_file,
                "--skip-hilbert",
                "-v",
            ],
        )

        # May succeed or fail depending on network
        # We just want to ensure the command runs without crashing
        if result.exit_code == 0:
            assert Path(output_file).exists()

    def test_python_api_conversion(self, output_file):
        """Test Python API extraction."""
        import geoparquet_io as gpio

        table = gpio.extract_arcgis(self.SMALL_SERVICE)
        assert table.num_rows >= 0
        assert "geometry" in table.column_names


class TestAlignTableToSchema:
    """Unit tests for the _align_table_to_schema helper function."""

    def test_reorders_columns_to_match_schema(self):
        """Test that columns are reordered to match target schema."""
        from geoparquet_io.core.arcgis import _align_table_to_schema

        # Source table with different column order
        source = pa.table(
            {
                "c": [3, 6],
                "a": [1, 4],
                "b": [2, 5],
            }
        )

        target_schema = pa.schema(
            [
                pa.field("a", pa.int64()),
                pa.field("b", pa.int64()),
                pa.field("c", pa.int64()),
            ]
        )

        result = _align_table_to_schema(source, target_schema)

        assert result.column_names == ["a", "b", "c"]
        assert result.column("a").to_pylist() == [1, 4]
        assert result.column("b").to_pylist() == [2, 5]
        assert result.column("c").to_pylist() == [3, 6]

    def test_drops_extra_columns(self):
        """Test that extra columns not in target schema are dropped."""
        from geoparquet_io.core.arcgis import _align_table_to_schema

        source = pa.table(
            {
                "a": [1, 2],
                "b": [3, 4],
                "extra": ["x", "y"],  # Not in target schema
            }
        )

        target_schema = pa.schema(
            [
                pa.field("a", pa.int64()),
                pa.field("b", pa.int64()),
            ]
        )

        result = _align_table_to_schema(source, target_schema)

        assert result.column_names == ["a", "b"]
        assert "extra" not in result.column_names

    def test_adds_missing_columns_with_nulls(self):
        """Test that missing columns are filled with nulls."""
        from geoparquet_io.core.arcgis import _align_table_to_schema

        source = pa.table(
            {
                "a": [1, 2],
            }
        )

        target_schema = pa.schema(
            [
                pa.field("a", pa.int64()),
                pa.field("missing", pa.string()),
            ]
        )

        result = _align_table_to_schema(source, target_schema)

        assert result.column_names == ["a", "missing"]
        assert result.column("missing").to_pylist() == [None, None]

    def test_handles_all_mismatches_together(self):
        """Test reorder + drop + add in combination."""
        from geoparquet_io.core.arcgis import _align_table_to_schema

        source = pa.table(
            {
                "z": [9, 10],  # wrong position
                "a": [1, 2],  # should be first
                "extra": ["x", "y"],  # should be dropped
            }
        )

        target_schema = pa.schema(
            [
                pa.field("a", pa.int64()),
                pa.field("missing", pa.float64()),  # should be added as null
                pa.field("z", pa.int64()),
            ]
        )

        result = _align_table_to_schema(source, target_schema)

        assert result.column_names == ["a", "missing", "z"]
        assert result.column("a").to_pylist() == [1, 2]
        assert result.column("missing").to_pylist() == [None, None]
        assert result.column("z").to_pylist() == [9, 10]
        assert "extra" not in result.column_names


class TestSchemaMismatchBetweenBatches:
    """Tests for issue #334: Schema mismatch with mixed-type chunks.

    When extracting from ArcGIS, different pages may have different inferred
    types for the same field (e.g., int64 vs double) due to the actual data
    values in each batch.
    """

    @pytest.fixture
    def output_file(self, tmp_path):
        """Create a temporary output file path."""
        output = tmp_path / f"test_output_{uuid.uuid4()}.parquet"
        yield str(output)
        safe_unlink(output)

    @patch("geoparquet_io.core.arcgis.fetch_all_features")
    def test_handles_int_to_double_type_variance(self, mock_fetch, output_file):
        """Test that double fields with integer values in some batches are handled.

        Reproduces issue #334: When a double field (like TownGlValu) has only
        whole numbers in some batches, DuckDB infers int64, causing schema
        mismatch when cast to the correct float64 from ArcGIS metadata.
        """
        from geoparquet_io.core.arcgis import ArcGISLayerInfo, _stream_features_to_parquet

        # Page 1: value field has decimal values (inferred as double)
        page1 = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [-72.5, 44.2]},
                    "properties": {"OBJECTID": 1, "TownGlValu": 15000.50},
                },
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [-72.6, 44.3]},
                    "properties": {"OBJECTID": 2, "TownGlValu": 25000.75},
                },
            ],
        }

        # Page 2: value field has only whole numbers (may be inferred as int64)
        page2 = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [-72.7, 44.4]},
                    "properties": {"OBJECTID": 3, "TownGlValu": 30000},
                },
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [-72.8, 44.5]},
                    "properties": {"OBJECTID": 4, "TownGlValu": 40000},
                },
            ],
        }

        mock_fetch.return_value = iter([page1, page2])

        # Layer metadata correctly defines TownGlValu as double
        layer_info = ArcGISLayerInfo(
            name="VT Property Transfers",
            geometry_type="esriGeometryPoint",
            spatial_reference={"wkid": 4326},
            fields=[
                {"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False},
                {"name": "TownGlValu", "type": "esriFieldTypeDouble", "nullable": True},
            ],
            max_record_count=2000,
            total_count=4,
        )

        # This should NOT raise a schema mismatch error
        total = _stream_features_to_parquet(
            service_url="https://example.com/FeatureServer/0",
            layer_info=layer_info,
            output_path=output_file,
        )

        assert total == 4

        # Verify all values are present and correctly typed as float64
        table = pq.read_table(output_file)
        assert table.num_rows == 4
        assert table.schema.field("TownGlValu").type == pa.float64()

    @patch("geoparquet_io.core.arcgis.fetch_all_features")
    def test_handles_null_column_becoming_string(self, mock_fetch, output_file):
        """Test that columns with nulls in early batches are handled.

        When a column has all nulls in batch 1, DuckDB may infer it as null type.
        Later batches with actual values should still work.
        """
        from geoparquet_io.core.arcgis import ArcGISLayerInfo, _stream_features_to_parquet

        # Page 1: description is null
        page1 = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [-72.5, 44.2]},
                    "properties": {"OBJECTID": 1, "description": None},
                },
            ],
        }

        # Page 2: description has actual values
        page2 = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [-72.6, 44.3]},
                    "properties": {"OBJECTID": 2, "description": "Some text"},
                },
            ],
        }

        mock_fetch.return_value = iter([page1, page2])

        layer_info = ArcGISLayerInfo(
            name="Test Layer",
            geometry_type="esriGeometryPoint",
            spatial_reference={"wkid": 4326},
            fields=[
                {"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False},
                {"name": "description", "type": "esriFieldTypeString", "nullable": True},
            ],
            max_record_count=2000,
            total_count=2,
        )

        total = _stream_features_to_parquet(
            service_url="https://example.com/FeatureServer/0",
            layer_info=layer_info,
            output_path=output_file,
        )

        assert total == 2

        table = pq.read_table(output_file)
        assert table.num_rows == 2
        assert table.schema.field("description").type == pa.string()

    @patch("geoparquet_io.core.arcgis.fetch_all_features")
    def test_handles_multiple_type_variance_columns(self, mock_fetch, output_file):
        """Test multiple columns with type variance across batches."""
        from geoparquet_io.core.arcgis import ArcGISLayerInfo, _stream_features_to_parquet

        # Page 1: mixed types
        page1 = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [-72.5, 44.2]},
                    "properties": {
                        "OBJECTID": 1,
                        "price": 100.50,  # float
                        "count": 10,  # int
                        "rating": None,  # null
                    },
                },
            ],
        }

        # Page 2: different inference
        page2 = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [-72.6, 44.3]},
                    "properties": {
                        "OBJECTID": 2,
                        "price": 200,  # whole number (might infer as int)
                        "count": 20,
                        "rating": 4.5,  # now has value
                    },
                },
            ],
        }

        mock_fetch.return_value = iter([page1, page2])

        layer_info = ArcGISLayerInfo(
            name="Test Layer",
            geometry_type="esriGeometryPoint",
            spatial_reference={"wkid": 4326},
            fields=[
                {"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False},
                {"name": "price", "type": "esriFieldTypeDouble", "nullable": True},
                {"name": "count", "type": "esriFieldTypeInteger", "nullable": True},
                {"name": "rating", "type": "esriFieldTypeSingle", "nullable": True},
            ],
            max_record_count=2000,
            total_count=2,
        )

        total = _stream_features_to_parquet(
            service_url="https://example.com/FeatureServer/0",
            layer_info=layer_info,
            output_path=output_file,
        )

        assert total == 2

        table = pq.read_table(output_file)
        assert table.num_rows == 2
        assert table.schema.field("price").type == pa.float64()
        assert table.schema.field("count").type == pa.int32()
        assert table.schema.field("rating").type == pa.float32()

    @patch("geoparquet_io.core.arcgis.fetch_all_features")
    def test_handles_column_order_mismatch(self, mock_fetch, output_file):
        """Test that column order differences between batches are handled.

        DuckDB may return columns in different order than the ArcGIS metadata.
        This should not cause a schema mismatch error.
        """
        from geoparquet_io.core.arcgis import ArcGISLayerInfo, _stream_features_to_parquet

        # Properties order differs from layer_info field order
        page1 = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [-72.5, 44.2]},
                    # Note: properties order is name, OBJECTID, value
                    "properties": {"name": "First", "OBJECTID": 1, "value": 100.0},
                },
            ],
        }

        mock_fetch.return_value = iter([page1])

        # Layer metadata defines: OBJECTID, name, value (different order!)
        layer_info = ArcGISLayerInfo(
            name="Test Layer",
            geometry_type="esriGeometryPoint",
            spatial_reference={"wkid": 4326},
            fields=[
                {"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False},
                {"name": "name", "type": "esriFieldTypeString", "nullable": True},
                {"name": "value", "type": "esriFieldTypeDouble", "nullable": True},
            ],
            max_record_count=2000,
            total_count=1,
        )

        # This should NOT raise a schema mismatch error
        total = _stream_features_to_parquet(
            service_url="https://example.com/FeatureServer/0",
            layer_info=layer_info,
            output_path=output_file,
        )

        assert total == 1

        table = pq.read_table(output_file)
        assert table.num_rows == 1
        # Verify schema matches target, not source order
        assert table.schema.names == ["geometry", "OBJECTID", "name", "value"]

    @patch("geoparquet_io.core.arcgis.fetch_all_features")
    def test_handles_extra_columns_from_service(self, mock_fetch, output_file):
        """Test that extra columns from service not in metadata are dropped.

        Some ArcGIS services return more fields than listed in metadata.
        We should only keep fields defined in layer metadata.
        """
        from geoparquet_io.core.arcgis import ArcGISLayerInfo, _stream_features_to_parquet

        page1 = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [-72.5, 44.2]},
                    "properties": {
                        "OBJECTID": 1,
                        "name": "Test",
                        "extra_field": "should_be_dropped",  # Not in metadata
                    },
                },
            ],
        }

        mock_fetch.return_value = iter([page1])

        layer_info = ArcGISLayerInfo(
            name="Test Layer",
            geometry_type="esriGeometryPoint",
            spatial_reference={"wkid": 4326},
            fields=[
                {"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False},
                {"name": "name", "type": "esriFieldTypeString", "nullable": True},
            ],
            max_record_count=2000,
            total_count=1,
        )

        total = _stream_features_to_parquet(
            service_url="https://example.com/FeatureServer/0",
            layer_info=layer_info,
            output_path=output_file,
        )

        assert total == 1

        table = pq.read_table(output_file)
        assert table.num_rows == 1
        # extra_field should NOT be in the output
        assert "extra_field" not in table.schema.names
        assert table.schema.names == ["geometry", "OBJECTID", "name"]

    @patch("geoparquet_io.core.arcgis.fetch_all_features")
    def test_handles_missing_columns_from_service(self, mock_fetch, output_file):
        """Test that missing columns from service are filled with nulls.

        Some ArcGIS services may not return all fields in every response.
        Missing fields should be filled with nulls of the correct type.
        """
        from geoparquet_io.core.arcgis import ArcGISLayerInfo, _stream_features_to_parquet

        page1 = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [-72.5, 44.2]},
                    "properties": {
                        "OBJECTID": 1,
                        # "name" is missing from this response
                    },
                },
            ],
        }

        mock_fetch.return_value = iter([page1])

        layer_info = ArcGISLayerInfo(
            name="Test Layer",
            geometry_type="esriGeometryPoint",
            spatial_reference={"wkid": 4326},
            fields=[
                {"name": "OBJECTID", "type": "esriFieldTypeOID", "nullable": False},
                {"name": "name", "type": "esriFieldTypeString", "nullable": True},
            ],
            max_record_count=2000,
            total_count=1,
        )

        total = _stream_features_to_parquet(
            service_url="https://example.com/FeatureServer/0",
            layer_info=layer_info,
            output_path=output_file,
        )

        assert total == 1

        table = pq.read_table(output_file)
        assert table.num_rows == 1
        # name should be present (as null) even though it wasn't in the service response
        assert "name" in table.schema.names
        assert table.column("name")[0].as_py() is None


@pytest.mark.network
class TestRealWorldSchemaMismatch:
    """Integration tests for schema mismatch with real-world ArcGIS services.

    These tests require network access and validate the fix for issue #334
    against actual ArcGIS REST endpoints that exhibited the problem.
    """

    # Vermont property transfer dataset from issue #334
    VT_PROPERTY_SERVICE = (
        "https://services1.arcgis.com/BkFxaEFNwHqX3tAw/arcgis/rest/services/"
        "FS_VCGI_OPENDATA_Cadastral_PTTR_point_WM_v1_view/FeatureServer/0"
    )

    @pytest.fixture
    def output_file(self, tmp_path):
        """Create a temporary output file path."""
        output = tmp_path / f"vt_property_{uuid.uuid4()}.parquet"
        yield str(output)
        safe_unlink(output)

    def test_vermont_property_transfers_issue_334(self, output_file):
        """Test extraction from VT property transfer dataset that triggered #334.

        This dataset has the TownGlValu field (esriFieldTypeDouble) which caused
        schema mismatches when some batches only had integer values.
        """
        import geoparquet_io as gpio

        # Extract a subset to keep test fast but still exercise multiple batches
        table = gpio.extract_arcgis(
            self.VT_PROPERTY_SERVICE,
            limit=5000,  # Multiple pages to trigger potential schema issues
        )

        assert table.num_rows > 0
        assert "geometry" in table.column_names

        # Verify the schema is consistent (no schema mismatch error occurred)
        # If we got here without error, the fix is working
        assert table.schema is not None
