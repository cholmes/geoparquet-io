"""Tests for core/metadata_utils.py module."""

import pyarrow as pa

from geoparquet_io.core.metadata_utils import (
    _calculate_overall_bbox,
    _check_extension_type,
    _check_parquet_schema_string,
    detect_geo_logical_type,
    parse_geometry_type_from_schema,
)


class TestCheckParquetSchemaString:
    """Tests for _check_parquet_schema_string function."""

    def test_detects_geography(self):
        """Test detection of Geography type in schema string."""
        # Real Parquet schema format uses 'required group' or similar
        schema_str = (
            'required group geometry (Geography(Point, XY, crs={"type":"OGC","code":"CRS84"}))'
        )
        result = _check_parquet_schema_string("geometry", schema_str)
        assert result == "Geography"

    def test_detects_geometry(self):
        """Test detection of Geometry type in schema string."""
        schema_str = "required group geometry (Geometry(Point, XY))"
        result = _check_parquet_schema_string("geometry", schema_str)
        assert result == "Geometry"

    def test_returns_none_for_no_geo_type(self):
        """Test returns None when no geo type is present."""
        schema_str = "optional binary name (STRING)"
        result = _check_parquet_schema_string("name", schema_str)
        assert result is None

    def test_handles_special_characters_in_field_name(self):
        """Test handling of special characters in field name."""
        schema_str = "required group my.geometry (Geometry(Point, XY))"
        result = _check_parquet_schema_string("my.geometry", schema_str)
        assert result == "Geometry"


class TestCheckExtensionType:
    """Tests for _check_extension_type function."""

    def test_returns_none_for_simple_type(self):
        """Test returns None for non-extension type."""
        field = pa.field("name", pa.string())
        result = _check_extension_type(field)
        assert result is None

    def test_returns_none_for_binary_type(self):
        """Test returns None for plain binary type."""
        field = pa.field("geometry", pa.binary())
        result = _check_extension_type(field)
        assert result is None


class TestDetectGeoLogicalType:
    """Tests for detect_geo_logical_type function."""

    def test_detects_geography_from_schema_string(self):
        """Test detection of Geography from schema string."""
        field = pa.field("geometry", pa.binary())
        schema_str = "required group geometry (Geography(Point, XY))"
        result = detect_geo_logical_type(field, schema_str)
        assert result == "Geography"

    def test_detects_geometry_from_schema_string(self):
        """Test detection of Geometry from schema string."""
        field = pa.field("geometry", pa.binary())
        schema_str = "required group geometry (Geometry(Point, XY))"
        result = detect_geo_logical_type(field, schema_str)
        assert result == "Geometry"

    def test_returns_none_for_non_geo_field(self):
        """Test returns None for non-geometry field."""
        field = pa.field("name", pa.string())
        result = detect_geo_logical_type(field, None)
        assert result is None

    def test_detects_from_type_string(self):
        """Test detection when type string contains Geography/Geometry."""
        # Create a mock field with Geography in type string
        field = pa.field("geometry", pa.binary())
        # Without schema_str, it checks the type string
        result = detect_geo_logical_type(field, None)
        # Binary type doesn't have Geography/Geometry in type string
        assert result is None


class TestParseGeometryTypeFromSchema:
    """Tests for parse_geometry_type_from_schema function."""

    def test_parses_simple_geometry(self):
        """Test parsing simple Geometry type."""
        schema_str = "required group geometry (Geometry(Point, XY))"
        result = parse_geometry_type_from_schema("geometry", schema_str)
        assert result is not None
        assert result.get("geometry_type") == "Point"
        assert result.get("coordinate_dimension") == "XY"

    def test_parses_geography_with_crs(self):
        """Test parsing Geography type with CRS."""
        schema_str = 'required group geometry (Geography(Polygon, XY, crs="OGC:CRS84"))'
        result = parse_geometry_type_from_schema("geometry", schema_str)
        assert result is not None
        assert result.get("geometry_type") == "Polygon"
        assert result.get("crs") == "OGC:CRS84"

    def test_parses_geography_with_algorithm(self):
        """Test parsing Geography type with algorithm."""
        schema_str = "required group geometry (Geography(Point, XY, algorithm=spherical))"
        result = parse_geometry_type_from_schema("geometry", schema_str)
        assert result is not None
        assert result.get("algorithm") == "spherical"

    def test_parses_xyz_coordinate_dimension(self):
        """Test parsing XYZ coordinate dimension."""
        schema_str = "required group geometry (Geometry(LineString, XYZ))"
        result = parse_geometry_type_from_schema("geometry", schema_str)
        assert result is not None
        assert result.get("geometry_type") == "LineString"
        assert result.get("coordinate_dimension") == "XYZ"

    def test_parses_multipolygon(self):
        """Test parsing MultiPolygon geometry type."""
        schema_str = "required group geometry (Geometry(MultiPolygon, XY))"
        result = parse_geometry_type_from_schema("geometry", schema_str)
        assert result is not None
        assert result.get("geometry_type") == "MultiPolygon"

    def test_returns_none_for_non_geo_field(self):
        """Test returns None for non-geometry field."""
        schema_str = "optional binary name (STRING)"
        result = parse_geometry_type_from_schema("name", schema_str)
        assert result is None

    def test_handles_json_crs(self):
        """Test handling of JSON CRS object."""
        crs_json = '{"type": "PROJCRS", "name": "NAD83"}'
        schema_str = f"required group geometry (Geometry(Polygon, XY, crs={crs_json}))"
        result = parse_geometry_type_from_schema("geometry", schema_str)
        assert result is not None
        # CRS parsing may return dict or string depending on format
        assert result.get("crs") is not None

    def test_parses_geometry_collection(self):
        """Test parsing GeometryCollection type."""
        schema_str = "required group geometry (Geometry(GeometryCollection, XY))"
        result = parse_geometry_type_from_schema("geometry", schema_str)
        assert result is not None
        assert result.get("geometry_type") == "GeometryCollection"


class TestCalculateOverallBbox:
    """Tests for _calculate_overall_bbox function."""

    def test_calculates_bbox_from_single_row_group(self):
        """Test bbox calculation from a single row group."""
        stats = [{"xmin": -122.5, "ymin": 37.5, "xmax": -122.0, "ymax": 38.0}]
        result = _calculate_overall_bbox(stats)
        assert result is not None
        assert result["xmin"] == -122.5
        assert result["ymin"] == 37.5
        assert result["xmax"] == -122.0
        assert result["ymax"] == 38.0

    def test_calculates_bbox_from_multiple_row_groups(self):
        """Test bbox calculation combines multiple row groups."""
        stats = [
            {"xmin": -122.5, "ymin": 37.5, "xmax": -122.0, "ymax": 38.0},
            {"xmin": -123.0, "ymin": 37.0, "xmax": -121.5, "ymax": 38.5},
        ]
        result = _calculate_overall_bbox(stats)
        assert result is not None
        assert result["xmin"] == -123.0  # min of all xmin
        assert result["ymin"] == 37.0  # min of all ymin
        assert result["xmax"] == -121.5  # max of all xmax
        assert result["ymax"] == 38.5  # max of all ymax

    def test_returns_none_for_empty_stats(self):
        """Test returns None for empty stats list."""
        result = _calculate_overall_bbox([])
        assert result is None

    def test_skips_incomplete_row_groups(self):
        """Test skips row groups missing bbox values."""
        stats = [
            {"xmin": -122.5, "ymin": 37.5},  # Missing xmax, ymax
            {"xmin": -123.0, "ymin": 37.0, "xmax": -121.5, "ymax": 38.5},  # Complete
        ]
        result = _calculate_overall_bbox(stats)
        assert result is not None
        assert result["xmin"] == -123.0
        assert result["ymax"] == 38.5


class TestHasParquetGeoRowGroupStats:
    """Tests for has_parquet_geo_row_group_stats function."""

    def test_with_file_with_bbox_column(self, places_test_file):
        """Test with file that has bbox column."""
        from geoparquet_io.core.metadata_utils import has_parquet_geo_row_group_stats

        result = has_parquet_geo_row_group_stats(places_test_file)
        # Places file should have bbox column
        assert isinstance(result, dict)
        assert "has_stats" in result
        assert "stats_source" in result

    def test_with_file_without_bbox(self, buildings_test_file):
        """Test with file that lacks bbox column."""
        from geoparquet_io.core.metadata_utils import has_parquet_geo_row_group_stats

        result = has_parquet_geo_row_group_stats(buildings_test_file)
        assert isinstance(result, dict)
        assert "has_stats" in result


class TestExtractBboxFromRowGroupStats:
    """Tests for extract_bbox_from_row_group_stats function."""

    def test_with_file_with_bbox_column(self, places_test_file):
        """Test extraction from file with bbox column."""
        from geoparquet_io.core.metadata_utils import extract_bbox_from_row_group_stats

        result = extract_bbox_from_row_group_stats(places_test_file, "geometry")
        # Result depends on whether places file has proper bbox stats
        if result is not None:
            assert len(result) == 4
            # Verify bbox structure: [xmin, ymin, xmax, ymax]
            assert result[0] <= result[2]  # xmin <= xmax
            assert result[1] <= result[3]  # ymin <= ymax

    def test_with_file_without_bbox(self, buildings_test_file):
        """Test extraction from file without bbox column."""
        from geoparquet_io.core.metadata_utils import extract_bbox_from_row_group_stats

        result = extract_bbox_from_row_group_stats(buildings_test_file, "geometry")
        # Should return None when no bbox column exists
        assert result is None


class TestFormatGeoparquetMetadata:
    """format_geoparquet_metadata: terminal and JSON rendering of the geo key."""

    def test_terminal_output_places(self, places_test_file, capsys):
        from geoparquet_io.core.metadata_utils import format_geoparquet_metadata

        format_geoparquet_metadata(places_test_file, json_output=False)
        out = capsys.readouterr().out
        assert "GeoParquet Metadata" in out
        assert "Version: 1.0.0" in out
        assert "Primary Column: geometry" in out
        assert "Encoding: WKB" in out
        # Absent optional keys are rendered with their spec defaults.
        assert "OGC:CRS84 (default value)" in out
        assert "Covering: Not present" in out

    def test_json_output_places(self, places_test_file, capsys):
        import json

        from geoparquet_io.core.metadata_utils import format_geoparquet_metadata

        format_geoparquet_metadata(places_test_file, json_output=True)
        data = json.loads(capsys.readouterr().out)
        assert data["version"] == "1.0.0"
        assert data["primary_column"] == "geometry"
        assert data["columns"]["geometry"]["encoding"] == "WKB"

    def test_terminal_no_geo_metadata(self, capsys):
        from geoparquet_io.core.metadata_utils import format_geoparquet_metadata

        # crs-projjson.parquet is plain Parquet with no 'geo' key.
        format_geoparquet_metadata("tests/data/crs-projjson.parquet", json_output=False)
        out = capsys.readouterr().out
        assert "No GeoParquet metadata found" in out

    def test_json_no_geo_metadata(self, capsys):
        import json

        from geoparquet_io.core.metadata_utils import format_geoparquet_metadata

        format_geoparquet_metadata("tests/data/crs-projjson.parquet", json_output=True)
        assert json.loads(capsys.readouterr().out) is None


class TestFormatParquetGeoMetadata:
    """format_parquet_geo_metadata: Parquet-spec geospatial metadata section."""

    def test_terminal_no_native_geo_columns(self, places_test_file, capsys):
        from geoparquet_io.core.metadata_utils import format_parquet_geo_metadata

        # places is GeoParquet 1.0 WKB: no native Parquet geo logical types.
        format_parquet_geo_metadata(places_test_file, json_output=False)
        out = capsys.readouterr().out
        assert "Parquet Geo Metadata" in out
        assert "No geospatial columns detected" in out

    def test_terminal_native_geometry_column(self, capsys):
        from geoparquet_io.core.metadata_utils import format_parquet_geo_metadata

        # GeoParquet 2.0 fixture with a native Geometry column and PROJJSON CRS.
        format_parquet_geo_metadata("tests/data/fields_gpq2_5070_brotli.parquet", json_output=False)
        out = capsys.readouterr().out
        assert "Type: Geometry" in out
        assert "Row Group Statistics:" in out

    def test_json_native_geometry_column(self, capsys):
        import json

        from geoparquet_io.core.metadata_utils import format_parquet_geo_metadata

        format_parquet_geo_metadata("tests/data/fields_gpq2_5070_brotli.parquet", json_output=True)
        data = json.loads(capsys.readouterr().out)
        assert data["total_row_groups"] == 1
        cols = data["geospatial_columns"]
        assert "geometry" in cols
        stats = cols["geometry"]["row_group_stats"]
        assert len(stats) == 1
        assert stats[0]["xmin"] < stats[0]["xmax"]


class TestFormatParquetMetadataEnhanced:
    """format_parquet_metadata_enhanced: full Parquet file metadata section."""

    def test_terminal_output(self, places_test_file, capsys):
        from geoparquet_io.core.metadata_utils import format_parquet_metadata_enhanced

        format_parquet_metadata_enhanced(
            places_test_file, json_output=False, primary_geom_col="geometry"
        )
        out = capsys.readouterr().out
        assert "Parquet File Metadata" in out
        assert "Total Rows: 766" in out
        assert "Row Groups: 1" in out
        assert "Schema:" in out

    def test_json_output(self, places_test_file, capsys):
        import json

        from geoparquet_io.core.metadata_utils import format_parquet_metadata_enhanced

        format_parquet_metadata_enhanced(places_test_file, json_output=True)
        data = json.loads(capsys.readouterr().out)
        assert data["num_rows"] == 766
        assert data["num_row_groups"] == 1
        assert data["num_columns"] == 10
        assert len(data["row_groups"]) == 1

    def test_json_output_all_row_groups(self, places_test_file, capsys):
        import json

        from geoparquet_io.core.metadata_utils import format_parquet_metadata_enhanced

        format_parquet_metadata_enhanced(places_test_file, json_output=True, row_groups_limit=None)
        data = json.loads(capsys.readouterr().out)
        assert len(data["row_groups"]) == data["num_row_groups"]


class TestFormatAllMetadata:
    """format_all_metadata: the three sections in one pass."""

    def test_terminal_output(self, places_test_file, capsys):
        from geoparquet_io.core.metadata_utils import format_all_metadata

        format_all_metadata(places_test_file, json_output=False)
        out = capsys.readouterr().out
        assert "Parquet File Metadata" in out
        assert "Parquet Geo Metadata" in out
        assert "GeoParquet Metadata" in out

    def test_json_output(self, places_test_file, capsys):
        import json

        from geoparquet_io.core.metadata_utils import format_all_metadata

        format_all_metadata(places_test_file, json_output=True)
        data = json.loads(capsys.readouterr().out)
        assert data["geoparquet_metadata"]["version"] == "1.0.0"


class TestFormatRowGroupGeoStats:
    """format_row_group_geo_stats: per-row-group bbox statistics."""

    def test_json_from_bbox_column(self, places_test_file, capsys):
        import json

        from geoparquet_io.core.metadata_utils import format_row_group_geo_stats

        format_row_group_geo_stats(places_test_file, json_output=True)
        data = json.loads(capsys.readouterr().out)
        stats = data["row_group_geo_stats"]
        assert len(stats) == 1
        assert stats[0]["num_rows"] == 766
        # The places extent (northern Ghana/Togo).
        assert -2 < stats[0]["xmin"] < 0
        assert 9 < stats[0]["ymin"] < 10

    def test_json_from_native_geo_stats(self, capsys):
        import json

        from geoparquet_io.core.metadata_utils import format_row_group_geo_stats

        format_row_group_geo_stats("tests/data/fields_gpq2_5070_brotli.parquet", json_output=True)
        data = json.loads(capsys.readouterr().out)
        stats = data["row_group_geo_stats"]
        assert len(stats) == 1
        assert stats[0]["num_rows"] == 100
        assert stats[0]["xmin"] < stats[0]["xmax"]

    def test_terminal_output(self, places_test_file, capsys):
        from geoparquet_io.core.metadata_utils import format_row_group_geo_stats

        format_row_group_geo_stats(places_test_file)
        out = capsys.readouterr().out
        assert "Per-Row-Group geo_bbox Statistics" in out
        assert "766" in out
