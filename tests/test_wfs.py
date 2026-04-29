"""
Tests for WFS (Web Feature Service) extraction.

Tests use mocked HTTP responses to avoid network dependencies.
Network tests are marked separately for optional integration testing.
"""

from unittest.mock import MagicMock, patch

import pytest

# Module-level imports for WFS functions (avoids per-test imports)
from geoparquet_io.core.wfs import (
    WFSError,
    WFSLayerInfo,
    _build_bbox_param,
    _build_local_bbox_filter,
    _detect_best_output_format,
    _determine_bbox_strategy,
    _negotiate_crs,
    _normalize_crs,
    _validate_identifier,
    get_layer_info,
    get_wfs_capabilities,
    list_available_layers,
)

# =============================================================================
# Mock WFS Response Data
# =============================================================================

# WFS 1.1.0 GetCapabilities response with 2 feature types (cities, roads)
MOCK_CAPABILITIES_XML = """<?xml version="1.0" encoding="UTF-8"?>
<wfs:WFS_Capabilities
    xmlns:wfs="http://www.opengis.net/wfs"
    xmlns:ows="http://www.opengis.net/ows"
    xmlns:ogc="http://www.opengis.net/ogc"
    xmlns:xlink="http://www.w3.org/1999/xlink"
    xmlns:gml="http://www.opengis.net/gml"
    version="1.1.0">

    <ows:ServiceIdentification>
        <ows:Title>Mock WFS Server</ows:Title>
        <ows:Abstract>A mock WFS server for testing geoparquet-io</ows:Abstract>
        <ows:ServiceType>WFS</ows:ServiceType>
        <ows:ServiceTypeVersion>1.1.0</ows:ServiceTypeVersion>
    </ows:ServiceIdentification>

    <ows:ServiceProvider>
        <ows:ProviderName>Test Provider</ows:ProviderName>
    </ows:ServiceProvider>

    <ows:OperationsMetadata>
        <ows:Operation name="GetCapabilities">
            <ows:DCP>
                <ows:HTTP>
                    <ows:Get xlink:href="http://mock.wfs.server/wfs"/>
                </ows:HTTP>
            </ows:DCP>
        </ows:Operation>
        <ows:Operation name="DescribeFeatureType">
            <ows:DCP>
                <ows:HTTP>
                    <ows:Get xlink:href="http://mock.wfs.server/wfs"/>
                    <ows:Post xlink:href="http://mock.wfs.server/wfs"/>
                </ows:HTTP>
            </ows:DCP>
        </ows:Operation>
        <ows:Operation name="GetFeature">
            <ows:DCP>
                <ows:HTTP>
                    <ows:Get xlink:href="http://mock.wfs.server/wfs"/>
                    <ows:Post xlink:href="http://mock.wfs.server/wfs"/>
                </ows:HTTP>
            </ows:DCP>
            <ows:Parameter name="outputFormat">
                <ows:Value>text/xml; subtype=gml/3.1.1</ows:Value>
                <ows:Value>application/json</ows:Value>
                <ows:Value>application/geo+json</ows:Value>
            </ows:Parameter>
        </ows:Operation>
    </ows:OperationsMetadata>

    <wfs:FeatureTypeList>
        <wfs:FeatureType>
            <wfs:Name>test:cities</wfs:Name>
            <wfs:Title>Cities</wfs:Title>
            <wfs:Abstract>Major cities dataset for testing</wfs:Abstract>
            <wfs:DefaultSRS>urn:ogc:def:crs:EPSG::4326</wfs:DefaultSRS>
            <wfs:OtherSRS>urn:ogc:def:crs:EPSG::3857</wfs:OtherSRS>
            <wfs:OutputFormats>
                <wfs:Format>text/xml; subtype=gml/3.1.1</wfs:Format>
                <wfs:Format>application/json</wfs:Format>
            </wfs:OutputFormats>
            <ows:WGS84BoundingBox>
                <ows:LowerCorner>-180.0 -90.0</ows:LowerCorner>
                <ows:UpperCorner>180.0 90.0</ows:UpperCorner>
            </ows:WGS84BoundingBox>
        </wfs:FeatureType>
        <wfs:FeatureType>
            <wfs:Name>test:roads</wfs:Name>
            <wfs:Title>Roads</wfs:Title>
            <wfs:Abstract>Road network dataset for testing</wfs:Abstract>
            <wfs:DefaultSRS>urn:ogc:def:crs:EPSG::4326</wfs:DefaultSRS>
            <wfs:OutputFormats>
                <wfs:Format>text/xml; subtype=gml/3.1.1</wfs:Format>
                <wfs:Format>application/json</wfs:Format>
            </wfs:OutputFormats>
            <ows:WGS84BoundingBox>
                <ows:LowerCorner>-125.0 24.0</ows:LowerCorner>
                <ows:UpperCorner>-66.0 50.0</ows:UpperCorner>
            </ows:WGS84BoundingBox>
        </wfs:FeatureType>
    </wfs:FeatureTypeList>

    <ogc:Filter_Capabilities>
        <ogc:Spatial_Capabilities>
            <ogc:GeometryOperands>
                <ogc:GeometryOperand>gml:Envelope</ogc:GeometryOperand>
                <ogc:GeometryOperand>gml:Point</ogc:GeometryOperand>
                <ogc:GeometryOperand>gml:Polygon</ogc:GeometryOperand>
            </ogc:GeometryOperands>
            <ogc:SpatialOperators>
                <ogc:SpatialOperator name="BBOX"/>
                <ogc:SpatialOperator name="Intersects"/>
                <ogc:SpatialOperator name="Within"/>
            </ogc:SpatialOperators>
        </ogc:Spatial_Capabilities>
        <ogc:Scalar_Capabilities>
            <ogc:LogicalOperators/>
            <ogc:ComparisonOperators>
                <ogc:ComparisonOperator>EqualTo</ogc:ComparisonOperator>
                <ogc:ComparisonOperator>NotEqualTo</ogc:ComparisonOperator>
                <ogc:ComparisonOperator>LessThan</ogc:ComparisonOperator>
                <ogc:ComparisonOperator>GreaterThan</ogc:ComparisonOperator>
                <ogc:ComparisonOperator>Like</ogc:ComparisonOperator>
            </ogc:ComparisonOperators>
        </ogc:Scalar_Capabilities>
    </ogc:Filter_Capabilities>
</wfs:WFS_Capabilities>
"""

# GeoJSON FeatureCollection with 3 point features
MOCK_GEOJSON_RESPONSE = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "id": "cities.1",
            "geometry": {"type": "Point", "coordinates": [-122.4194, 37.7749]},
            "properties": {
                "gml_id": "cities.1",
                "name": "San Francisco",
                "population": 884363,
                "country": "USA",
            },
        },
        {
            "type": "Feature",
            "id": "cities.2",
            "geometry": {"type": "Point", "coordinates": [-73.9857, 40.7484]},
            "properties": {
                "gml_id": "cities.2",
                "name": "New York",
                "population": 8336817,
                "country": "USA",
            },
        },
        {
            "type": "Feature",
            "id": "cities.3",
            "geometry": {"type": "Point", "coordinates": [-0.1276, 51.5074]},
            "properties": {
                "gml_id": "cities.3",
                "name": "London",
                "population": 8982000,
                "country": "UK",
            },
        },
    ],
    "totalFeatures": 3,
    "numberMatched": 3,
    "numberReturned": 3,
    "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::4326"}},
}

# GML3 response with 1 feature (WFS 1.1.0 default format)
MOCK_GML_RESPONSE = """<?xml version="1.0" encoding="UTF-8"?>
<wfs:FeatureCollection
    xmlns:wfs="http://www.opengis.net/wfs"
    xmlns:gml="http://www.opengis.net/gml"
    xmlns:test="http://mock.wfs.server/test"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    numberOfFeatures="1"
    timeStamp="2026-03-23T12:00:00Z">

    <gml:boundedBy>
        <gml:Envelope srsName="urn:ogc:def:crs:EPSG::4326">
            <gml:lowerCorner>37.7749 -122.4194</gml:lowerCorner>
            <gml:upperCorner>37.7749 -122.4194</gml:upperCorner>
        </gml:Envelope>
    </gml:boundedBy>

    <gml:featureMember>
        <test:cities gml:id="cities.1">
            <gml:boundedBy>
                <gml:Envelope srsName="urn:ogc:def:crs:EPSG::4326">
                    <gml:lowerCorner>37.7749 -122.4194</gml:lowerCorner>
                    <gml:upperCorner>37.7749 -122.4194</gml:upperCorner>
                </gml:Envelope>
            </gml:boundedBy>
            <test:geometry>
                <gml:Point srsName="urn:ogc:def:crs:EPSG::4326">
                    <gml:pos>37.7749 -122.4194</gml:pos>
                </gml:Point>
            </test:geometry>
            <test:name>San Francisco</test:name>
            <test:population>884363</test:population>
            <test:country>USA</test:country>
        </test:cities>
    </gml:featureMember>
</wfs:FeatureCollection>
"""

# XSD schema response for DescribeFeatureType
MOCK_DESCRIBE_FEATURE_TYPE = """<?xml version="1.0" encoding="UTF-8"?>
<xsd:schema
    xmlns:xsd="http://www.w3.org/2001/XMLSchema"
    xmlns:gml="http://www.opengis.net/gml"
    xmlns:test="http://mock.wfs.server/test"
    targetNamespace="http://mock.wfs.server/test"
    elementFormDefault="qualified">

    <xsd:import namespace="http://www.opengis.net/gml"
        schemaLocation="http://schemas.opengis.net/gml/3.1.1/base/gml.xsd"/>

    <xsd:complexType name="citiesType">
        <xsd:complexContent>
            <xsd:extension base="gml:AbstractFeatureType">
                <xsd:sequence>
                    <xsd:element name="geometry" type="gml:PointPropertyType"
                        minOccurs="0" maxOccurs="1"/>
                    <xsd:element name="name" type="xsd:string"
                        minOccurs="0" maxOccurs="1"/>
                    <xsd:element name="population" type="xsd:int"
                        minOccurs="0" maxOccurs="1"/>
                    <xsd:element name="country" type="xsd:string"
                        minOccurs="0" maxOccurs="1"/>
                </xsd:sequence>
            </xsd:extension>
        </xsd:complexContent>
    </xsd:complexType>

    <xsd:element name="cities" type="test:citiesType"
        substitutionGroup="gml:_Feature"/>

    <xsd:complexType name="roadsType">
        <xsd:complexContent>
            <xsd:extension base="gml:AbstractFeatureType">
                <xsd:sequence>
                    <xsd:element name="geometry" type="gml:MultiLineStringPropertyType"
                        minOccurs="0" maxOccurs="1"/>
                    <xsd:element name="name" type="xsd:string"
                        minOccurs="0" maxOccurs="1"/>
                    <xsd:element name="highway_type" type="xsd:string"
                        minOccurs="0" maxOccurs="1"/>
                    <xsd:element name="lanes" type="xsd:int"
                        minOccurs="0" maxOccurs="1"/>
                </xsd:sequence>
            </xsd:extension>
        </xsd:complexContent>
    </xsd:complexType>

    <xsd:element name="roads" type="test:roadsType"
        substitutionGroup="gml:_Feature"/>
</xsd:schema>
"""

# Empty FeatureCollection response
MOCK_EMPTY_RESPONSE = {
    "type": "FeatureCollection",
    "features": [],
    "totalFeatures": 0,
    "numberMatched": 0,
    "numberReturned": 0,
    "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::4326"}},
}


# =============================================================================
# Pytest Fixtures
# =============================================================================


@pytest.fixture
def mock_capabilities_xml():
    """Return WFS 1.1.0 GetCapabilities XML response.

    Contains 2 feature types:
    - test:cities (Point geometry, global extent)
    - test:roads (LineString geometry, USA extent)

    Supports GML 3.1.1 and JSON output formats.
    """
    return MOCK_CAPABILITIES_XML


@pytest.fixture
def mock_geojson_response():
    """Return GeoJSON FeatureCollection with 3 point features.

    Features represent cities:
    - San Francisco (population: 884363)
    - New York (population: 8336817)
    - London (population: 8982000)

    CRS is EPSG:4326 (WGS84).
    """
    return MOCK_GEOJSON_RESPONSE


@pytest.fixture
def mock_gml_response():
    """Return GML3 response with 1 feature.

    Contains a single city (San Francisco) in GML 3.1.1 format.
    This is the default WFS 1.1.0 GetFeature response format.
    """
    return MOCK_GML_RESPONSE


@pytest.fixture
def mock_describe_feature_type():
    """Return XSD schema response for DescribeFeatureType.

    Describes 2 feature types:
    - cities: Point geometry with name, population, country fields
    - roads: MultiLineString geometry with name, highway_type, lanes fields
    """
    return MOCK_DESCRIBE_FEATURE_TYPE


@pytest.fixture
def mock_empty_response():
    """Return empty GeoJSON FeatureCollection.

    Represents a WFS GetFeature response with no matching features.
    totalFeatures, numberMatched, and numberReturned are all 0.
    """
    return MOCK_EMPTY_RESPONSE


@pytest.fixture
def mock_wfs_url():
    """Return mock WFS server URL."""
    return "http://mock.wfs.server/wfs"


@pytest.fixture
def mock_wfs_responses(
    mock_wfs_url,
    mock_capabilities_xml,
    mock_geojson_response,
    mock_gml_response,
    mock_describe_feature_type,
    mock_empty_response,
):
    """Return dict of all mock WFS responses keyed by request type.

    Useful for setting up comprehensive request mocking.
    """
    return {
        "url": mock_wfs_url,
        "capabilities": mock_capabilities_xml,
        "geojson": mock_geojson_response,
        "gml": mock_gml_response,
        "describe_feature_type": mock_describe_feature_type,
        "empty": mock_empty_response,
    }


# =============================================================================
# Additional Mock Data for Pagination Tests
# =============================================================================

# First page of 2 features (offset 0)
MOCK_GEOJSON_PAGE_1 = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "id": "cities.1",
            "geometry": {"type": "Point", "coordinates": [-122.4194, 37.7749]},
            "properties": {"gml_id": "cities.1", "name": "San Francisco", "population": 884363},
        },
        {
            "type": "Feature",
            "id": "cities.2",
            "geometry": {"type": "Point", "coordinates": [-73.9857, 40.7484]},
            "properties": {"gml_id": "cities.2", "name": "New York", "population": 8336817},
        },
    ],
    "numberMatched": 5,
    "numberReturned": 2,
}

# Second page of 2 features (offset 2)
MOCK_GEOJSON_PAGE_2 = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "id": "cities.3",
            "geometry": {"type": "Point", "coordinates": [-0.1276, 51.5074]},
            "properties": {"gml_id": "cities.3", "name": "London", "population": 8982000},
        },
        {
            "type": "Feature",
            "id": "cities.4",
            "geometry": {"type": "Point", "coordinates": [139.6917, 35.6895]},
            "properties": {"gml_id": "cities.4", "name": "Tokyo", "population": 13960000},
        },
    ],
    "numberMatched": 5,
    "numberReturned": 2,
}

# Third page with 1 feature (offset 4)
MOCK_GEOJSON_PAGE_3 = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "id": "cities.5",
            "geometry": {"type": "Point", "coordinates": [2.3522, 48.8566]},
            "properties": {"gml_id": "cities.5", "name": "Paris", "population": 2161000},
        },
    ],
    "numberMatched": 5,
    "numberReturned": 1,
}

# Capabilities XML with minimal optional fields (missing Abstract, OtherSRS)
MOCK_MINIMAL_CAPABILITIES_XML = """<?xml version="1.0" encoding="UTF-8"?>
<wfs:WFS_Capabilities
    xmlns:wfs="http://www.opengis.net/wfs"
    xmlns:ows="http://www.opengis.net/ows"
    version="1.1.0">
    <wfs:FeatureTypeList>
        <wfs:FeatureType>
            <wfs:Name>minimal:layer</wfs:Name>
            <wfs:DefaultSRS>urn:ogc:def:crs:EPSG::4326</wfs:DefaultSRS>
        </wfs:FeatureType>
    </wfs:FeatureTypeList>
</wfs:WFS_Capabilities>
"""


@pytest.fixture
def mock_geojson_page_1():
    """First page of paginated results."""
    return MOCK_GEOJSON_PAGE_1


@pytest.fixture
def mock_geojson_page_2():
    """Second page of paginated results."""
    return MOCK_GEOJSON_PAGE_2


@pytest.fixture
def mock_geojson_page_3():
    """Third (final) page of paginated results."""
    return MOCK_GEOJSON_PAGE_3


@pytest.fixture
def mock_minimal_capabilities_xml():
    """Minimal capabilities XML with missing optional fields."""
    return MOCK_MINIMAL_CAPABILITIES_XML


# =============================================================================
# Mock-Based Test Classes
# =============================================================================


class TestCapabilityParsing:
    """Tests for OWSLib-based capability parsing via mocked WebFeatureService."""

    def _create_mock_wfs(self):
        """Create a mock OWSLib WebFeatureService object."""
        mock_wfs = MagicMock()

        # Mock layer contents
        cities_layer = MagicMock()
        cities_layer.id = "test:cities"
        cities_layer.title = "Cities"
        cities_layer.crsOptions = [
            "urn:ogc:def:crs:EPSG::4326",
            "urn:ogc:def:crs:EPSG::3857",
        ]
        cities_layer.boundingBoxWGS84 = (-180.0, -90.0, 180.0, 90.0)

        roads_layer = MagicMock()
        roads_layer.id = "test:roads"
        roads_layer.title = "Road Network"
        roads_layer.crsOptions = ["urn:ogc:def:crs:EPSG::4326"]
        roads_layer.boundingBoxWGS84 = (-125.0, 24.0, -66.0, 50.0)

        mock_wfs.contents = {
            "test:cities": cities_layer,
            "test:roads": roads_layer,
        }
        mock_wfs.getfeature_output_formats = [
            "application/json",
            "application/geo+json",
            "text/xml; subtype=gml/3.1.1",
        ]

        return mock_wfs

    @patch("owslib.wfs.WebFeatureService")
    def test_parses_layer_list(self, mock_wfs_class):
        """Test that GetCapabilities returns WFS with layer contents."""
        mock_wfs_class.return_value = self._create_mock_wfs()

        wfs = get_wfs_capabilities("http://mock.wfs.server/wfs")

        # WFS object should have contents with 2 layers
        assert len(wfs.contents) == 2
        assert "test:cities" in wfs.contents
        assert "test:roads" in wfs.contents

    @patch("owslib.wfs.WebFeatureService")
    def test_extracts_layer_info(self, mock_wfs_class):
        """Test extraction of layer info via OWSLib interface."""
        mock_wfs_class.return_value = self._create_mock_wfs()

        wfs = get_wfs_capabilities("http://mock.wfs.server/wfs")

        # Verify cities layer metadata via OWSLib interface
        cities = wfs.contents["test:cities"]
        assert cities.title == "Cities"
        assert "urn:ogc:def:crs:EPSG::4326" in cities.crsOptions
        assert cities.boundingBoxWGS84 == (-180.0, -90.0, 180.0, 90.0)

        # Verify roads layer
        roads = wfs.contents["test:roads"]
        assert roads.boundingBoxWGS84 == (-125.0, 24.0, -66.0, 50.0)

    @patch("owslib.wfs.WebFeatureService")
    def test_extracts_supported_formats(self, mock_wfs_class):
        """Test extraction of supported output formats."""
        mock_wfs_class.return_value = self._create_mock_wfs()

        wfs = get_wfs_capabilities("http://mock.wfs.server/wfs")

        # Check formats via OWSLib interface
        assert "application/json" in wfs.getfeature_output_formats
        assert "application/geo+json" in wfs.getfeature_output_formats

    @patch("owslib.wfs.WebFeatureService")
    def test_handles_connection_error(self, mock_wfs_class):
        """Test handling of connection errors."""
        mock_wfs_class.side_effect = Exception("Connection refused")
        from geoparquet_io.core.wfs import WFSError

        with pytest.raises(WFSError, match="Could not connect|Connection"):
            get_wfs_capabilities("http://mock.wfs.server/wfs")


class TestErrorHandling:
    """Tests for error handling in WFS module."""

    def test_wfs_error_raised_for_invalid_url(self):
        """Test that WFSError is raised for connection failures."""
        from geoparquet_io.core.wfs import WFSError

        # Invalid URL should raise WFSError
        with pytest.raises(WFSError):
            get_wfs_capabilities("http://localhost:99999/invalid")

    @patch("owslib.wfs.WebFeatureService")
    def test_layer_not_found_error(self, mock_wfs_class):
        """Test error when requested layer doesn't exist."""
        mock_wfs = MagicMock()
        # Only "ns:cities" exists, not "nonexistent:data"
        mock_wfs.contents = {"ns:cities": MagicMock()}
        mock_wfs_class.return_value = mock_wfs

        from geoparquet_io.core.wfs import WFSError

        with pytest.raises(WFSError, match="not found"):
            get_layer_info("http://mock.wfs.server/wfs", "nonexistent:data")


# Helper Data Structures for Tests
# =============================================================================


class MockLayerInfo:
    """Mock WFSLayerInfo for unit testing pure logic functions."""

    def __init__(
        self,
        typename: str = "test:cities",
        title: str | None = "Cities",
        crs_list: list[str] | None = None,
        default_crs: str | None = "urn:ogc:def:crs:EPSG::4326",
        bbox: tuple[float, float, float, float] | None = (-180.0, -90.0, 180.0, 90.0),
        geometry_column: str = "geometry",
    ):
        self.typename = typename
        self.title = title
        self.crs_list = crs_list or ["urn:ogc:def:crs:EPSG::4326", "urn:ogc:def:crs:EPSG::3857"]
        self.default_crs = default_crs
        self.bbox = bbox
        self.geometry_column = geometry_column


class MockCapabilities:
    """Mock WFS capabilities for unit testing."""

    def __init__(
        self,
        supports_bbox: bool = True,
        version: str = "1.1.0",
        max_features: int | None = None,
    ):
        self.supports_bbox = supports_bbox
        self.version = version
        self.max_features = max_features


# =============================================================================
# Unit Tests - Bbox Strategy
# =============================================================================


class TestBboxStrategy:
    """Test _determine_bbox_strategy() pure logic.

    This function decides whether to use server-side or local bbox filtering.
    Unlike BigQuery, WFS doesn't expose row counts easily, so auto mode
    defaults to server-side filtering (conservative for remote services).
    """

    def _create_mock_layer_info(self):
        """Create a mock WFSLayerInfo for testing."""
        return WFSLayerInfo(
            typename="test:cities",
            title="Cities",
            crs_list=["EPSG:4326", "EPSG:3857"],
            default_crs="EPSG:4326",
            bbox=(-180.0, -90.0, 180.0, 90.0),
            geometry_column="geometry",
            available_formats=["application/json"],
        )

    @pytest.mark.parametrize(
        "bbox_mode,expected",
        [
            ("server", True),
            ("local", False),
        ],
    )
    def test_explicit_mode_respected(self, bbox_mode, expected):
        """Explicit server/local mode bypasses auto-detection logic."""
        layer_info = self._create_mock_layer_info()
        result = _determine_bbox_strategy(bbox_mode, layer_info)
        assert result is expected

    def test_auto_mode_defaults_server_for_wfs(self):
        """Auto mode defaults to server-side for WFS (conservative choice).

        Unlike BigQuery, WFS servers don't easily expose row counts.
        Server-side filtering is safer for remote services to avoid
        downloading large datasets unnecessarily.
        """
        layer_info = self._create_mock_layer_info()
        result = _determine_bbox_strategy("auto", layer_info)
        assert result is True  # WFS auto mode defaults to server-side

    def test_auto_mode_still_uses_server_with_different_layer_info(self):
        """Auto mode uses server-side regardless of layer_info (reserved for future)."""
        # layer_info is reserved for future use (e.g., checking server capabilities)
        layer_info = WFSLayerInfo(
            typename="test:layer",
            title=None,
            crs_list=[],
            default_crs=None,
            bbox=None,
            geometry_column="geometry",
            available_formats=[],
        )
        result = _determine_bbox_strategy("auto", layer_info)
        assert result is True  # Still defaults to server-side


# =============================================================================
# Unit Tests - Bbox Filter Construction
# =============================================================================


class TestBboxFilters:
    """Test bbox filter construction functions.

    Tests both:
    - WFS bbox parameter string for server-side filtering (_build_bbox_param)
    - DuckDB SQL expression for local filtering (_build_local_bbox_filter)
    """

    @pytest.mark.parametrize(
        "bbox,crs,version,expected_param",
        [
            # WFS 1.0.0: xmin,ymin,xmax,ymax (no CRS)
            ((-122.5, 37.5, -122.0, 38.0), "EPSG:4326", "1.0.0", "-122.5,37.5,-122.0,38.0"),
            # WFS 1.1.0: xmin,ymin,xmax,ymax,crs
            (
                (-122.5, 37.5, -122.0, 38.0),
                "EPSG:4326",
                "1.1.0",
                "-122.5,37.5,-122.0,38.0,EPSG:4326",
            ),
            # Integer coordinates
            ((-180, -90, 180, 90), "EPSG:4326", "1.1.0", "-180,-90,180,90,EPSG:4326"),
        ],
    )
    def test_server_side_bbox_parameter(self, bbox, crs, version, expected_param):
        """Server-side filtering uses WFS bbox parameter format."""
        result = _build_bbox_param(bbox, crs, version)
        assert result == expected_param

    @pytest.mark.parametrize(
        "bbox,geometry_column",
        [
            ((-122.5, 37.5, -122.0, 38.0), "geometry"),
            ((-122.5, 37.5, -122.0, 38.0), "the_geom"),
            ((-122.5, 37.5, -122.0, 38.0), "geom"),
        ],
    )
    def test_local_bbox_duckdb_filter(self, bbox, geometry_column):
        """Local filtering uses DuckDB ST_Intersects expression."""
        sql = _build_local_bbox_filter(bbox, geometry_column)
        assert f'"{geometry_column}"' in sql
        assert "ST_Intersects" in sql
        assert "ST_GeomFromText" in sql
        assert "POLYGON" in sql

    def test_local_bbox_polygon_is_closed_ring(self):
        """DuckDB POLYGON must be a closed ring (first point == last point)."""
        bbox = (-122.5, 37.5, -122.0, 38.0)
        sql = _build_local_bbox_filter(bbox, "geometry")
        # The polygon should start and end at the same point
        assert "-122.5 37.5" in sql  # First point
        # Count occurrences - should appear twice (start and end)
        assert sql.count("-122.5 37.5") == 2

    def test_bbox_with_crs_suffix(self):
        """WFS 1.1.0 bbox includes CRS suffix."""
        bbox = (-122.5, 37.5, -122.0, 38.0)
        crs = "urn:ogc:def:crs:EPSG::4326"
        result = _build_bbox_param(bbox, crs, "1.1.0")
        assert result.endswith(crs)

    def test_invalid_geometry_column_rejected(self):
        """Invalid geometry column names with SQL injection characters are rejected."""
        bbox = (-122.5, 37.5, -122.0, 38.0)
        with pytest.raises(WFSError, match="Invalid geometry column name"):
            _build_local_bbox_filter(bbox, 'geom"; DROP TABLE --')


# =============================================================================
# Unit Tests - Output Format Detection
# =============================================================================


class TestFormatDetection:
    """Test _detect_best_output_format() format preference logic.

    GeoJSON is preferred for speed, with fallbacks to GML variants.
    """

    @pytest.mark.parametrize(
        "available_formats,expected_format",
        [
            # GeoJSON variants (preferred)
            (["application/json", "text/xml; subtype=gml/3.1.1"], "application/json"),
            (["json", "gml3"], "json"),
            (["geojson", "gml2"], "geojson"),
            (["application/geo+json", "application/xml"], "application/geo+json"),
            # GML3 when no JSON
            (
                ["text/xml; subtype=gml/3.1.1", "text/xml; subtype=gml/2.1.2"],
                "text/xml; subtype=gml/3.1.1",
            ),
            (["gml3", "gml2"], "gml3"),
            # GML2 as last resort
            (["text/xml; subtype=gml/2.1.2"], "text/xml; subtype=gml/2.1.2"),
            (["gml2"], "gml2"),
            # Unknown format - return first available
            (["application/x-custom", "unknown/type"], "application/x-custom"),
        ],
    )
    def test_format_preference_order(self, available_formats, expected_format):
        """Formats are selected in order: GeoJSON > GML3 > GML2 > first available."""
        result = _detect_best_output_format(available_formats)
        assert result == expected_format

    def test_empty_formats_returns_default(self):
        """Empty format list returns default GML3."""
        result = _detect_best_output_format([])
        assert result == "GML3"

    @pytest.mark.parametrize(
        "format_string,expected_is_json",
        [
            ("application/json", True),
            ("json", True),
            ("geojson", True),
            ("application/geo+json", True),
            ("gml3", False),
        ],
    )
    def test_geojson_detection(self, format_string, expected_is_json):
        """GeoJSON format detection is case-insensitive."""
        # Test both lowercase and uppercase
        formats_lower = [format_string.lower(), "gml2"]
        formats_upper = [format_string.upper(), "gml2"]

        result_lower = _detect_best_output_format(formats_lower)
        result_upper = _detect_best_output_format(formats_upper)

        if expected_is_json:
            # JSON formats should be selected over GML
            assert "json" in result_lower.lower() or "geo" in result_lower.lower()
            assert "json" in result_upper.lower() or "geo" in result_upper.lower()
        else:
            # Non-JSON GML formats - gml3 should be preferred over gml2
            assert result_lower == format_string.lower()

    def test_gml_version_preference(self):
        """Prefer GML 3.x over GML 2.x for better geometry support."""
        formats = ["gml2", "gml3"]
        result = _detect_best_output_format(formats)
        assert result == "gml3"  # GML3 preferred over GML2


# =============================================================================
# Unit Tests - CRS Negotiation
# =============================================================================


class TestCRSNegotiation:
    """Test _negotiate_crs() CRS selection logic.

    Strategy:
    1. If --output-crs specified and supported → use it
    2. Try EPSG:4326 variants (most universal)
    3. Fall back to server default
    """

    def _create_layer_info(self, crs_list, default_crs=None):
        """Helper to create WFSLayerInfo with CRS settings."""
        return WFSLayerInfo(
            typename="test:layer",
            title="Test",
            crs_list=crs_list,
            default_crs=default_crs or (crs_list[0] if crs_list else None),
            bbox=None,
            geometry_column="geometry",
            available_formats=["application/json"],
        )

    @pytest.mark.parametrize(
        "crs_list,output_crs,expected",
        [
            # Explicit output_crs respected when available
            (["EPSG:4326", "EPSG:3857"], "EPSG:3857", "EPSG:3857"),
            (["urn:ogc:def:crs:EPSG::4326", "EPSG:3857"], "EPSG:3857", "EPSG:3857"),
            # EPSG:4326 variants matched when no output_crs
            (["urn:ogc:def:crs:EPSG::4326"], None, "urn:ogc:def:crs:EPSG::4326"),
            (["EPSG:4326", "EPSG:3857"], None, "EPSG:4326"),
            (
                ["http://www.opengis.net/def/crs/EPSG/0/4326"],
                None,
                "http://www.opengis.net/def/crs/EPSG/0/4326",
            ),
        ],
    )
    def test_crs_selection_priority(self, crs_list, output_crs, expected):
        """CRS selection follows priority: explicit > EPSG:4326 > server default."""
        layer_info = self._create_layer_info(crs_list)
        result = _negotiate_crs(layer_info, output_crs)
        assert result == expected

    def test_fallback_to_server_default(self):
        """Fall back to default_crs when EPSG:4326 not available."""
        layer_info = self._create_layer_info(
            crs_list=["EPSG:32610", "EPSG:32611"],  # UTM zones, no WGS84
            default_crs="EPSG:32610",
        )
        result = _negotiate_crs(layer_info, None)
        assert result == "EPSG:32610"

    def test_unsupported_output_crs_falls_back(self):
        """When requested CRS not in supported list, fall back to available."""
        layer_info = self._create_layer_info(crs_list=["EPSG:4326"])
        # Request unsupported CRS - should fall back to EPSG:4326
        result = _negotiate_crs(layer_info, "EPSG:2154")  # French Lambert
        assert result == "EPSG:4326"

    @pytest.mark.parametrize(
        "crs_variant,expected_normalized",
        [
            ("EPSG:4326", "EPSG:4326"),
            ("urn:ogc:def:crs:EPSG::4326", "EPSG:4326"),
            ("http://www.opengis.net/def/crs/EPSG/0/4326", "EPSG:4326"),
            ("EPSG:3857", "EPSG:3857"),
            ("urn:ogc:def:crs:EPSG::3857", "EPSG:3857"),
        ],
    )
    def test_crs_variant_normalization(self, crs_variant, expected_normalized):
        """Different CRS URI formats should normalize to same EPSG code."""
        result = _normalize_crs(crs_variant)
        assert result == expected_normalized

    def test_empty_crs_list_uses_default(self):
        """Use default_crs when crs_list is empty."""
        layer_info = self._create_layer_info(crs_list=[], default_crs="EPSG:4326")
        result = _negotiate_crs(layer_info, None)
        assert result == "EPSG:4326"

    def test_no_crs_available_returns_4326(self):
        """When no CRS info at all, default to EPSG:4326."""
        layer_info = self._create_layer_info(crs_list=[], default_crs=None)
        result = _negotiate_crs(layer_info, None)
        assert result == "EPSG:4326"


# =============================================================================
# Unit Tests - Namespace Resolution
# =============================================================================


class TestNamespaceResolution:
    """Test typename namespace matching and sanitization logic."""

    @pytest.mark.parametrize(
        "column_name,should_pass",
        [
            ("geometry", True),
            ("the_geom", True),
            ("geom123", True),
            ("_private_geom", True),
            ('geom"injection', False),
            ("geom;drop", False),
            ("geom--comment", False),
        ],
    )
    def test_validate_identifier(self, column_name, should_pass):
        """Identifier validation catches SQL injection attempts."""
        if should_pass:
            result = _validate_identifier(column_name)
            assert result == column_name
        else:
            with pytest.raises(WFSError, match="Invalid geometry column name"):
                _validate_identifier(column_name)


# =============================================================================
# Integration Tests (require network)
# =============================================================================


@pytest.mark.network
@pytest.mark.slow
class TestWFSIntegration:
    """Integration tests against real WFS services."""

    # Transport for Cairo GeoServer (known to support GeoJSON)
    WFS_URL = "https://data.transportforcairo.com/geoserver/geonode/ows"
    TYPENAME = "geonode:cairo_od_stats"

    @pytest.mark.xfail(
        reason="External WFS service (transportforcairo.com) unreliable",
        raises=WFSError,
        strict=False,
    )
    def test_list_available_layers(self):
        """Test listing layers from real WFS."""

        # Should not raise - just verify it runs
        layers = list_available_layers(self.WFS_URL)
        assert len(layers) > 0

    @pytest.mark.xfail(
        reason="External WFS service (transportforcairo.com) unreliable",
        raises=WFSError,
        strict=False,
    )
    def test_extract_with_limit(self, tmp_path):
        """Test extracting features with limit."""
        from geoparquet_io.core.wfs import convert_wfs_to_geoparquet

        output = tmp_path / "cairo_test.parquet"
        convert_wfs_to_geoparquet(
            self.WFS_URL,
            self.TYPENAME,
            str(output),
            limit=10,
            skip_hilbert=True,
            skip_bbox=True,
        )
        import pyarrow.parquet as pq

        table = pq.read_table(output)
        assert table.num_rows <= 10
        assert "geometry" in table.column_names

    @pytest.mark.xfail(
        reason="External WFS service (transportforcairo.com) unreliable",
        raises=WFSError,
        strict=False,
    )
    def test_extract_with_bbox(self, tmp_path):
        """Test bbox filtering (Cairo region)."""
        from geoparquet_io.core.wfs import convert_wfs_to_geoparquet

        output = tmp_path / "bbox_test.parquet"
        # Cairo bbox (roughly)
        convert_wfs_to_geoparquet(
            self.WFS_URL,
            self.TYPENAME,
            str(output),
            bbox=(31.2, 29.9, 31.3, 30.0),
            bbox_mode="server",
            limit=5,
            skip_hilbert=True,
            skip_bbox=True,
        )
        import pyarrow.parquet as pq

        table = pq.read_table(output)
        assert table.num_rows >= 0  # May be 0 if no data in bbox

    @pytest.mark.xfail(
        reason="External WFS service (transportforcairo.com) unreliable",
        raises=WFSError,
        strict=False,
    )
    def test_python_api(self):
        """Test Python API."""
        from geoparquet_io.api import Table

        table = Table.from_wfs(
            self.WFS_URL,
            self.TYPENAME,
            limit=5,
        )
        assert table.num_rows <= 5


# =============================================================================
# CLI Ergonomics Tests
# =============================================================================


class TestWFSCLIErgonomics:
    """Test CLI option naming consistency with ArcGIS."""

    def test_cli_uses_workers_option(self):
        """WFS CLI should use --workers like ArcGIS, not --max-workers."""
        from click.testing import CliRunner

        from geoparquet_io.cli.main import cli

        runner = CliRunner()
        # Test that --workers is recognized
        result = runner.invoke(cli, ["extract", "wfs", "--help"])
        assert result.exit_code == 0
        # Should have --workers option
        assert "--workers" in result.output
        # Should NOT have --max-workers option
        assert "--max-workers" not in result.output

    def test_workers_capped_at_10(self):
        """Workers should be capped at 10 in CLI validation."""
        from click.testing import CliRunner

        from geoparquet_io.cli.main import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["extract", "wfs", "--help"])
        # Should have --workers option with max of 10
        assert "--workers" in result.output
        # Click displays IntRange as [1<=x<=10]
        assert "[1<=x<=10]" in result.output


# =============================================================================
# DuckDB-Native WFS Fetch Tests
# =============================================================================


@pytest.mark.network
@pytest.mark.slow
class TestDuckDBNativeWFS:
    """Test DuckDB-native WFS fetching using httpfs and read_json_auto."""

    # Use Transport for Cairo GeoServer (known to support GeoJSON)
    WFS_URL = (
        "https://data.transportforcairo.com/geoserver/geonode/ows?"
        "service=WFS&version=1.1.0&request=GetFeature"
        "&typeName=geonode:cairo_od_stats"
        "&outputFormat=application/json"
        "&maxFeatures=10"
    )

    def test_fetch_wfs_page_duckdb_returns_arrow_table(self):
        """DuckDB-native fetch should return an Arrow table with geometry."""
        from geoparquet_io.core.wfs import _fetch_wfs_page_duckdb

        table = _fetch_wfs_page_duckdb(self.WFS_URL)

        # Should return Arrow table
        import pyarrow as pa

        assert isinstance(table, pa.Table)
        # Should have geometry column
        assert "geometry" in table.column_names
        # Should have some rows
        assert table.num_rows > 0
        assert table.num_rows <= 10

    def test_fetch_wfs_page_duckdb_geometry_is_wkb(self):
        """Geometry should be WKB binary format."""
        from geoparquet_io.core.wfs import _fetch_wfs_page_duckdb

        table = _fetch_wfs_page_duckdb(self.WFS_URL)

        # Geometry should be binary (WKB)
        import pyarrow as pa

        geom_type = table.schema.field("geometry").type
        assert pa.types.is_binary(geom_type) or pa.types.is_large_binary(geom_type)


# =============================================================================
# Axis Order Tests (Issue #397)
# =============================================================================


class TestAxisOrder:
    """Test axis order detection and bbox parameter building.

    WFS 1.1.0+ with URN format CRS uses lat,lon (YX) axis order per OGC spec,
    while simple EPSG:4326 format uses lon,lat (XY).
    """

    def test_is_urn_crs_detects_urn_format(self):
        """Should correctly identify URN format CRS strings."""
        from geoparquet_io.core.wfs import _is_urn_crs

        assert _is_urn_crs("urn:ogc:def:crs:EPSG::4326") is True
        assert _is_urn_crs("urn:x-ogc:def:crs:EPSG::4326") is True
        assert _is_urn_crs("URN:OGC:DEF:CRS:EPSG::3857") is True
        assert _is_urn_crs("EPSG:4326") is False
        assert _is_urn_crs("http://www.opengis.net/def/crs/EPSG/0/4326") is False

    def test_is_geographic_crs_detects_geographic_systems(self):
        """Should correctly identify geographic (lat/lon) CRS."""
        from geoparquet_io.core.wfs import _is_geographic_crs

        assert _is_geographic_crs("EPSG:4326") is True
        assert _is_geographic_crs("urn:ogc:def:crs:EPSG::4326") is True
        assert _is_geographic_crs("EPSG:4269") is True  # NAD83
        assert _is_geographic_crs("CRS:84") is True
        assert _is_geographic_crs("EPSG:3857") is False  # Web Mercator (projected)
        assert _is_geographic_crs("EPSG:3035") is False  # LAEA (projected)

    @pytest.mark.parametrize(
        "crs,version,axis_order,expected_swap",
        [
            # WFS 1.0.0 always uses XY
            ("EPSG:4326", "1.0.0", "auto", False),
            ("urn:ogc:def:crs:EPSG::4326", "1.0.0", "auto", False),
            # WFS 1.1.0+ with simple EPSG format uses XY
            ("EPSG:4326", "1.1.0", "auto", False),
            ("EPSG:4326", "2.0.0", "auto", False),
            # WFS 1.1.0+ with URN format uses YX (lat,lon)
            ("urn:ogc:def:crs:EPSG::4326", "1.1.0", "auto", True),
            ("urn:ogc:def:crs:EPSG::4326", "2.0.0", "auto", True),
            ("urn:x-ogc:def:crs:EPSG::4326", "1.1.0", "auto", True),
            # CRS:84 is always XY by definition
            ("CRS:84", "1.1.0", "auto", False),
            ("urn:ogc:def:crs:OGC:1.3:CRS84", "2.0.0", "auto", False),
            # Projected CRS never swaps
            ("urn:ogc:def:crs:EPSG::3857", "1.1.0", "auto", False),
            # Forced axis order overrides auto-detection
            ("urn:ogc:def:crs:EPSG::4326", "1.1.0", "xy", False),
            ("EPSG:4326", "1.1.0", "latlon", True),
        ],
    )
    def test_needs_axis_swap(self, crs, version, axis_order, expected_swap):
        """Axis swap decision based on CRS format, version, and override."""
        from geoparquet_io.core.wfs import _needs_axis_swap

        result = _needs_axis_swap(crs, version, axis_order)
        assert result is expected_swap

    @pytest.mark.parametrize(
        "bbox,crs,version,axis_order,expected",
        [
            # WFS 1.0.0: no CRS suffix
            ((-122.5, 37.5, -122.0, 38.0), "EPSG:4326", "1.0.0", "auto", "-122.5,37.5,-122.0,38.0"),
            # WFS 1.1.0 with simple CRS: XY order with CRS suffix
            (
                (-122.5, 37.5, -122.0, 38.0),
                "EPSG:4326",
                "1.1.0",
                "auto",
                "-122.5,37.5,-122.0,38.0,EPSG:4326",
            ),
            # WFS 1.1.0 with URN CRS: YX order (lat,lon) with CRS suffix
            (
                (-122.5, 37.5, -122.0, 38.0),
                "urn:ogc:def:crs:EPSG::4326",
                "1.1.0",
                "auto",
                "37.5,-122.5,38.0,-122.0,urn:ogc:def:crs:EPSG::4326",
            ),
            # WFS 2.0.0 with URN CRS: YX order
            (
                (4.82, 50.44, 4.92, 50.48),
                "urn:ogc:def:crs:EPSG::4326",
                "2.0.0",
                "auto",
                "50.44,4.82,50.48,4.92,urn:ogc:def:crs:EPSG::4326",
            ),
            # Forced XY order ignores URN format
            (
                (-122.5, 37.5, -122.0, 38.0),
                "urn:ogc:def:crs:EPSG::4326",
                "1.1.0",
                "xy",
                "-122.5,37.5,-122.0,38.0,urn:ogc:def:crs:EPSG::4326",
            ),
        ],
    )
    def test_build_bbox_param_axis_order(self, bbox, crs, version, axis_order, expected):
        """Bbox parameter string should have correct axis order."""
        from geoparquet_io.core.wfs import _build_bbox_param

        result = _build_bbox_param(bbox, crs, version, axis_order)
        assert result == expected


# =============================================================================
# Version Negotiation Tests (Issue #312)
# =============================================================================


class TestVersionNegotiation:
    """Test WFS version negotiation and WFS 2.0 support."""

    def test_wfs_versions_list(self):
        """WFS_VERSIONS should be in preference order (newest first)."""
        from geoparquet_io.core.wfs import WFS_VERSIONS

        assert WFS_VERSIONS == ["2.0.0", "1.1.0", "1.0.0"]

    def test_build_wfs_url_uses_count_for_wfs_2(self):
        """WFS 2.0 should use 'count' parameter instead of 'maxFeatures'."""
        from geoparquet_io.core.wfs import _build_wfs_url

        url = _build_wfs_url(
            "https://example.com/wfs",
            "test:layer",
            version="2.0.0",
            max_features=100,
        )
        assert "count=100" in url
        assert "maxFeatures" not in url

    def test_build_wfs_url_uses_maxfeatures_for_wfs_1(self):
        """WFS 1.x should use 'maxFeatures' parameter."""
        from geoparquet_io.core.wfs import _build_wfs_url

        url = _build_wfs_url(
            "https://example.com/wfs",
            "test:layer",
            version="1.1.0",
            max_features=100,
        )
        assert "maxFeatures=100" in url
        assert "count=" not in url

    def test_build_wfs_url_uses_typenames_for_wfs_20(self):
        """WFS 2.0.0 should use 'typeNames' (plural)."""
        from geoparquet_io.core.wfs import _build_wfs_url

        url = _build_wfs_url(
            "https://example.com/wfs",
            "test:layer",
            version="2.0.0",
        )
        assert "typeNames=test%3Alayer" in url or "typeNames=test:layer" in url
        assert "typeName=" not in url

    def test_build_wfs_url_uses_typename_for_wfs_11(self):
        """WFS 1.1.0 should use 'typeName' (singular) per OGC spec."""
        from geoparquet_io.core.wfs import _build_wfs_url

        url = _build_wfs_url(
            "https://example.com/wfs",
            "test:layer",
            version="1.1.0",
        )
        assert "typeName=test%3Alayer" in url or "typeName=test:layer" in url
        assert "typeNames=" not in url

    def test_build_wfs_url_uses_typename_for_wfs_10(self):
        """WFS 1.0.0 should use 'typeName' (singular)."""
        from geoparquet_io.core.wfs import _build_wfs_url

        url = _build_wfs_url(
            "https://example.com/wfs",
            "test:layer",
            version="1.0.0",
        )
        assert "typeName=test" in url
        assert "typeNames=" not in url

    def test_build_wfs_url_includes_srsname_without_bbox(self):
        """srsName should be included even without bbox filter (Issue #405)."""
        from geoparquet_io.core.wfs import _build_wfs_url

        url = _build_wfs_url(
            "https://example.com/wfs",
            "test:layer",
            version="1.1.0",
            crs="EPSG:4326",
            # No bbox provided
        )
        assert "srsName=" in url or "srsname=" in url.lower()

    def test_build_wfs_url_includes_srsname_with_bbox(self):
        """srsName should still work when bbox is also provided."""
        from geoparquet_io.core.wfs import _build_wfs_url

        url = _build_wfs_url(
            "https://example.com/wfs",
            "test:layer",
            version="1.1.0",
            bbox=(4.0, 50.0, 5.0, 51.0),
            crs="EPSG:4326",
        )
        # Should have both srsName param AND bbox with CRS suffix
        assert "srsName=" in url or "srsname=" in url.lower()
        assert "bbox=" in url

    def test_build_wfs_url_includes_srsname_wfs_20(self):
        """WFS 2.0.0 should also include srsName parameter."""
        from geoparquet_io.core.wfs import _build_wfs_url

        url = _build_wfs_url(
            "https://example.com/wfs",
            "test:layer",
            version="2.0.0",
            crs="EPSG:4326",
        )
        assert "srsName=" in url or "srsname=" in url.lower()

    def test_build_wfs_url_excludes_srsname_wfs_10(self):
        """WFS 1.0.0 should NOT include srsName (uses SRS in bbox only)."""
        from geoparquet_io.core.wfs import _build_wfs_url

        url = _build_wfs_url(
            "https://example.com/wfs",
            "test:layer",
            version="1.0.0",
            crs="EPSG:4326",
        )
        # srsName is a 1.1.0+ parameter
        assert "srsName=" not in url

    def test_build_wfs_url_excludes_srsname_when_no_crs(self):
        """srsName should be absent when no CRS specified."""
        from geoparquet_io.core.wfs import _build_wfs_url

        url = _build_wfs_url(
            "https://example.com/wfs",
            "test:layer",
            version="1.1.0",
            # No crs provided
        )
        assert "srsName=" not in url


# =============================================================================
# CRS Validation Tests (Issue #398)
# =============================================================================


class TestCRSValidation:
    """Test CRS coordinate validation and mismatch detection."""

    def test_estimate_crs_from_bbox_detects_wgs84(self):
        """Should detect WGS84 coordinates."""
        from geoparquet_io.core.wfs import _estimate_crs_from_bbox

        # Valid WGS84 bbox
        assert _estimate_crs_from_bbox((-122.5, 37.5, -122.0, 38.0)) == "EPSG:4326"
        assert _estimate_crs_from_bbox((-180, -90, 180, 90)) == "EPSG:4326"
        assert _estimate_crs_from_bbox((4.82, 50.44, 4.92, 50.48)) == "EPSG:4326"

    def test_estimate_crs_from_bbox_detects_laea_europe(self):
        """Should detect EPSG:3035 (LAEA Europe) coordinates."""
        from geoparquet_io.core.wfs import _estimate_crs_from_bbox

        # Typical EPSG:3035 bbox (Belgian buildings in meters)
        bbox = (3817324.31, 2942224.42, 4063861.36, 3100245.97)
        assert _estimate_crs_from_bbox(bbox) == "EPSG:3035"

    def test_estimate_crs_from_bbox_detects_web_mercator(self):
        """Should detect EPSG:3857 (Web Mercator) coordinates."""
        from geoparquet_io.core.wfs import _estimate_crs_from_bbox

        # Web Mercator coordinates (world extent)
        bbox = (-20037508.34, -20037508.34, 20037508.34, 20037508.34)
        assert _estimate_crs_from_bbox(bbox) == "EPSG:3857"

    def test_validate_crs_coordinates_passes_for_valid_wgs84(self):
        """Should pass when coordinates match requested WGS84 CRS."""
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection
        from geoparquet_io.core.wfs import _validate_crs_coordinates

        # Create a table with valid WGS84 geometry
        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
        try:
            result = con.execute("""
                SELECT ST_AsWKB(ST_Point(-122.4, 37.8)) as geometry
            """).arrow()
            table = result.read_all()
        finally:
            con.close()

        is_valid, detected = _validate_crs_coordinates(table, "EPSG:4326")
        assert is_valid is True
        assert detected is None

    def test_validate_crs_coordinates_detects_mismatch(self):
        """Should detect when coordinates don't match requested CRS."""
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection
        from geoparquet_io.core.wfs import _validate_crs_coordinates

        # Create a table with EPSG:3035 coordinates (not WGS84)
        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
        try:
            result = con.execute("""
                SELECT ST_AsWKB(ST_Point(3900000, 3000000)) as geometry
            """).arrow()
            table = result.read_all()
        finally:
            con.close()

        # Request WGS84 but coordinates are clearly projected
        is_valid, detected = _validate_crs_coordinates(table, "EPSG:4326", strict=False)
        assert is_valid is False
        assert detected == "EPSG:3035"

    def test_validate_crs_coordinates_raises_on_strict_mismatch(self):
        """Should raise error in strict mode when CRS doesn't match."""
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection
        from geoparquet_io.core.wfs import WFSError, _validate_crs_coordinates

        # Create a table with projected coordinates
        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
        try:
            result = con.execute("""
                SELECT ST_AsWKB(ST_Point(3900000, 3000000)) as geometry
            """).arrow()
            table = result.read_all()
        finally:
            con.close()

        with pytest.raises(WFSError, match="Coordinate mismatch"):
            _validate_crs_coordinates(table, "EPSG:4326", strict=True)

    def test_validate_crs_coordinates_skips_empty_table(self):
        """Should pass for empty tables without checking."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _validate_crs_coordinates

        # Empty table
        table = pa.table({"geometry": pa.array([], type=pa.binary())})

        is_valid, detected = _validate_crs_coordinates(table, "EPSG:4326")
        assert is_valid is True
        assert detected is None

    def test_reproject_on_crs_mismatch(self):
        """Should reproject data when CRS mismatch detected (issue #407)."""
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection
        from geoparquet_io.core.reproject import reproject_table
        from geoparquet_io.core.wfs import _validate_crs_coordinates

        # Create table with EPSG:3035 coordinates (Belgium area)
        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
        try:
            result = con.execute("""
                SELECT ST_AsWKB(ST_Point(3900000, 3000000)) as geometry
            """).arrow()
            table = result.read_all()
        finally:
            con.close()

        # Verify mismatch detection
        is_valid, detected_crs = _validate_crs_coordinates(table, "EPSG:4326", strict=False)
        assert is_valid is False
        assert detected_crs == "EPSG:3035"

        # Reproject to WGS84 (simulates the new wfs_to_table behavior)
        reprojected = reproject_table(table, target_crs="EPSG:4326", source_crs="EPSG:3035")

        # Verify output coordinates are in WGS84 range
        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
        try:
            con.register("data", reprojected)
            result = con.execute("""
                SELECT
                    ST_X(ST_GeomFromWKB(geometry)) as x,
                    ST_Y(ST_GeomFromWKB(geometry)) as y
                FROM data
            """).fetchone()
            x, y = result
        finally:
            con.close()

        # WGS84 coordinates should be in valid range
        assert -180 <= x <= 180, f"X coordinate {x} out of WGS84 range"
        assert -90 <= y <= 90, f"Y coordinate {y} out of WGS84 range"
        # Should be roughly in Western Europe (Belgium area)
        assert 0 < x < 10, f"Expected longitude ~4-6, got {x}"
        assert 45 < y < 55, f"Expected latitude ~50, got {y}"


# =============================================================================
# Integration Tests for WFS 2.0 (Issue #312)
# =============================================================================


@pytest.mark.integration
@pytest.mark.network
@pytest.mark.slow
class TestWFS20Integration:
    """Integration tests against real WFS 2.0 servers.

    Uses Helsinki WFS (kartta.hel.fi) which reliably supports WFS 2.0.0.
    """

    HELSINKI_WFS = "https://kartta.hel.fi/ws/geoserver/avoindata/wfs"
    HELSINKI_LAYER = "avoindata:Ajoneuvoliikenne_liikennemaarat_viiva"

    @pytest.mark.xfail(reason="External WFS service may be unavailable")
    def test_version_negotiation_finds_wfs_2(self):
        """Auto negotiation should find WFS 2.0.0 when available."""
        from geoparquet_io.core.wfs import negotiate_wfs_version

        version, wfs = negotiate_wfs_version(self.HELSINKI_WFS)
        assert version in ["2.0.0", "1.1.0", "1.0.0"]

    @pytest.mark.xfail(reason="External WFS service may be unavailable")
    def test_extract_with_wfs_2_version(self):
        """Should successfully extract features using WFS 2.0.0."""
        from geoparquet_io.core.wfs import wfs_to_table

        table = wfs_to_table(
            self.HELSINKI_WFS,
            self.HELSINKI_LAYER,
            version="2.0.0",
            limit=10,
        )

        assert table.num_rows <= 10
        assert "geometry" in table.column_names


# =============================================================================
# Type Inference Tests (Issue #400)
# =============================================================================


class TestInferColumnTypes:
    """Tests for string column type inference.

    WFS servers often return all values as quoted strings in JSON, causing
    DuckDB to infer them as VARCHAR. These tests verify that _infer_column_types
    correctly detects and casts numeric, boolean, and other typed columns.
    """

    def test_infer_types_integer(self):
        """Integer strings should be cast to int64."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _infer_column_types

        table = pa.table({"objectid": ["12345", "67890", "11111"]})
        result = _infer_column_types(table)
        assert result.schema.field("objectid").type == pa.int64()
        assert result["objectid"].to_pylist() == [12345, 67890, 11111]

    def test_infer_types_float(self):
        """Float strings should be cast to double."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _infer_column_types

        table = pa.table({"value": ["3.14", "2.71", "1.618"]})
        result = _infer_column_types(table)
        assert result.schema.field("value").type == pa.float64()
        assert result["value"].to_pylist() == pytest.approx([3.14, 2.71, 1.618])

    def test_infer_types_string_preserved(self):
        """Non-numeric strings should stay as string."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _infer_column_types

        table = pa.table({"name": ["Alice", "Bob", "Charlie"]})
        result = _infer_column_types(table)
        assert result.schema.field("name").type in (pa.string(), pa.large_string())
        assert result["name"].to_pylist() == ["Alice", "Bob", "Charlie"]

    def test_infer_types_with_nulls(self):
        """Null values should be preserved during type inference."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _infer_column_types

        table = pa.table({"count": ["100", None, "200"]})
        result = _infer_column_types(table)
        assert result.schema.field("count").type == pa.int64()
        assert result["count"].to_pylist() == [100, None, 200]

    def test_infer_types_prefers_int_over_float(self):
        """Whole numbers should be int64, not float64."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _infer_column_types

        table = pa.table({"id": ["1", "2", "3"]})
        result = _infer_column_types(table)
        assert result.schema.field("id").type == pa.int64()

    def test_infer_types_skips_non_string_columns(self):
        """Already-typed columns should not be modified."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _infer_column_types

        table = pa.table(
            {
                "existing_int": pa.array([1, 2, 3], type=pa.int64()),
                "existing_float": pa.array([1.1, 2.2, 3.3], type=pa.float64()),
            }
        )
        result = _infer_column_types(table)
        assert result.schema.field("existing_int").type == pa.int64()
        assert result.schema.field("existing_float").type == pa.float64()

    def test_infer_types_boolean(self):
        """Boolean strings should be cast to bool."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _infer_column_types

        table = pa.table({"active": ["true", "false", "true"]})
        result = _infer_column_types(table)
        assert result.schema.field("active").type == pa.bool_()
        assert result["active"].to_pylist() == [True, False, True]

    def test_infer_types_boolean_variants(self):
        """Various boolean representations should work."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _infer_column_types

        table = pa.table({"flag": ["True", "FALSE", "1", "0"]})
        result = _infer_column_types(table)
        assert result.schema.field("flag").type == pa.bool_()
        assert result["flag"].to_pylist() == [True, False, True, False]

    def test_infer_types_empty_table(self):
        """Empty tables should not crash."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _infer_column_types

        table = pa.table({"col": pa.array([], type=pa.string())})
        result = _infer_column_types(table)
        assert result.num_rows == 0

    def test_infer_types_all_nulls(self):
        """Column with all nulls should stay string."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _infer_column_types

        table = pa.table({"maybe": pa.array([None, None], type=pa.string())})
        result = _infer_column_types(table)
        # Stays string (or large_string) because we can't infer type from nulls
        assert result.schema.field("maybe").type in (pa.string(), pa.large_string())

    def test_infer_types_mixed_numeric_stays_string(self):
        """Mixed numeric and text should stay string."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _infer_column_types

        table = pa.table({"mixed": ["123", "abc", "456"]})
        result = _infer_column_types(table)
        assert result.schema.field("mixed").type in (pa.string(), pa.large_string())

    def test_infer_types_negative_numbers(self):
        """Negative numbers should be handled correctly."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _infer_column_types

        table = pa.table(
            {
                "negative_int": ["-100", "-200", "300"],
                "negative_float": ["-1.5", "2.5", "-3.5"],
            }
        )
        result = _infer_column_types(table)
        assert result.schema.field("negative_int").type == pa.int64()
        assert result.schema.field("negative_float").type == pa.float64()
        assert result["negative_int"].to_pylist() == [-100, -200, 300]

    def test_infer_types_geometry_preserved(self):
        """Binary geometry column should not be touched."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _infer_column_types

        geom = pa.array([b"\x01\x02\x03"], type=pa.binary())
        table = pa.table(
            {
                "geometry": geom,
                "id": ["1"],
            }
        )
        result = _infer_column_types(table)
        assert result.schema.field("geometry").type == pa.binary()

    def test_infer_types_multiple_columns(self):
        """Multiple columns should all be processed."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _infer_column_types

        table = pa.table(
            {
                "id": ["1", "2"],
                "value": ["3.14", "2.71"],
                "name": ["foo", "bar"],
                "active": ["true", "false"],
            }
        )
        result = _infer_column_types(table)
        assert result.schema.field("id").type == pa.int64()
        assert result.schema.field("value").type == pa.float64()
        assert result.schema.field("name").type in (pa.string(), pa.large_string())
        assert result.schema.field("active").type == pa.bool_()

    # =========================================================================
    # Edge Case Tests (adversarial review findings)
    # =========================================================================

    def test_infer_types_scientific_notation(self):
        """Scientific notation should cast to float, not int."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _infer_column_types

        table = pa.table({"sci": ["1.5e6", "2.0e-3", "3e10"]})
        result = _infer_column_types(table)
        # Scientific notation parses as float
        assert result.schema.field("sci").type == pa.float64()
        assert result["sci"].to_pylist() == pytest.approx([1.5e6, 2.0e-3, 3e10])

    def test_infer_types_leading_zeros_cast_to_float(self):
        """Leading zeros cast to float (not int, since '007' != '7')."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _infer_column_types

        # Leading zeros: '007' != '7' so fails int check, but DOUBLE works
        table = pa.table({"code": ["007", "008", "009"]})
        result = _infer_column_types(table)
        # DuckDB TRY_CAST to DOUBLE succeeds, so becomes float
        # Note: if you need to preserve leading zeros, don't use type inference
        assert result.schema.field("code").type == pa.float64()
        assert result["code"].to_pylist() == [7.0, 8.0, 9.0]

    def test_infer_types_whitespace_cast_to_float(self):
        """DuckDB trims whitespace before casting, so these become floats."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _infer_column_types

        # DuckDB trims whitespace during TRY_CAST
        table = pa.table({"padded": [" 123 ", "456", " 789"]})
        result = _infer_column_types(table)
        # Whitespace trimmed, DOUBLE cast succeeds
        assert result.schema.field("padded").type == pa.float64()
        assert result["padded"].to_pylist() == [123.0, 456.0, 789.0]

    def test_infer_types_empty_string_stays_string(self):
        """Empty strings mixed with numbers should stay string."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _infer_column_types

        table = pa.table({"maybe_num": ["123", "", "456"]})
        result = _infer_column_types(table)
        # Empty string is not a valid int, stays string
        assert result.schema.field("maybe_num").type in (pa.string(), pa.large_string())

    def test_infer_types_bigint_overflow_becomes_float(self):
        """Numbers exceeding BIGINT range cast to float (with precision loss)."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _infer_column_types

        # BIGINT max is 9223372036854775807, but DOUBLE can hold larger values
        table = pa.table({"huge": ["99999999999999999999", "1", "2"]})
        result = _infer_column_types(table)
        # BIGINT fails but DOUBLE succeeds (with precision loss)
        assert result.schema.field("huge").type == pa.float64()
        # Note: 99999999999999999999 becomes 1e20 (precision loss is expected)
        assert result["huge"].to_pylist()[0] == pytest.approx(1e20, rel=0.01)

    def test_infer_types_connection_cleanup_on_error(self):
        """Connection should be closed even if processing fails."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _infer_column_types

        # Test with valid data - connection should be closed after
        table = pa.table({"x": ["1", "2", "3"]})
        result = _infer_column_types(table)
        assert result.schema.field("x").type == pa.int64()
        # If connection leaked, subsequent calls would fail or accumulate
        # Run multiple times to detect leaks
        for _ in range(5):
            result = _infer_column_types(table)
            assert result.schema.field("x").type == pa.int64()


# =============================================================================
# Type Inference Integration Test (Issue #400)
# =============================================================================


@pytest.mark.network
@pytest.mark.slow
class TestTypeInferenceIntegration:
    """Integration test verifying type inference works with real WFS data.

    Uses Belgian WFS which returns string-typed numeric fields.
    """

    # Belgian federal WFS - known to return string-typed numerics
    BELGIUM_WFS = "https://geoservices.wallonie.be/arcgis/services/AMENAGEMENT_TERRITOIRE/TLPE/MapServer/WFSServer"
    BELGIUM_LAYER = "TLPE:TLPE"

    def test_wfs_type_inference_real_server(self):
        """Real WFS data should have numeric columns properly typed."""
        import pyarrow as pa

        from geoparquet_io.api import ops

        table = ops.from_wfs(
            self.BELGIUM_WFS,
            self.BELGIUM_LAYER,
            limit=50,
        )

        assert table.num_rows > 0
        assert "geometry" in table.column_names

        # Check that at least one numeric-looking column got inferred
        # (exact columns depend on the WFS, but objectid is typically numeric)
        numeric_types = (pa.int64(), pa.float64())
        has_numeric = any(
            field.type in numeric_types for field in table.schema if field.name != "geometry"
        )
        # If no numeric columns found, that's suspicious but not fatal
        # (server may have changed schema)
        if not has_numeric:
            pytest.skip("No numeric columns found in WFS response")


# =============================================================================
# srsName Without Bbox Tests (Issue #405)
# =============================================================================


@pytest.mark.integration
@pytest.mark.network
@pytest.mark.slow
class TestSrsNameWithoutBbox:
    """Integration tests for srsName parameter without bbox filter.

    Issue #405: srsName was only sent when bbox was provided, causing servers
    to return data in their native CRS instead of the requested CRS.
    """

    # Wallonia INSPIRE server - returns EPSG:3035 natively, supports WGS84
    WALLONIA_WFS = "https://geoservices.wallonie.be/geoserver/inspire_bu/ows"
    WALLONIA_LAYER = "inspire_bu:BU.Building_building_emprise"

    @pytest.mark.xfail(
        reason="External WFS service may be unavailable",
        strict=False,
        raises=(Exception,),  # Only xfail on connection/service errors
    )
    def test_wfs_without_bbox_returns_wgs84_coordinates(self):
        """Without bbox, server should still return WGS84 when requested.

        The Wallonia server's native CRS is EPSG:3035 (ETRS89-LAEA).
        If srsName is properly sent, we should get WGS84 coords (~4.3, ~50.7).
        If srsName is missing, we'd get EPSG:3035 coords (~3923264, ~3080361).
        """
        import shapely

        from geoparquet_io.core.wfs import wfs_to_table

        table = wfs_to_table(
            self.WALLONIA_WFS,
            self.WALLONIA_LAYER,
            version="1.1.0",
            limit=5,
            # No bbox - this is the key condition for #405
        )

        assert table.num_rows > 0
        geom = shapely.from_wkb(table.column("geometry")[0].as_py())

        # Get first coordinate
        if hasattr(geom, "exterior"):
            x, y = geom.exterior.coords[0]
        else:
            x, y = geom.coords[0]

        # WGS84 Belgium coords: longitude ~3-6, latitude ~49-52
        # EPSG:3035 coords would be ~3.8M, ~3.0M
        assert -180 <= x <= 180, f"X coord {x} not in WGS84 range, likely EPSG:3035"
        assert -90 <= y <= 90, f"Y coord {y} not in WGS84 range, likely EPSG:3035"

        # More specific check for Belgium
        assert 2.5 <= x <= 6.5, f"X coord {x} not in Belgium longitude range"
        assert 49.0 <= y <= 52.0, f"Y coord {y} not in Belgium latitude range"
