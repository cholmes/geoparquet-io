"""
Tests for WFS (Web Feature Service) extraction.

Tests use mocked HTTP responses to avoid network dependencies.
Network tests are marked separately for optional integration testing.
"""

import os
from unittest.mock import MagicMock, patch

import pytest

# Module-level imports for WFS functions (avoids per-test imports)
from geoparquet_io.core import wfs as wfs_module
from geoparquet_io.core.wfs import (
    EmptyLayerError,
    LayerNotFoundError,
    WFSAuthenticationError,
    WFSError,
    WFSLayerInfo,
    _build_bbox_param,
    _build_local_bbox_filter,
    _build_wfs_url,
    _detect_best_output_format,
    _detect_sortable_attribute,
    _determine_bbox_strategy,
    _negotiate_crs,
    _normalize_crs,
    _validate_identifier,
    get_layer_info,
    get_wfs_capabilities,
    list_available_layers,
)


@pytest.fixture
def offline_wfs_probes():
    """Complete the mock for ``wfs_to_table``: block every real HTTP request.

    Mocking ``negotiate_wfs_version`` / ``get_layer_info`` /
    ``fetch_all_features_duckdb`` is *not* enough to take ``wfs_to_table``
    offline. Before it reaches the mocked fetch, its auto-tiling logic calls
    ``_get_feature_count`` twice (once for WFS 2.0.0, then for the requested
    version), and those calls are unmocked. Against a fake host they resolve to
    real DNS lookups that fail, and ``_get_feature_count`` swallows every
    exception -- so the tests still passed while quietly burning the 1s+2s
    retry backoff twice, ~6s per test.

    Stubbing the probes to ``None`` reproduces exactly what the failed requests
    already returned, so no assertion changes meaning. ``_make_request`` is
    additionally replaced with a tripwire, and the recorded calls are asserted
    on teardown so a *future* unmocked network path fails loudly instead of
    silently sleeping.
    """
    escaped_requests: list[str] = []

    def _blocked_request(url, *args, **kwargs):
        escaped_requests.append(url)
        raise AssertionError(f"unmocked WFS HTTP request to {url}")

    with (
        patch.object(wfs_module, "_get_feature_count", return_value=None),
        patch.object(wfs_module, "_probe_startindex_limit", return_value=None),
        patch.object(wfs_module, "_make_request", side_effect=_blocked_request),
    ):
        yield

    assert not escaped_requests, (
        f"wfs_to_table made unmocked HTTP request(s): {escaped_requests}. "
        "Extend the offline_wfs_probes fixture to cover the new call."
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


class TestWFSExceptionHierarchy:
    """Test typed WFS exception subclasses for downstream consumers."""

    def test_empty_layer_error_is_wfs_error(self):
        """EmptyLayerError should be catchable as WFSError (backward compatible)."""
        assert issubclass(EmptyLayerError, WFSError)

    def test_layer_not_found_error_is_wfs_error(self):
        """LayerNotFoundError should be catchable as WFSError (backward compatible)."""
        assert issubclass(LayerNotFoundError, WFSError)

    def test_auth_error_is_wfs_error(self):
        """WFSAuthenticationError should be catchable as WFSError (backward compatible)."""
        assert issubclass(WFSAuthenticationError, WFSError)

    def test_empty_layer_error_has_typename(self):
        """EmptyLayerError should expose typename attribute."""
        err = EmptyLayerError("ns:layername")
        assert err.typename == "ns:layername"
        assert "ns:layername" in str(err)
        assert "No features returned" in str(err)

    def test_layer_not_found_error_has_typename_and_available(self):
        """LayerNotFoundError should expose typename and available layers."""
        err = LayerNotFoundError("ns:missing", ["ns:layer1", "ns:layer2"])
        assert err.typename == "ns:missing"
        assert err.available == ["ns:layer1", "ns:layer2"]
        assert "ns:missing" in str(err)
        assert "not found" in str(err)

    def test_layer_not_found_error_with_no_available(self):
        """LayerNotFoundError should work without available layers."""
        err = LayerNotFoundError("ns:missing")
        assert err.typename == "ns:missing"
        assert err.available == []
        assert "ns:missing" in str(err)

    def test_auth_error_has_url_and_status_code(self):
        """WFSAuthenticationError should expose url and status_code."""
        err = WFSAuthenticationError("http://example.com/wfs", 401, "Auth required")
        assert err.url == "http://example.com/wfs"
        assert err.status_code == 401
        assert "Auth required" in str(err)

    def test_empty_layer_error_caught_as_wfs_error(self):
        """Verify backward compatibility - catching as WFSError works."""
        try:
            raise EmptyLayerError("test:layer")
        except WFSError as e:
            assert isinstance(e, EmptyLayerError)
            assert e.typename == "test:layer"

    @patch("owslib.wfs.WebFeatureService")
    def test_layer_not_found_raises_typed_error(self, mock_wfs_class):
        """LayerNotFoundError is raised when layer doesn't exist."""
        mock_wfs = MagicMock()
        mock_wfs.contents = {"ns:cities": MagicMock(), "ns:roads": MagicMock()}
        mock_wfs_class.return_value = mock_wfs

        with pytest.raises(LayerNotFoundError) as exc_info:
            get_layer_info("http://mock.wfs.server/wfs", "nonexistent:data")

        assert exc_info.value.typename == "nonexistent:data"
        assert "ns:cities" in exc_info.value.available
        assert "ns:roads" in exc_info.value.available


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
        """WFS 1.1.0 bbox always uses EPSG:4326 regardless of output CRS.

        Many WFS servers (especially GeoServer) only accept bbox in WGS84,
        so we always send bbox with EPSG:4326 suffix regardless of output CRS.
        """
        bbox = (-122.5, 37.5, -122.0, 38.0)
        crs = "urn:ogc:def:crs:EPSG::4326"  # Output CRS (ignored for bbox)
        result = _build_bbox_param(bbox, crs, "1.1.0")
        # Bbox should always end with EPSG:4326, not the output CRS
        assert result.endswith("EPSG:4326")
        assert result == "-122.5,37.5,-122.0,38.0,EPSG:4326"

    def test_invalid_geometry_column_rejected(self):
        """Invalid geometry column names with SQL injection characters are rejected."""
        bbox = (-122.5, 37.5, -122.0, 38.0)
        with pytest.raises(WFSError, match="Invalid geometry column name"):
            _build_local_bbox_filter(bbox, 'geom"; DROP TABLE --')


# =============================================================================
# Unit Tests - Sort By Parameter (Issue #488)
# =============================================================================


class TestSortByParameter:
    """Test sortBy parameter for stable pagination on PK-less layers.

    GeoServer requires sortBy for pagination on layers without a primary key.
    """

    def test_build_wfs_url_includes_sortby_when_provided(self):
        """sortBy parameter is included in URL when specified."""
        url = _build_wfs_url(
            "http://example.com/wfs",
            "layer",
            version="2.0.0",
            max_features=1000,
            start_index=10000,
            sort_by="gid",
        )
        assert "sortBy=gid" in url

    def test_build_wfs_url_no_sortby_when_not_provided(self):
        """sortBy parameter is omitted when not specified."""
        url = _build_wfs_url(
            "http://example.com/wfs",
            "layer",
            version="2.0.0",
            max_features=1000,
            start_index=10000,
        )
        assert "sortBy" not in url

    def test_build_wfs_url_no_sortby_for_wfs_1_0(self):
        """sortBy is not included for WFS 1.0.0 (not supported)."""
        url = _build_wfs_url(
            "http://example.com/wfs",
            "layer",
            version="1.0.0",
            max_features=1000,
            sort_by="gid",
        )
        assert "sortBy" not in url

    def test_detect_sortable_attribute_picks_first_non_geometry(self):
        """_detect_sortable_attribute returns first non-geometry attribute."""
        mock_wfs = MagicMock()
        mock_wfs.get_schema.return_value = {
            "geometry_column": "the_geom",
            "properties": {
                "gid": "int",
                "name": "string",
                "the_geom": "geometry",
            },
        }
        result = _detect_sortable_attribute(mock_wfs, "test:layer")
        assert result == "gid"

    def test_detect_sortable_attribute_skips_geometry_types(self):
        """_detect_sortable_attribute skips columns with geometry types."""
        mock_wfs = MagicMock()
        mock_wfs.get_schema.return_value = {
            "geometry_column": "geom",
            "properties": {
                "geom": "gml:PointPropertyType",
                "boundary": "gml:MultiPolygonPropertyType",
                "id": "int",
            },
        }
        result = _detect_sortable_attribute(mock_wfs, "test:layer")
        assert result == "id"

    def test_detect_sortable_attribute_returns_none_when_no_properties(self):
        """_detect_sortable_attribute returns None if no properties."""
        mock_wfs = MagicMock()
        mock_wfs.get_schema.return_value = {"geometry_column": "geom"}
        result = _detect_sortable_attribute(mock_wfs, "test:layer")
        assert result is None

    def test_detect_sortable_attribute_handles_exception(self):
        """_detect_sortable_attribute returns None on schema fetch error."""
        mock_wfs = MagicMock()
        mock_wfs.get_schema.side_effect = Exception("Schema unavailable")
        result = _detect_sortable_attribute(mock_wfs, "test:layer")
        assert result is None


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

    @pytest.mark.network
    @pytest.mark.xfail(reason="requires external WFS endpoint, flaky in CI", strict=False)
    def test_fetch_wfs_page_returns_arrow_table(self):
        """DuckDB-native fetch should return an Arrow table with geometry."""
        from geoparquet_io.core.wfs import _fetch_wfs_page

        table = _fetch_wfs_page(self.WFS_URL)

        # Should return Arrow table
        import pyarrow as pa

        assert isinstance(table, pa.Table)
        # Should have geometry column
        assert "geometry" in table.column_names
        # Should have some rows
        assert table.num_rows > 0
        assert table.num_rows <= 10

    @pytest.mark.network
    @pytest.mark.xfail(reason="requires external WFS endpoint, flaky in CI", strict=False)
    def test_fetch_wfs_page_geometry_is_wkb(self):
        """Geometry should be WKB binary format."""
        from geoparquet_io.core.wfs import _fetch_wfs_page

        table = _fetch_wfs_page(self.WFS_URL)

        # Geometry should be binary (WKB)
        import pyarrow as pa

        geom_type = table.schema.field("geometry").type
        assert pa.types.is_binary(geom_type) or pa.types.is_large_binary(geom_type)


class TestContentTypeValidation:
    """Test content-type validation catches error pages returned with 200 OK."""

    def test_rejects_html_content_type(self):
        """HTML responses should be rejected with clear error."""
        import httpx

        from geoparquet_io.core.wfs import WFSError, _fetch_wfs_page

        html_response = b"<html><body>Error: Service unavailable</body></html>"

        def mock_stream(*args, **kwargs):
            class MockResponse:
                status_code = 200
                headers = {"content-type": "text/html; charset=utf-8"}

                def raise_for_status(self):
                    pass

                def read(self):
                    return html_response

                def __enter__(self):
                    return self

                def __exit__(self, *args):
                    pass

            return MockResponse()

        with patch.object(httpx.Client, "stream", mock_stream):
            with pytest.raises(WFSError, match="Expected JSON.*text/html"):
                _fetch_wfs_page("http://mock.wfs/wfs?service=WFS")

    def test_rejects_xml_content_type(self):
        """XML error responses should be rejected."""
        import httpx

        from geoparquet_io.core.wfs import WFSError, _fetch_wfs_page

        xml_response = b"<ows:ExceptionReport>Service error</ows:ExceptionReport>"

        def mock_stream(*args, **kwargs):
            class MockResponse:
                status_code = 200
                headers = {"content-type": "application/xml"}

                def raise_for_status(self):
                    pass

                def read(self):
                    return xml_response

                def __enter__(self):
                    return self

                def __exit__(self, *args):
                    pass

            return MockResponse()

        with patch.object(httpx.Client, "stream", mock_stream):
            with pytest.raises(WFSError, match="Expected JSON.*application/xml"):
                _fetch_wfs_page("http://mock.wfs/wfs?service=WFS")

    def test_rejects_text_plain_content_type(self):
        """text/plain error responses should be rejected."""
        import httpx

        from geoparquet_io.core.wfs import WFSError, _fetch_wfs_page

        text_response = b"Error: Invalid request parameters"

        def mock_stream(*args, **kwargs):
            class MockResponse:
                status_code = 200
                headers = {"content-type": "text/plain"}

                def raise_for_status(self):
                    pass

                def read(self):
                    return text_response

                def __enter__(self):
                    return self

                def __exit__(self, *args):
                    pass

            return MockResponse()

        with patch.object(httpx.Client, "stream", mock_stream):
            with pytest.raises(WFSError, match="Expected JSON.*text/plain"):
                _fetch_wfs_page("http://mock.wfs/wfs?service=WFS")

    def test_accepts_application_json(self):
        """application/json should be accepted."""
        import httpx

        from geoparquet_io.core.wfs import _fetch_wfs_page

        json_response = b'{"type": "FeatureCollection", "features": []}'

        def mock_stream(*args, **kwargs):
            class MockResponse:
                status_code = 200
                headers = {"content-type": "application/json"}

                def raise_for_status(self):
                    pass

                def iter_bytes(self, chunk_size=None):
                    yield json_response

                def __enter__(self):
                    return self

                def __exit__(self, *args):
                    pass

            return MockResponse()

        with patch.object(httpx.Client, "stream", mock_stream):
            # Should not raise, returns empty table
            result = _fetch_wfs_page("http://mock.wfs/wfs?service=WFS")
            assert result.num_rows == 0

    def test_accepts_text_javascript(self):
        """text/javascript (used by some servers) should be accepted."""
        import httpx

        from geoparquet_io.core.wfs import _fetch_wfs_page

        json_response = b'{"type": "FeatureCollection", "features": []}'

        def mock_stream(*args, **kwargs):
            class MockResponse:
                status_code = 200
                headers = {"content-type": "text/javascript"}

                def raise_for_status(self):
                    pass

                def iter_bytes(self, chunk_size=None):
                    yield json_response

                def __enter__(self):
                    return self

                def __exit__(self, *args):
                    pass

            return MockResponse()

        with patch.object(httpx.Client, "stream", mock_stream):
            result = _fetch_wfs_page("http://mock.wfs/wfs?service=WFS")
            assert result.num_rows == 0


class TestEmptyProperties:
    """Test handling of empty/null properties in GeoJSON features.

    When WFS features have empty properties ({}) or null properties,
    DuckDB infers MAP(VARCHAR, JSON) or JSON types instead of STRUCT.
    The unnest() function doesn't support these types, so we fall back
    to returning geometry-only tables.

    See: https://github.com/geoparquet/geoparquet-io/issues/441
    """

    def _make_mock_stream(self, geojson_bytes: bytes):
        """Create a mock httpx stream response."""
        import httpx

        def mock_stream(*args, **kwargs):
            class MockResponse:
                status_code = 200
                headers = {"content-type": "application/json"}

                def raise_for_status(self):
                    pass

                def iter_bytes(self, chunk_size=None):
                    yield geojson_bytes

                def __enter__(self):
                    return self

                def __exit__(self, *args):
                    pass

            return MockResponse()

        return patch.object(httpx.Client, "stream", mock_stream)

    def test_empty_properties_returns_geometry_only(self):
        """Empty properties ({}) should return geometry-only table."""
        import json

        import pyarrow as pa

        from geoparquet_io.core.wfs import _fetch_wfs_page

        geojson = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [0, 0]},
                    "properties": {},
                },
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [1, 1]},
                    "properties": {},
                },
            ],
        }

        with self._make_mock_stream(json.dumps(geojson).encode()):
            result = _fetch_wfs_page("http://mock.wfs/wfs?service=WFS")

        assert isinstance(result, pa.Table)
        assert result.num_rows == 2
        assert "geometry" in result.column_names
        # No property columns - only geometry
        assert result.column_names == ["geometry"]

    def test_null_properties_returns_geometry_only(self):
        """Null properties should return geometry-only table."""
        import json

        import pyarrow as pa

        from geoparquet_io.core.wfs import _fetch_wfs_page

        geojson = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [0, 0]},
                    "properties": None,
                },
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [1, 1]},
                    "properties": None,
                },
            ],
        }

        with self._make_mock_stream(json.dumps(geojson).encode()):
            result = _fetch_wfs_page("http://mock.wfs/wfs?service=WFS")

        assert isinstance(result, pa.Table)
        assert result.num_rows == 2
        assert result.column_names == ["geometry"]

    def test_empty_properties_with_extract_fid(self):
        """Empty properties with extract_fid=True should include _wfs_fid."""
        import json

        import pyarrow as pa

        from geoparquet_io.core.wfs import _fetch_wfs_page

        geojson = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "id": "line.1",
                    "geometry": {"type": "Point", "coordinates": [0, 0]},
                    "properties": {},
                },
                {
                    "type": "Feature",
                    "id": "line.2",
                    "geometry": {"type": "Point", "coordinates": [1, 1]},
                    "properties": {},
                },
            ],
        }

        with self._make_mock_stream(json.dumps(geojson).encode()):
            result = _fetch_wfs_page("http://mock.wfs/wfs?service=WFS", extract_fid=True)

        assert isinstance(result, pa.Table)
        assert result.num_rows == 2
        assert "_wfs_fid" in result.column_names
        assert "geometry" in result.column_names
        assert result.column_names == ["_wfs_fid", "geometry"]

    def test_normal_properties_still_works(self):
        """Normal properties should still be unnested into columns."""
        import json

        import pyarrow as pa

        from geoparquet_io.core.wfs import _fetch_wfs_page

        geojson = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [0, 0]},
                    "properties": {"name": "A", "population": 100},
                },
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [1, 1]},
                    "properties": {"name": "B", "population": 200},
                },
            ],
        }

        with self._make_mock_stream(json.dumps(geojson).encode()):
            result = _fetch_wfs_page("http://mock.wfs/wfs?service=WFS")

        assert isinstance(result, pa.Table)
        assert result.num_rows == 2
        assert "geometry" in result.column_names
        assert "name" in result.column_names
        assert "population" in result.column_names

    def test_heterogeneous_value_types_still_works(self):
        """Heterogeneous value types (promoted to JSON) should still work."""
        import json

        import pyarrow as pa

        from geoparquet_io.core.wfs import _fetch_wfs_page

        geojson = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [0, 0]},
                    "properties": {"value": 123},
                },
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [1, 1]},
                    "properties": {"value": "text"},
                },
            ],
        }

        with self._make_mock_stream(json.dumps(geojson).encode()):
            result = _fetch_wfs_page("http://mock.wfs/wfs?service=WFS")

        assert isinstance(result, pa.Table)
        assert result.num_rows == 2
        assert "geometry" in result.column_names
        assert "value" in result.column_names

    def test_missing_properties_key_returns_geometry_only(self):
        """Missing properties key (malformed GeoJSON) should return geometry-only table.

        Per RFC 7946, the 'properties' member is required, but we handle
        malformed GeoJSON gracefully by returning geometry-only results.
        """
        import json

        import pyarrow as pa

        from geoparquet_io.core.wfs import _fetch_wfs_page

        geojson = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [0, 0]},
                    # No 'properties' key - malformed per RFC 7946
                },
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [1, 1]},
                },
            ],
        }

        with self._make_mock_stream(json.dumps(geojson).encode()):
            result = _fetch_wfs_page("http://mock.wfs/wfs?service=WFS")

        assert isinstance(result, pa.Table)
        assert result.num_rows == 2
        assert result.column_names == ["geometry"]


class TestAutoPageSingleWorker:
    """Test that single-worker mode auto-paginates for large datasets."""

    def test_single_worker_paginates_when_count_exceeds_page_size(self):
        """With max_workers=1 and total > page_size, should paginate sequentially."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import fetch_all_features_duckdb

        # Each page must have page_size rows so adaptive pagination doesn't
        # mistake 1-row results for a server-side maxFeatures cap.
        page1 = pa.table(
            {
                "geometry": pa.array([b"\x01\x02"] * 10000, type=pa.binary()),
                "name": pa.array(["a"] * 10000),
            }
        )
        page2 = pa.table(
            {
                "geometry": pa.array([b"\x01\x03"] * 10000, type=pa.binary()),
                "name": pa.array(["b"] * 10000),
            }
        )

        call_count = 0

        def mock_fetch(url, extract_fid=False):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return page1
            return page2

        with (
            patch("geoparquet_io.core.wfs._get_feature_count", return_value=20000),
            patch("geoparquet_io.core.wfs._fetch_wfs_page", side_effect=mock_fetch),
        ):
            result = fetch_all_features_duckdb(
                "https://mock.wfs/wfs",
                "layer",
                max_workers=1,
                page_size=10000,
            )

        assert call_count == 2
        assert result.num_rows == 20000

    def test_single_worker_no_pagination_when_count_within_page_size(self):
        """With max_workers=1 and total <= page_size, should use single request."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import fetch_all_features_duckdb

        mock_table = pa.table(
            {
                "geometry": pa.array([b"\x01\x02"] * 5000, type=pa.binary()),
                "name": pa.array(["a"] * 5000),
            }
        )

        with (
            patch("geoparquet_io.core.wfs._get_feature_count", return_value=5000),
            patch("geoparquet_io.core.wfs._fetch_wfs_page", return_value=mock_table) as mock_fetch,
        ):
            result = fetch_all_features_duckdb(
                "https://mock.wfs/wfs",
                "layer",
                max_workers=1,
                page_size=10000,
            )

        mock_fetch.assert_called_once()
        assert result.num_rows == 5000

    def test_single_worker_no_pagination_when_count_unknown(self):
        """With max_workers=1 and unknown count, should use single request."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import fetch_all_features_duckdb

        mock_table = pa.table(
            {
                "geometry": pa.array([b"\x01\x02"], type=pa.binary()),
                "name": pa.array(["a"]),
            }
        )

        with (
            patch("geoparquet_io.core.wfs._get_feature_count", return_value=None),
            patch("geoparquet_io.core.wfs._fetch_wfs_page", return_value=mock_table) as mock_fetch,
        ):
            result = fetch_all_features_duckdb(
                "https://mock.wfs/wfs",
                "layer",
                max_workers=1,
                page_size=10000,
            )

        mock_fetch.assert_called_once()
        assert result.num_rows == 1

    def test_single_worker_pagination_respects_max_features(self):
        """Auto-pagination should respect max_features limit."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import fetch_all_features_duckdb

        call_count = 0

        def mock_fetch(url, extract_fid=False):
            nonlocal call_count
            call_count += 1
            n = 10000 if call_count == 1 else 5000
            return pa.table(
                {
                    "geometry": pa.array([b"\x01\x02"] * n, type=pa.binary()),
                    "name": pa.array(["a"] * n),
                }
            )

        with (
            patch("geoparquet_io.core.wfs._get_feature_count", return_value=100000),
            patch("geoparquet_io.core.wfs._fetch_wfs_page", side_effect=mock_fetch),
        ):
            result = fetch_all_features_duckdb(
                "https://mock.wfs/wfs",
                "layer",
                max_workers=1,
                max_features=15000,
                page_size=10000,
            )

        assert call_count == 2
        assert result.num_rows == 15000

    def test_parallel_workers_use_httpx_fetcher(self):
        """Parallel mode should use _fetch_wfs_page (httpx-based)."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import fetch_all_features_duckdb

        page = pa.table(
            {
                "geometry": pa.array([b"\x01\x02"], type=pa.binary()),
                "name": pa.array(["a"]),
            }
        )

        with (
            patch("geoparquet_io.core.wfs._get_feature_count", return_value=20000),
            patch("geoparquet_io.core.wfs._fetch_wfs_page", return_value=page) as mock_fetch,
        ):
            fetch_all_features_duckdb(
                "https://mock.wfs/wfs",
                "layer",
                max_workers=2,
                page_size=10000,
            )

        assert mock_fetch.call_count == 2

    def test_startindex_limit_raises_clear_error(self):
        """When server has startIndex limit, should raise with actionable guidance."""
        from geoparquet_io.core.wfs import WFSError, fetch_all_features_duckdb

        with (
            patch("geoparquet_io.core.wfs._get_feature_count", return_value=11000000),
            patch("geoparquet_io.core.wfs._probe_startindex_limit", return_value=50000),
        ):
            with pytest.raises(WFSError, match="startIndex.*50,000"):
                fetch_all_features_duckdb(
                    "https://mock.wfs/wfs",
                    "layer",
                    max_workers=1,
                    page_size=10000,
                )

    @pytest.mark.skipif(
        os.environ.get("PYTEST_XDIST_WORKER") is not None,
        reason="DuckDB resource contention crashes pytest-xdist workers",
    )
    def test_startindex_limit_allows_small_datasets(self):
        """When total features fit within the startIndex limit, should proceed."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import fetch_all_features_duckdb

        page = pa.table(
            {
                "geometry": pa.array([b"\x01\x02"], type=pa.binary()),
                "name": pa.array(["a"]),
            }
        )

        with (
            patch("geoparquet_io.core.wfs._get_feature_count", return_value=30000),
            patch("geoparquet_io.core.wfs._probe_startindex_limit", return_value=50000),
            patch("geoparquet_io.core.wfs._fetch_wfs_page", return_value=page),
        ):
            result = fetch_all_features_duckdb(
                "https://mock.wfs/wfs",
                "layer",
                max_workers=1,
                page_size=10000,
            )

        assert result.num_rows > 0

    @pytest.mark.skipif(
        os.environ.get("PYTEST_XDIST_WORKER") is not None,
        reason="DuckDB resource contention crashes pytest-xdist workers",
    )
    def test_no_startindex_limit_proceeds_normally(self):
        """When server has no startIndex limit, pagination should proceed."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import fetch_all_features_duckdb

        page = pa.table(
            {
                "geometry": pa.array([b"\x01\x02"], type=pa.binary()),
                "name": pa.array(["a"]),
            }
        )

        with (
            patch("geoparquet_io.core.wfs._get_feature_count", return_value=20000),
            patch("geoparquet_io.core.wfs._probe_startindex_limit", return_value=None),
            patch("geoparquet_io.core.wfs._fetch_wfs_page", return_value=page),
        ):
            result = fetch_all_features_duckdb(
                "https://mock.wfs/wfs",
                "layer",
                max_workers=1,
                page_size=10000,
            )

        assert result.num_rows > 0


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

    def test_is_geographic_crs_handles_codes_outside_allowlist(self):
        """Geographic CRSs beyond the small allowlist must not be called projected.

        pyproj-backed detection identifies any geographic CRS; a hardcoded list
        would misclassify these and trigger a false coordinate mismatch.
        """
        from geoparquet_io.core.wfs import _is_geographic_crs

        assert _is_geographic_crs("EPSG:4171") is True  # RGF93 (geographic)
        assert _is_geographic_crs("EPSG:4258") is True  # ETRS89 (geographic)
        assert _is_geographic_crs("urn:ogc:def:crs:EPSG::4171") is True
        assert _is_geographic_crs("EPSG:22174") is False  # POSGAR 98 (projected)
        assert _is_geographic_crs("EPSG:25830") is False  # ETRS89 / UTM 30N (projected)

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
            # WFS 1.1.0+: Always EPSG:4326 with XY order (bbox is always WGS84)
            # This ensures compatibility with servers that only accept WGS84 bbox
            (
                (-122.5, 37.5, -122.0, 38.0),
                "EPSG:4326",
                "1.1.0",
                "auto",
                "-122.5,37.5,-122.0,38.0,EPSG:4326",
            ),
            # Even with URN output CRS, bbox uses simple EPSG:4326
            (
                (-122.5, 37.5, -122.0, 38.0),
                "urn:ogc:def:crs:EPSG::4326",
                "1.1.0",
                "auto",
                "-122.5,37.5,-122.0,38.0,EPSG:4326",
            ),
            # WFS 2.0.0 also uses EPSG:4326 for bbox
            (
                (4.82, 50.44, 4.92, 50.48),
                "urn:ogc:def:crs:EPSG::4326",
                "2.0.0",
                "auto",
                "4.82,50.44,4.92,50.48,EPSG:4326",
            ),
            # Projected output CRS still uses WGS84 bbox
            (
                (-122.5, 37.5, -122.0, 38.0),
                "EPSG:3857",
                "1.1.0",
                "auto",
                "-122.5,37.5,-122.0,38.0,EPSG:4326",
            ),
        ],
    )
    def test_build_bbox_param_axis_order(self, bbox, crs, version, axis_order, expected):
        """Bbox parameter string should always use WGS84 for server compatibility."""
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

    def test_validate_crs_coordinates_does_not_override_projected_crs(self):
        """Should NOT override a server-honored projected CRS on a bbox guess (issue #499).

        EPSG:22174 (POSGAR 98 / Argentina 4) and EPSG:3857 both use large metric
        coordinates that the bbox heuristic cannot distinguish. When the server
        honored the requested projected CRS, we must trust it rather than relabel
        it as the heuristic's guess, which silently corrupts downstream reprojection.
        """
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection
        from geoparquet_io.core.wfs import _validate_crs_coordinates

        # Real EPSG:22174 coordinates from IDECOR (Córdoba, Argentina)
        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
        try:
            result = con.execute("""
                SELECT ST_AsWKB(ST_Point(4527586.01, 6386852.32)) as geometry
            """).arrow()
            table = result.read_all()
        finally:
            con.close()

        # Requested and server-returned 22174 — heuristic would guess 3857, but
        # both are projected, so there is no real (category) mismatch.
        is_valid, detected = _validate_crs_coordinates(table, "EPSG:22174", strict=False)
        assert is_valid is True
        assert detected is None

    def test_validate_crs_coordinates_detects_projected_data_for_geographic_request(self):
        """Should still flag a real category mismatch: geographic requested, metric data."""
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection
        from geoparquet_io.core.wfs import _validate_crs_coordinates

        # Projected (metric) coordinates returned for a non-4326 geographic request
        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
        try:
            result = con.execute("""
                SELECT ST_AsWKB(ST_Point(4527586.01, 6386852.32)) as geometry
            """).arrow()
            table = result.read_all()
        finally:
            con.close()

        # EPSG:4258 (ETRS89) is geographic — metric values are a real mismatch.
        is_valid, detected = _validate_crs_coordinates(table, "EPSG:4258", strict=False)
        assert is_valid is False
        assert detected == "EPSG:3857"

    def test_validate_crs_coordinates_detects_geographic_data_for_projected_request(self):
        """Should flag a real category mismatch: projected requested, degree data."""
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection
        from geoparquet_io.core.wfs import _validate_crs_coordinates

        # Degree-range coordinates returned for a projected request
        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
        try:
            result = con.execute("""
                SELECT ST_AsWKB(ST_Point(-62.7, -32.6)) as geometry
            """).arrow()
            table = result.read_all()
        finally:
            con.close()

        # EPSG:22174 is projected — degree values are a real mismatch.
        is_valid, detected = _validate_crs_coordinates(table, "EPSG:22174", strict=False)
        assert is_valid is False
        assert detected == "EPSG:4326"

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

    def test_wfs_to_table_reprojects_on_mismatch(self, offline_wfs_probes):
        """wfs_to_table should reproject when output_crs set and server returns different CRS."""
        from unittest.mock import MagicMock, patch

        from geoparquet_io.core.duckdb_utils import get_duckdb_connection
        from geoparquet_io.core.wfs import wfs_to_table

        # Create mock table with EPSG:3035 coordinates
        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
        try:
            result = con.execute("""
                SELECT ST_AsWKB(ST_Point(3900000, 3000000)) as geometry
            """).arrow()
            mock_table = result.read_all()
        finally:
            con.close()

        # Mock the WFS fetching to return our test table
        with (
            patch("geoparquet_io.core.wfs.negotiate_wfs_version") as mock_version,
            patch("geoparquet_io.core.wfs.get_layer_info") as mock_layer_info,
            patch("geoparquet_io.core.wfs.fetch_all_features_duckdb") as mock_fetch,
        ):
            mock_version.return_value = ("2.0.0", MagicMock())
            mock_layer_info.return_value = MagicMock(
                typename="mock:layer",
                title="Mock Layer",
                crs_list=["EPSG:4326", "EPSG:3035"],
                default_crs="EPSG:4326",
                available_formats=["application/json"],
            )
            mock_fetch.return_value = mock_table

            # Call wfs_to_table with output_crs - should trigger reprojection
            result = wfs_to_table(
                service_url="https://mock.wfs.server/wfs",
                typename="mock:layer",
                output_crs="EPSG:4326",
            )

        # Verify result is reprojected to WGS84
        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
        try:
            con.register("data", result)
            coords = con.execute("""
                SELECT
                    ST_X(ST_GeomFromWKB(geometry)) as x,
                    ST_Y(ST_GeomFromWKB(geometry)) as y
                FROM data
            """).fetchone()
            x, y = coords
        finally:
            con.close()

        # Should be in WGS84 range (Western Europe)
        assert -180 <= x <= 180, f"X {x} not in WGS84 range"
        assert -90 <= y <= 90, f"Y {y} not in WGS84 range"

    def test_reproject_error_wrapped_in_wfs_error(self, offline_wfs_probes):
        """Reprojection errors should be wrapped in WFSError with context."""
        from unittest.mock import MagicMock, patch

        import pyarrow as pa

        from geoparquet_io.core.wfs import WFSError, wfs_to_table

        # Create mock table with invalid geometry
        mock_table = pa.table({"geometry": [b"invalid_wkb"]})

        with (
            patch("geoparquet_io.core.wfs.negotiate_wfs_version") as mock_version,
            patch("geoparquet_io.core.wfs.get_layer_info") as mock_layer_info,
            patch("geoparquet_io.core.wfs.fetch_all_features_duckdb") as mock_fetch,
            patch("geoparquet_io.core.wfs._validate_crs_coordinates") as mock_validate,
        ):
            mock_version.return_value = ("2.0.0", MagicMock())
            mock_layer_info.return_value = MagicMock(
                typename="mock:layer",
                title="Mock Layer",
                crs_list=["EPSG:4326"],
                default_crs="EPSG:4326",
                available_formats=["application/json"],
            )
            mock_fetch.return_value = mock_table
            # Force mismatch detection
            mock_validate.return_value = (False, "EPSG:3035")

            with pytest.raises(WFSError) as exc_info:
                wfs_to_table(
                    service_url="https://mock.wfs.server/wfs",
                    typename="mock:layer",
                    output_crs="EPSG:4326",
                )

            assert "Failed to reproject" in str(exc_info.value)
            assert "EPSG:3035" in str(exc_info.value)
            assert "EPSG:4326" in str(exc_info.value)


class TestServerDeclaredCRS:
    """The server's GeoJSON ``crs`` member is authoritative (issue #499).

    When a WFS server echoes the CRS it actually used in the FeatureCollection
    ``crs`` member, we must read and trust it rather than guessing from the
    bounding box — the bbox heuristic cannot tell two projected CRSs apart.
    """

    def _make_mock_stream(self, geojson_bytes: bytes):
        """Create a mock httpx stream response yielding the given bytes."""
        import httpx

        def mock_stream(*args, **kwargs):
            class MockResponse:
                status_code = 200
                headers = {"content-type": "application/json"}

                def raise_for_status(self):
                    pass

                def iter_bytes(self, chunk_size=None):
                    yield geojson_bytes

                def __enter__(self):
                    return self

                def __exit__(self, *args):
                    pass

            return MockResponse()

        return patch.object(httpx.Client, "stream", mock_stream)

    def _geojson_with_crs(self, crs_name: str | None) -> bytes:
        import json

        feature_collection: dict = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [4527586.01, 6386852.32]},
                    "properties": {"name": "a"},
                }
            ],
        }
        if crs_name is not None:
            feature_collection["crs"] = {"type": "name", "properties": {"name": crs_name}}
        return json.dumps(feature_collection).encode()

    def test_fetch_wfs_page_extracts_server_crs(self):
        """_fetch_wfs_page records the server-declared CRS in schema metadata."""
        from geoparquet_io.core.wfs import _SERVER_CRS_METADATA_KEY, _fetch_wfs_page

        body = self._geojson_with_crs("urn:ogc:def:crs:EPSG::22174")
        with self._make_mock_stream(body):
            table = _fetch_wfs_page("http://mock.wfs/wfs?service=WFS")

        assert table.schema.metadata is not None
        assert table.schema.metadata[_SERVER_CRS_METADATA_KEY] == b"EPSG:22174"

    def test_fetch_wfs_page_no_crs_member(self):
        """No ``crs`` member (RFC 7946 GeoJSON) leaves no server-CRS metadata."""
        from geoparquet_io.core.wfs import _SERVER_CRS_METADATA_KEY, _fetch_wfs_page

        with self._make_mock_stream(self._geojson_with_crs(None)):
            table = _fetch_wfs_page("http://mock.wfs/wfs?service=WFS")

        metadata = table.schema.metadata or {}
        assert _SERVER_CRS_METADATA_KEY not in metadata

    def test_fetch_wfs_page_empty_response_carries_crs(self):
        """An empty FeatureCollection still propagates its declared CRS."""
        import json

        from geoparquet_io.core.wfs import _SERVER_CRS_METADATA_KEY, _fetch_wfs_page

        body = json.dumps(
            {
                "type": "FeatureCollection",
                "crs": {"type": "name", "properties": {"name": "urn:ogc:def:crs:EPSG::22174"}},
                "features": [],
            }
        ).encode()
        with self._make_mock_stream(body):
            table = _fetch_wfs_page("http://mock.wfs/wfs?service=WFS")

        assert table.num_rows == 0
        assert (table.schema.metadata or {}).get(_SERVER_CRS_METADATA_KEY) == b"EPSG:22174"

    def test_fetch_wfs_page_unrecognized_crs_shape(self):
        """A ``crs`` member without a name (e.g. a link CRS) yields no metadata.

        ``json_extract_string`` returns NULL rather than raising on an
        unexpected shape, so extraction falls back to the bbox heuristic.
        """
        import json

        from geoparquet_io.core.wfs import _SERVER_CRS_METADATA_KEY, _fetch_wfs_page

        body = json.dumps(
            {
                "type": "FeatureCollection",
                "crs": {"type": "link", "properties": {"href": "http://example/crs"}},
                "features": [
                    {
                        "type": "Feature",
                        "geometry": {"type": "Point", "coordinates": [4527586.01, 6386852.32]},
                        "properties": {"name": "a"},
                    }
                ],
            }
        ).encode()
        with self._make_mock_stream(body):
            table = _fetch_wfs_page("http://mock.wfs/wfs?service=WFS")

        assert table.num_rows == 1
        assert _SERVER_CRS_METADATA_KEY not in (table.schema.metadata or {})

    def test_read_and_with_server_crs_roundtrip(self):
        """_with_server_crs / _read_server_crs round-trip through metadata."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _read_server_crs, _with_server_crs

        table = pa.table({"geometry": pa.array([], type=pa.binary())})
        assert _read_server_crs(table) is None

        tagged = _with_server_crs(table, "EPSG:22174")
        assert _read_server_crs(tagged) == "EPSG:22174"

        # No-op when CRS is None.
        assert _read_server_crs(_with_server_crs(table, None)) is None

    def test_fetch_all_features_propagates_server_crs(self):
        """Server CRS survives the single-request fetch path."""
        from unittest.mock import patch

        import pyarrow as pa

        from geoparquet_io.core.wfs import (
            _read_server_crs,
            _with_server_crs,
            fetch_all_features_duckdb,
        )

        page = _with_server_crs(
            pa.table({"geometry": pa.array([b"\x00"], type=pa.binary())}),
            "EPSG:22174",
        )
        with (
            patch("geoparquet_io.core.wfs._get_feature_count", return_value=1),
            patch("geoparquet_io.core.wfs._fetch_wfs_page", return_value=page),
            patch("geoparquet_io.core.wfs._infer_column_types", side_effect=lambda t: t),
        ):
            result = fetch_all_features_duckdb("http://mock.wfs/wfs", "layer", version="2.0.0")

        assert _read_server_crs(result) == "EPSG:22174"


class TestResolveCRSForOutput:
    """wfs_to_table CRS resolution: trust the server, guess only as a fallback."""

    def _point_table(self, x: float, y: float):
        from geoparquet_io.core.duckdb_utils import get_duckdb_connection

        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
        try:
            result = con.execute(f"SELECT ST_AsWKB(ST_Point({x}, {y})) as geometry").arrow()
            return result.read_all()
        finally:
            con.close()

    def test_trusts_matching_server_crs_without_guessing(self):
        """Issue #499: server honored EPSG:22174 → keep it, never guess EPSG:3857."""
        from unittest.mock import patch

        from geoparquet_io.core.wfs import _resolve_crs_for_output, _with_server_crs

        # Argentine Gauss-Krüger coords the bbox heuristic would misread as 3857.
        table = _with_server_crs(self._point_table(4527586.01, 6386852.32), "EPSG:22174")

        with patch("geoparquet_io.core.wfs._validate_crs_coordinates") as mock_validate:
            out_table, crs = _resolve_crs_for_output(table, "EPSG:22174", None, False)

        assert crs == "EPSG:22174"
        # The heuristic validator must not even run when the server declared a CRS.
        mock_validate.assert_not_called()

    def test_server_crs_contradiction_labels_with_server_crs(self):
        """Server ignored srsName (returned 3857 for 22174) and no output_crs → label 3857."""
        from geoparquet_io.core.wfs import _resolve_crs_for_output, _with_server_crs

        table = _with_server_crs(self._point_table(4527586.01, 6386852.32), "EPSG:3857")

        out_table, crs = _resolve_crs_for_output(table, "EPSG:22174", None, False)

        # Authoritative server CRS wins over the requested label; no bbox guess.
        assert crs == "EPSG:3857"
        assert out_table is table

    def test_server_crs_contradiction_reprojects_to_output_crs(self):
        """Contradiction with output_crs set → reproject from the server CRS."""
        from unittest.mock import patch

        from geoparquet_io.core.wfs import _resolve_crs_for_output, _with_server_crs

        table = _with_server_crs(self._point_table(4527586.01, 6386852.32), "EPSG:3857")

        with patch(
            "geoparquet_io.core.wfs.reproject_table", return_value="REPROJECTED"
        ) as mock_reproject:
            out_table, crs = _resolve_crs_for_output(table, "EPSG:22174", "EPSG:4326", False)

        assert crs == "EPSG:4326"
        assert out_table == "REPROJECTED"
        mock_reproject.assert_called_once()
        assert mock_reproject.call_args.kwargs["source_crs"] == "EPSG:3857"
        assert mock_reproject.call_args.kwargs["target_crs"] == "EPSG:4326"

    def test_server_crs_contradiction_strict_raises(self):
        """Contradiction under strict_crs → WFSError, even with no output_crs."""
        from geoparquet_io.core.wfs import WFSError, _resolve_crs_for_output, _with_server_crs

        table = _with_server_crs(self._point_table(4527586.01, 6386852.32), "EPSG:3857")

        with pytest.raises(WFSError, match="server may have ignored srsName"):
            _resolve_crs_for_output(table, "EPSG:22174", None, True)

    def test_falls_back_to_heuristic_without_server_crs(self):
        """No server CRS → fall back to coordinate-range guessing (degrees for 4326)."""
        from unittest.mock import patch

        from geoparquet_io.core.wfs import _resolve_crs_for_output

        # Projected metric coords but WGS84 requested, no server CRS declared.
        table = self._point_table(3900000, 3000000)

        with patch("geoparquet_io.core.wfs.debug") as mock_debug:
            out_table, crs = _resolve_crs_for_output(table, "EPSG:4326", None, False)

        # Heuristic detects projected data and relabels (fallback path preserved).
        assert crs == "EPSG:3035"
        # The fallback to coordinate inference is announced (visible under --verbose).
        assert any("declared no CRS" in str(call.args[0]) for call in mock_debug.call_args_list)

    def test_wfs_to_table_trusts_server_crs_end_to_end(self, offline_wfs_probes):
        """End-to-end #499 regression: declared EPSG:22174 stays EPSG:22174."""
        import json
        from unittest.mock import MagicMock, patch

        from geoparquet_io.core.crs_utils import parse_crs_string_to_projjson
        from geoparquet_io.core.wfs import _with_server_crs, wfs_to_table

        table = _with_server_crs(self._point_table(4527586.01, 6386852.32), "EPSG:22174")

        with (
            patch("geoparquet_io.core.wfs.negotiate_wfs_version") as mock_version,
            patch("geoparquet_io.core.wfs.get_layer_info") as mock_layer_info,
            patch("geoparquet_io.core.wfs.fetch_all_features_duckdb", return_value=table),
            patch("geoparquet_io.core.wfs._validate_crs_coordinates") as mock_validate,
        ):
            mock_version.return_value = ("2.0.0", MagicMock())
            mock_layer_info.return_value = MagicMock(
                typename="mock:layer",
                title="Mock Layer",
                crs_list=["EPSG:22174"],
                default_crs="EPSG:22174",
                available_formats=["application/json"],
                sortable_attribute=None,
                bbox=None,
            )
            result = wfs_to_table(
                service_url="https://mock.wfs.server/wfs",
                typename="mock:layer",
                output_crs="EPSG:22174",
            )

        mock_validate.assert_not_called()
        # Output metadata must encode EPSG:22174, not a bbox guess.
        geo = json.loads(result.schema.metadata[b"geo"])
        expected = parse_crs_string_to_projjson("EPSG:22174")
        assert geo["columns"]["geometry"]["crs"] == expected

    def test_wfs_to_table_preserves_server_crs_through_local_bbox_filter(self, offline_wfs_probes):
        """Issue #499: local bbox filtering must not drop the server CRS.

        The local-filter path round-trips the table through DuckDB, which
        strips Arrow schema metadata. If the server-declared CRS is lost there,
        resolution silently falls back to the bbox heuristic — the exact bug
        #499 fixes — so we assert the heuristic validator is never consulted.
        """
        import json
        from unittest.mock import MagicMock, patch

        from geoparquet_io.core.crs_utils import parse_crs_string_to_projjson
        from geoparquet_io.core.wfs import _with_server_crs, wfs_to_table

        table = _with_server_crs(self._point_table(4527586.01, 6386852.32), "EPSG:22174")

        with (
            patch("geoparquet_io.core.wfs.negotiate_wfs_version") as mock_version,
            patch("geoparquet_io.core.wfs.get_layer_info") as mock_layer_info,
            patch("geoparquet_io.core.wfs.fetch_all_features_duckdb", return_value=table),
            patch("geoparquet_io.core.wfs._validate_crs_coordinates") as mock_validate,
        ):
            mock_version.return_value = ("2.0.0", MagicMock())
            mock_layer_info.return_value = MagicMock(
                typename="mock:layer",
                title="Mock Layer",
                crs_list=["EPSG:22174"],
                default_crs="EPSG:22174",
                available_formats=["application/json"],
                sortable_attribute=None,
                bbox=None,
            )
            result = wfs_to_table(
                service_url="https://mock.wfs.server/wfs",
                typename="mock:layer",
                bbox=(4_000_000, 6_000_000, 5_000_000, 7_000_000),
                bbox_mode="local",
            )

        # Server CRS survived the local-filter roundtrip → no heuristic guess.
        mock_validate.assert_not_called()
        assert result.num_rows == 1
        geo = json.loads(result.schema.metadata[b"geo"])
        assert geo["columns"]["geometry"]["crs"] == parse_crs_string_to_projjson("EPSG:22174")

    def test_local_bbox_filter_reprojects_bbox_to_server_crs(self, offline_wfs_probes):
        """Issue #499: a bbox in the requested CRS must be aligned to the CRS the
        server actually returned, or local filtering drops valid rows.

        Requested EPSG:4326 (bbox in degrees), but the server returns EPSG:22174
        geometry. The degree bbox would not intersect the metric point unless it
        is reprojected into EPSG:22174 first.
        """
        from unittest.mock import MagicMock, patch

        from geoparquet_io.core.wfs import _with_server_crs, wfs_to_table

        # EPSG:22174 point near Córdoba, AR (= -62.7059, -32.6603 in EPSG:4326).
        table = _with_server_crs(self._point_table(4527586.01, 6386852.32), "EPSG:22174")
        deg_bbox = (-62.8059, -32.7603, -62.6059, -32.5603)

        with (
            patch("geoparquet_io.core.wfs.negotiate_wfs_version") as mock_version,
            patch("geoparquet_io.core.wfs.get_layer_info") as mock_layer_info,
            patch("geoparquet_io.core.wfs.fetch_all_features_duckdb", return_value=table),
        ):
            mock_version.return_value = ("2.0.0", MagicMock())
            mock_layer_info.return_value = MagicMock(
                typename="mock:layer",
                title="Mock Layer",
                crs_list=["EPSG:4326"],
                default_crs="EPSG:4326",
                available_formats=["application/json"],
                sortable_attribute=None,
                bbox=None,
            )
            result = wfs_to_table(
                service_url="https://mock.wfs.server/wfs",
                typename="mock:layer",
                bbox=deg_bbox,
                bbox_mode="local",
            )

        # The point is retained because the bbox was reprojected to EPSG:22174.
        assert result.num_rows == 1

    def test_wfs_to_table_strips_server_crs_marker_when_no_projjson(self, offline_wfs_probes):
        """The internal server-CRS marker never leaks into the output schema.

        Even when no geo metadata is written (projjson unavailable for the
        resolved CRS), the ``_wfs_server_crs`` marker must be removed.
        """
        from unittest.mock import MagicMock, patch

        from geoparquet_io.core.wfs import (
            _SERVER_CRS_METADATA_KEY,
            _with_server_crs,
            wfs_to_table,
        )

        table = _with_server_crs(self._point_table(4527586.01, 6386852.32), "EPSG:22174")

        with (
            patch("geoparquet_io.core.wfs.negotiate_wfs_version") as mock_version,
            patch("geoparquet_io.core.wfs.get_layer_info") as mock_layer_info,
            patch("geoparquet_io.core.wfs.fetch_all_features_duckdb", return_value=table),
            patch("geoparquet_io.core.wfs.parse_crs_string_to_projjson", return_value=None),
        ):
            mock_version.return_value = ("2.0.0", MagicMock())
            mock_layer_info.return_value = MagicMock(
                typename="mock:layer",
                title="Mock Layer",
                crs_list=["EPSG:22174"],
                default_crs="EPSG:22174",
                available_formats=["application/json"],
                sortable_attribute=None,
                bbox=None,
            )
            result = wfs_to_table(
                service_url="https://mock.wfs.server/wfs",
                typename="mock:layer",
                output_crs="EPSG:22174",
            )

        metadata = result.schema.metadata or {}
        assert _SERVER_CRS_METADATA_KEY not in metadata
        assert b"geo" not in metadata


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

    @pytest.mark.xfail(
        reason="External WFS service may be unavailable",
        strict=False,
        raises=(Exception,),
    )
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


@pytest.mark.integration
@pytest.mark.network
@pytest.mark.slow
class TestProjectedCRSPreservedIntegration:
    """Live regression for #499: a server-honored projected CRS must be kept.

    IDECOR (Córdoba, Argentina) serves all layers in EPSG:22174 (an Argentine
    Gauss-Krüger zone). The old bbox heuristic saw the large metric coordinates
    (~4.5M, ~6.4M) and relabeled the output EPSG:3857, silently corrupting any
    downstream reprojection. The geometry was correct, so this can only be
    caught by asserting on the stored CRS metadata, not coordinate ranges.
    """

    IDECOR_WFS = "https://idecor-ws.mapascordoba.gob.ar/geoserver/idecor/wfs"
    IDECOR_LAYER = "idecor:puntos_interes_bv"

    @pytest.mark.xfail(
        reason="External WFS service may be unavailable",
        strict=False,
        raises=(Exception,),  # Only xfail on connection/service errors
    )
    def test_projected_crs_not_relabeled(self):
        """Output metadata must report EPSG:22174, never the bbox-guessed 3857."""
        import json

        from geoparquet_io.core.wfs import wfs_to_table

        table = wfs_to_table(
            self.IDECOR_WFS,
            self.IDECOR_LAYER,
            output_crs="EPSG:22174",
            limit=5,
        )

        assert table.num_rows > 0

        geo = json.loads(table.schema.metadata[b"geo"])
        crs = geo["columns"]["geometry"]["crs"]
        # PROJJSON for EPSG:22174 — the authoritative code must be preserved.
        assert crs["id"]["authority"] == "EPSG"
        assert int(crs["id"]["code"]) == 22174, (
            f"Output mislabeled as {crs['id']}; expected EPSG:22174 (issue #499)"
        )


# =============================================================================
# Spatial Tiling Tests (startIndex limit workaround)
# =============================================================================


class TestGenerateTileGrid:
    """Test grid generation for spatial tiling."""

    def test_correct_count(self):
        """Grid should produce at least the requested number of tiles."""
        from geoparquet_io.core.wfs import _generate_tile_grid

        bbox = (3.0, 50.0, 7.0, 54.0)
        tiles = _generate_tile_grid(bbox, 4)
        assert len(tiles) >= 4

    def test_covers_full_bbox(self):
        """Union of tile bboxes should cover the original bbox."""
        from geoparquet_io.core.wfs import _generate_tile_grid

        bbox = (3.0, 50.0, 7.0, 54.0)
        tiles = _generate_tile_grid(bbox, 6)

        all_xmin = min(t[0] for t in tiles)
        all_ymin = min(t[1] for t in tiles)
        all_xmax = max(t[2] for t in tiles)
        all_ymax = max(t[3] for t in tiles)

        assert all_xmin == pytest.approx(bbox[0])
        assert all_ymin == pytest.approx(bbox[1])
        assert all_xmax == pytest.approx(bbox[2])
        assert all_ymax == pytest.approx(bbox[3])

    def test_single_tile(self):
        """Requesting 1 tile should return the original bbox."""
        from geoparquet_io.core.wfs import _generate_tile_grid

        bbox = (3.0, 50.0, 7.0, 54.0)
        tiles = _generate_tile_grid(bbox, 1)
        assert len(tiles) == 1
        assert tiles[0] == pytest.approx(bbox)

    def test_respects_aspect_ratio(self):
        """Wide bbox should produce more columns than rows."""
        from geoparquet_io.core.wfs import _generate_tile_grid

        wide_bbox = (0.0, 0.0, 10.0, 2.0)
        tiles = _generate_tile_grid(wide_bbox, 10)

        xs = sorted({t[0] for t in tiles} | {t[2] for t in tiles})
        ys = sorted({t[1] for t in tiles} | {t[3] for t in tiles})
        cols = len(xs) - 1
        rows = len(ys) - 1
        assert cols > rows


class TestRefineTilesAdaptive:
    """Test adaptive quadtree refinement of tile grid."""

    def test_splits_oversized_tile(self):
        """Tile with count > limit should be subdivided into 4."""
        from geoparquet_io.core.wfs import _refine_tiles_adaptive

        tiles = [(0.0, 0.0, 1.0, 1.0)]

        def mock_count(service_url, typename, version, bbox=None, crs=None, axis_order="auto"):
            if bbox == (0.0, 0.0, 1.0, 1.0):
                return 100000
            return 20000

        with patch("geoparquet_io.core.wfs._get_feature_count", side_effect=mock_count):
            result = _refine_tiles_adaptive(
                tiles,
                "http://mock/wfs",
                "layer",
                "1.1.0",
                crs="EPSG:4326",
                axis_order="auto",
                max_per_tile=50000,
            )

        assert len(result) == 4

    def test_leaves_small_tiles_alone(self):
        """Tile with count < limit should not be subdivided."""
        from geoparquet_io.core.wfs import _refine_tiles_adaptive

        tiles = [(0.0, 0.0, 1.0, 1.0)]

        with patch("geoparquet_io.core.wfs._get_feature_count", return_value=10000):
            result = _refine_tiles_adaptive(
                tiles,
                "http://mock/wfs",
                "layer",
                "1.1.0",
                crs="EPSG:4326",
                axis_order="auto",
                max_per_tile=50000,
            )

        assert len(result) == 1
        assert result[0] == tiles[0]

    def test_max_depth_prevents_infinite_recursion(self):
        """Recursion should stop at max_depth even if tiles are still too large."""
        from geoparquet_io.core.wfs import _refine_tiles_adaptive

        tiles = [(0.0, 0.0, 1.0, 1.0)]

        with patch("geoparquet_io.core.wfs._get_feature_count", return_value=999999):
            result = _refine_tiles_adaptive(
                tiles,
                "http://mock/wfs",
                "layer",
                "1.1.0",
                crs="EPSG:4326",
                axis_order="auto",
                max_per_tile=50000,
                max_depth=2,
            )

        # depth 0: 1 tile -> 4 (depth 1) -> 16 (depth 2, max)
        assert len(result) == 16

    def test_handles_none_count_gracefully(self):
        """If _get_feature_count returns None, tile should be kept as-is."""
        from geoparquet_io.core.wfs import _refine_tiles_adaptive

        tiles = [(0.0, 0.0, 1.0, 1.0)]

        with patch("geoparquet_io.core.wfs._get_feature_count", return_value=None):
            result = _refine_tiles_adaptive(
                tiles,
                "http://mock/wfs",
                "layer",
                "1.1.0",
                crs="EPSG:4326",
                axis_order="auto",
                max_per_tile=50000,
            )

        assert len(result) == 1


class TestGetFeatureCountWithBbox:
    """Test _get_feature_count extended with bbox parameter."""

    def test_bbox_included_in_request(self):
        """When bbox provided, it should appear in the hits request."""
        from geoparquet_io.core.wfs import _get_feature_count

        with patch("geoparquet_io.core.wfs._make_request") as mock_req:
            mock_req.return_value = b'numberOfFeatures="42"'
            result = _get_feature_count(
                "http://mock/wfs",
                "layer",
                "1.1.0",
                bbox=(4.0, 52.0, 5.0, 53.0),
                crs="EPSG:4326",
            )

        assert result == 42
        call_params = mock_req.call_args[1]["params"]
        assert "bbox" in call_params

    def test_no_bbox_backward_compatible(self):
        """Without bbox, request should not include bbox param."""
        from geoparquet_io.core.wfs import _get_feature_count

        with patch("geoparquet_io.core.wfs._make_request") as mock_req:
            mock_req.return_value = b'numberOfFeatures="100"'
            result = _get_feature_count("http://mock/wfs", "layer", "1.1.0")

        assert result == 100
        call_params = mock_req.call_args[1]["params"]
        assert "bbox" not in call_params


class TestDeduplicateTiles:
    """Test deduplication of features across tile boundaries."""

    def test_removes_duplicates_by_fid(self):
        """Features with same _wfs_fid should be deduplicated."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _deduplicate_tiles

        table = pa.table(
            {
                "_wfs_fid": pa.array(["a", "b", "a", "c"]),
                "geometry": pa.array([b"\x01", b"\x02", b"\x01", b"\x03"], type=pa.binary()),
                "name": pa.array(["x", "y", "x", "z"]),
            }
        )

        result = _deduplicate_tiles(table)
        assert result.num_rows == 3

    def test_drops_fid_column(self):
        """_wfs_fid column should be removed from output."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _deduplicate_tiles

        table = pa.table(
            {
                "_wfs_fid": pa.array(["a", "b"]),
                "geometry": pa.array([b"\x01", b"\x02"], type=pa.binary()),
            }
        )

        result = _deduplicate_tiles(table)
        assert "_wfs_fid" not in result.column_names

    def test_fallback_geometry_dedup_when_no_fid(self):
        """When _wfs_fid is absent, deduplicate by geometry bytes."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _deduplicate_tiles

        table = pa.table(
            {
                "geometry": pa.array([b"\x01", b"\x02", b"\x01"], type=pa.binary()),
                "name": pa.array(["x", "y", "x"]),
            }
        )

        result = _deduplicate_tiles(table)
        assert result.num_rows == 2

    def test_null_fid_uses_geometry_dedup(self):
        """Features with NULL _wfs_fid should be deduplicated by geometry."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _deduplicate_tiles

        # Mix of valid fids and NULLs - NULLs should use geometry dedup
        table = pa.table(
            {
                "_wfs_fid": pa.array(["a", None, None, "b"], type=pa.string()),
                "geometry": pa.array([b"\x01", b"\x02", b"\x02", b"\x03"], type=pa.binary()),
                "name": pa.array(["w", "x", "x", "z"]),
            }
        )

        result = _deduplicate_tiles(table)
        # "a" and "b" kept (unique fids), one of the two NULL rows kept (same geometry)
        assert result.num_rows == 3
        assert "_wfs_fid" not in result.column_names

    def test_all_null_fids_uses_geometry_dedup(self):
        """When all fids are NULL, deduplicate entirely by geometry."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _deduplicate_tiles

        table = pa.table(
            {
                "_wfs_fid": pa.array([None, None, None, None], type=pa.string()),
                "geometry": pa.array([b"\x01", b"\x02", b"\x01", b"\x02"], type=pa.binary()),
                "name": pa.array(["a", "b", "a", "b"]),
            }
        )

        result = _deduplicate_tiles(table)
        assert result.num_rows == 2
        assert "_wfs_fid" not in result.column_names


class TestFetchWithSpatialTiles:
    """Test the spatial tiling orchestrator."""

    def test_combines_tile_results(self):
        """Should fetch each tile and combine results."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import _fetch_with_spatial_tiles

        tile1 = pa.table(
            {
                "_wfs_fid": pa.array(["a"]),
                "geometry": pa.array([b"\x01\x02"], type=pa.binary()),
                "name": pa.array(["x"]),
            }
        )
        tile2 = pa.table(
            {
                "_wfs_fid": pa.array(["b"]),
                "geometry": pa.array([b"\x01\x03"], type=pa.binary()),
                "name": pa.array(["y"]),
            }
        )

        call_count = 0

        def mock_fetch(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            return tile1 if call_count == 1 else tile2

        with (
            patch(
                "geoparquet_io.core.wfs._generate_tile_grid",
                return_value=[
                    (0.0, 0.0, 0.5, 0.5),
                    (0.5, 0.0, 1.0, 0.5),
                ],
            ),
            patch(
                "geoparquet_io.core.wfs._refine_tiles_adaptive",
                side_effect=lambda tiles, *a, **_kw: tiles,
            ),
            patch("geoparquet_io.core.wfs.fetch_all_features_duckdb", side_effect=mock_fetch),
        ):
            result = _fetch_with_spatial_tiles(
                service_url="http://mock/wfs",
                typename="layer",
                version="1.1.0",
                total_count=100000,
                startindex_limit=50000,
                layer_bbox=(0.0, 0.0, 1.0, 0.5),
                crs="EPSG:4326",
            )

        assert result.num_rows == 2
        assert "_wfs_fid" not in result.column_names

    def test_auto_tile_triggers_tiling(self):
        """wfs_to_table with auto_tile=True should invoke tiling when limit detected."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import wfs_to_table

        mock_table = pa.table(
            {
                "geometry": pa.array([b"\x01"], type=pa.binary()),
                "name": pa.array(["a"]),
            }
        )

        with (
            patch(
                "geoparquet_io.core.wfs.negotiate_wfs_version", return_value=("1.1.0", MagicMock())
            ),
            patch("geoparquet_io.core.wfs.get_layer_info") as mock_info,
            patch("geoparquet_io.core.wfs._get_feature_count", return_value=11000000),
            patch("geoparquet_io.core.wfs._probe_startindex_limit", return_value=50000),
            patch(
                "geoparquet_io.core.wfs._fetch_with_spatial_tiles", return_value=mock_table
            ) as mock_tile,
        ):
            mock_info.return_value = MagicMock(
                typename="layer",
                title="Layer",
                crs_list=["EPSG:4326"],
                default_crs="EPSG:4326",
                available_formats=["application/json"],
                bbox=(3.0, 50.0, 7.0, 54.0),
            )
            wfs_to_table("http://mock/wfs", "layer", auto_tile=True)

        mock_tile.assert_called_once()

    def test_auto_tile_noop_when_no_limit(self):
        """When server has no startIndex limit, should use normal fetch path."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import wfs_to_table

        mock_table = pa.table(
            {
                "geometry": pa.array([b"\x01"], type=pa.binary()),
                "name": pa.array(["a"]),
            }
        )

        with (
            patch(
                "geoparquet_io.core.wfs.negotiate_wfs_version", return_value=("1.1.0", MagicMock())
            ),
            patch("geoparquet_io.core.wfs.get_layer_info") as mock_info,
            patch("geoparquet_io.core.wfs._get_feature_count", return_value=20000),
            patch("geoparquet_io.core.wfs._probe_startindex_limit", return_value=None),
            patch(
                "geoparquet_io.core.wfs.fetch_all_features_duckdb", return_value=mock_table
            ) as mock_fetch,
            patch("geoparquet_io.core.wfs._fetch_with_spatial_tiles") as mock_tile,
        ):
            mock_info.return_value = MagicMock(
                typename="layer",
                title="Layer",
                crs_list=["EPSG:4326"],
                default_crs="EPSG:4326",
                available_formats=["application/json"],
                bbox=(3.0, 50.0, 7.0, 54.0),
            )
            wfs_to_table("http://mock/wfs", "layer", auto_tile=True)

        mock_fetch.assert_called_once()
        mock_tile.assert_not_called()


class TestServerCapDetection:
    """Cap detection must tolerate slight count drift (issue #503).

    Parallel workers retry pages on transient errors, which can nudge the total
    a few features past an exact server cap (e.g. 1,000,002 instead of
    1,000,000). The detector must still recognise these as caps so reactive
    auto-tiling fires.
    """

    def test_exact_common_caps_detected(self):
        from geoparquet_io.core.wfs import _looks_like_server_cap

        for cap in (1000000, 500000, 100000, 50000, 10000):
            assert _looks_like_server_cap(cap) is True

    def test_slight_drift_above_cap_detected(self):
        """The core #503 bug: parallel-worker drift past a round cap."""
        from geoparquet_io.core.wfs import _looks_like_server_cap

        assert _looks_like_server_cap(1000002) is True
        assert _looks_like_server_cap(1000500) is True  # +0.05%

    def test_slight_drift_below_cap_detected(self):
        from geoparquet_io.core.wfs import _looks_like_server_cap

        assert _looks_like_server_cap(999998) is True
        assert _looks_like_server_cap(49997) is True

    def test_outside_tolerance_not_detected(self):
        """Counts beyond ±0.1% of a cap (and not round) are real totals."""
        from geoparquet_io.core.wfs import _looks_like_server_cap

        assert _looks_like_server_cap(1002000) is False  # +0.2%, not round
        assert _looks_like_server_cap(1234567) is False

    def test_round_numbers_still_detected(self):
        """Existing divisible-by-10000 heuristic is preserved."""
        from geoparquet_io.core.wfs import _looks_like_server_cap

        assert _looks_like_server_cap(1230000) is True
        assert _looks_like_server_cap(40000) is True

    def test_zero_and_negative_not_detected(self):
        from geoparquet_io.core.wfs import _looks_like_server_cap

        assert _looks_like_server_cap(0) is False
        assert _looks_like_server_cap(-5) is False

    def test_reactive_tiling_triggers_on_drifted_cap(self):
        """End-to-end: a capped response of 10,005 (drifted from 10,000) with a
        higher 2.0.0 count must trigger reactive spatial tiling (issue #503)."""
        import pyarrow as pa

        from geoparquet_io.core.duckdb_utils import get_duckdb_connection
        from geoparquet_io.core.wfs import wfs_to_table

        # Build a real table whose row count looks like a drifted server cap.
        con = get_duckdb_connection(load_spatial=True, load_httpfs=False)
        try:
            capped_table = (
                con.execute("SELECT ST_AsWKB(ST_Point(0, 0)) AS geometry FROM range(10005)")
                .arrow()
                .read_all()
            )
        finally:
            con.close()

        tiled_table = pa.table(
            {
                "geometry": pa.array([b"\x01"], type=pa.binary()),
                "name": pa.array(["full"]),
            }
        )

        with (
            patch(
                "geoparquet_io.core.wfs.negotiate_wfs_version", return_value=("1.1.0", MagicMock())
            ),
            patch("geoparquet_io.core.wfs.get_layer_info") as mock_info,
            # 2.0.0 reports the true count; the capped fetch returns fewer.
            patch("geoparquet_io.core.wfs._get_feature_count", return_value=11000),
            patch("geoparquet_io.core.wfs._probe_startindex_limit", return_value=None),
            patch("geoparquet_io.core.wfs.fetch_all_features_duckdb", return_value=capped_table),
            patch(
                "geoparquet_io.core.wfs._fetch_with_spatial_tiles", return_value=tiled_table
            ) as mock_tile,
        ):
            mock_info.return_value = MagicMock(
                typename="layer",
                title="Layer",
                crs_list=["EPSG:4326"],
                default_crs="EPSG:4326",
                available_formats=["application/json"],
                bbox=(3.0, 50.0, 7.0, 54.0),
            )
            wfs_to_table("http://mock/wfs", "layer", auto_tile=True)

        mock_tile.assert_called_once()


class TestCountThreadedToFetch:
    """The trusted 2.0.0 count must be threaded into the fetch (issue #503).

    Previously ``fetch_all_features_duckdb`` re-queried the count at the
    requested version (e.g. 1.1.0), which on some GeoServer instances returns a
    DIFFERENT (capped) number than 2.0.0 — so pagination stopped early.
    """

    def test_fetch_uses_provided_total_count(self):
        """When total_count is passed, fetch must not re-query the count."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import fetch_all_features_duckdb

        # Table size must match total_count to avoid fallback to pagination
        small_table = pa.table({"geometry": pa.array([b"\x01"], type=pa.binary())})

        with (
            patch("geoparquet_io.core.wfs._get_feature_count") as mock_count,
            patch("geoparquet_io.core.wfs._single_fetch_mode", return_value=small_table),
        ):
            fetch_all_features_duckdb("http://mock/wfs", "layer", version="1.1.0", total_count=1)

        mock_count.assert_not_called()

    def test_wfs_to_table_threads_count_into_fetch(self):
        """wfs_to_table must pass the 2.0.0 expected_count as total_count."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import wfs_to_table

        mock_table = pa.table(
            {
                "geometry": pa.array([b"\x01"], type=pa.binary()),
                "name": pa.array(["a"]),
            }
        )

        with (
            patch(
                "geoparquet_io.core.wfs.negotiate_wfs_version", return_value=("1.1.0", MagicMock())
            ),
            patch("geoparquet_io.core.wfs.get_layer_info") as mock_info,
            patch("geoparquet_io.core.wfs._get_feature_count", return_value=20000),
            patch("geoparquet_io.core.wfs._probe_startindex_limit", return_value=None),
            patch(
                "geoparquet_io.core.wfs.fetch_all_features_duckdb", return_value=mock_table
            ) as mock_fetch,
        ):
            mock_info.return_value = MagicMock(
                typename="layer",
                title="Layer",
                crs_list=["EPSG:4326"],
                default_crs="EPSG:4326",
                available_formats=["application/json"],
                bbox=(3.0, 50.0, 7.0, 54.0),
            )
            wfs_to_table("http://mock/wfs", "layer", auto_tile=True)

        mock_fetch.assert_called_once()
        assert mock_fetch.call_args.kwargs.get("total_count") == 20000


class TestMultiLayerExtraction:
    """Tests for multi-layer parallel extraction."""

    def test_convert_wfs_layers_creates_directory(self, tmp_path):
        """Multi-layer extraction should create output directory and files."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import convert_wfs_layers_to_directory

        mock_table = pa.table(
            {
                "geometry": pa.array(
                    [
                        b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?"
                    ],
                    type=pa.binary(),
                ),
                "name": pa.array(["test"]),
            }
        )

        output_dir = tmp_path / "output"

        with (
            patch("geoparquet_io.core.wfs.wfs_to_table", return_value=mock_table),
            patch("geoparquet_io.core.wfs.configure_verbose"),
        ):
            results = convert_wfs_layers_to_directory(
                service_url="http://mock/wfs",
                typenames=["layer1", "layer2"],
                output_dir=str(output_dir),
                skip_hilbert=True,
                skip_bbox=True,
            )

        assert len(results) == 2
        assert "layer1" in results
        assert "layer2" in results
        assert output_dir.exists()
        assert (output_dir / "layer1.parquet").exists()
        assert (output_dir / "layer2.parquet").exists()

    def test_convert_wfs_layers_parallel_mode(self, tmp_path):
        """Multi-layer extraction with parallel_layers > 1 should use ThreadPoolExecutor."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import convert_wfs_layers_to_directory

        mock_table = pa.table(
            {
                "geometry": pa.array(
                    [
                        b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?"
                    ],
                    type=pa.binary(),
                ),
                "name": pa.array(["test"]),
            }
        )

        output_dir = tmp_path / "output"

        with (
            patch("geoparquet_io.core.wfs.wfs_to_table", return_value=mock_table),
            patch("geoparquet_io.core.wfs.configure_verbose"),
        ):
            results = convert_wfs_layers_to_directory(
                service_url="http://mock/wfs",
                typenames=["layer1", "layer2", "layer3"],
                output_dir=str(output_dir),
                parallel_layers=3,
                skip_hilbert=True,
                skip_bbox=True,
            )

        assert len(results) == 3

    def test_cli_parses_comma_separated_typenames(self, tmp_path):
        """CLI should parse comma-separated typenames for multi-layer mode."""
        from click.testing import CliRunner

        from geoparquet_io.cli.main import cli

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        runner = CliRunner()

        # Just test that the CLI parses correctly - don't actually extract
        # Patch at the import location inside the function
        with patch("geoparquet_io.core.wfs.convert_wfs_layers_to_directory") as mock_extract:
            result = runner.invoke(
                cli,
                [
                    "extract",
                    "wfs",
                    "http://mock/wfs",
                    "layer1,layer2,layer3",
                    str(output_dir),
                    "--skip-hilbert",
                    "--skip-bbox",
                ],
            )

        # Should have called multi-layer function with parsed typenames
        assert result.exit_code == 0
        mock_extract.assert_called_once()
        call_kwargs = mock_extract.call_args[1]
        assert call_kwargs["typenames"] == ["layer1", "layer2", "layer3"]

    def test_cli_rejects_empty_typenames(self, tmp_path):
        """CLI should error (not IndexError) when typename parses to an empty list."""
        from click.testing import CliRunner

        from geoparquet_io.cli.main import cli

        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "extract",
                "wfs",
                "http://mock/wfs",
                ", ,",  # whitespace/comma-only -> no valid typenames
                str(tmp_path / "out.parquet"),
            ],
        )

        assert result.exit_code != 0
        assert "No valid typename" in result.output

    def test_sanitizes_typename_for_filename(self, tmp_path):
        """Typenames with colons should be sanitized for filenames."""
        import pyarrow as pa

        from geoparquet_io.core.wfs import convert_wfs_layers_to_directory

        mock_table = pa.table(
            {
                "geometry": pa.array(
                    [
                        b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\xf0?"
                    ],
                    type=pa.binary(),
                ),
                "name": pa.array(["test"]),
            }
        )

        output_dir = tmp_path / "output"

        with (
            patch("geoparquet_io.core.wfs.wfs_to_table", return_value=mock_table),
            patch("geoparquet_io.core.wfs.configure_verbose"),
        ):
            convert_wfs_layers_to_directory(
                service_url="http://mock/wfs",
                typenames=["ns:layer_name"],
                output_dir=str(output_dir),
                skip_hilbert=True,
                skip_bbox=True,
            )

        # Colons should be replaced with underscores in filename
        assert (output_dir / "ns_layer_name.parquet").exists()
