"""
Tests for GeoJSON RFC 7946 CRS defaulting (fixes #323).

When a GeoJSON file has no detectable CRS metadata (common with remote files
on GCS/S3), the code should default to EPSG:4326 per RFC 7946 instead of
raising 'No CRS found'.
"""

from unittest.mock import patch

import click
import pytest

from geoparquet_io.core.convert import _determine_effective_crs, _is_geojson_file


class TestIsGeojsonFile:
    """Tests for _is_geojson_file() helper."""

    def test_geojson_extension(self):
        assert _is_geojson_file("data.geojson") is True

    def test_json_extension(self):
        assert _is_geojson_file("data.json") is True

    def test_gpkg_extension(self):
        assert _is_geojson_file("data.gpkg") is False

    def test_shp_extension(self):
        assert _is_geojson_file("data.shp") is False

    def test_csv_extension(self):
        assert _is_geojson_file("data.csv") is False

    def test_parquet_extension(self):
        assert _is_geojson_file("data.parquet") is False

    def test_gcs_url(self):
        assert _is_geojson_file("gs://my-bucket/path/data.geojson") is True

    def test_s3_url(self):
        assert _is_geojson_file("s3://my-bucket/path/data.geojson") is True

    def test_url_with_query_params(self):
        assert _is_geojson_file("gs://bucket/file.geojson?token=abc") is True

    def test_case_insensitive(self):
        assert _is_geojson_file("data.GeoJSON") is True


class TestGeojsonCrsDefault:
    """Tests for GeoJSON CRS defaulting in _determine_effective_crs()."""

    @patch("geoparquet_io.core.convert.detect_crs_from_spatial_file", return_value=None)
    def test_geojson_defaults_to_wgs84_when_no_crs_detected(self, _mock_detect):
        """GeoJSON with no detected CRS should return None (WGS84 default), not raise."""
        result = _determine_effective_crs(
            input_file="gs://bucket/data.geojson",
            input_url="gs://bucket/data.geojson",
            crs="EPSG:4326",
            is_csv=False,
            is_parquet=False,
            con=None,
            verbose=False,
        )
        assert result is None

    @patch("geoparquet_io.core.convert.detect_crs_from_spatial_file", return_value=None)
    def test_non_geojson_still_raises_when_no_crs_detected(self, _mock_detect):
        """Non-GeoJSON spatial files should still raise when CRS is missing."""
        with pytest.raises(click.ClickException, match="No CRS found"):
            _determine_effective_crs(
                input_file="gs://bucket/data.gpkg",
                input_url="gs://bucket/data.gpkg",
                crs="EPSG:4326",
                is_csv=False,
                is_parquet=False,
                con=None,
                verbose=False,
            )

    @patch("geoparquet_io.core.convert.detect_crs_from_spatial_file", return_value=None)
    def test_geojson_verbose_logs_rfc7946(self, _mock_detect):
        """Verbose mode should log the RFC 7946 default."""
        with patch("geoparquet_io.core.convert.debug") as mock_debug:
            _determine_effective_crs(
                input_file="data.geojson",
                input_url="data.geojson",
                crs="EPSG:4326",
                is_csv=False,
                is_parquet=False,
                con=None,
                verbose=True,
            )
            mock_debug.assert_called_with(
                "GeoJSON file with no detected CRS, assuming WGS84 per RFC 7946"
            )
