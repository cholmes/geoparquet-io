"""Tests for Overture Maps latest release fetching."""

from unittest.mock import patch

from geoparquet_io.core.overture import get_latest_overture_release, get_overture_divisions_url


class TestGetLatestOvertureRelease:
    """Test fetching the latest Overture Maps release."""

    def test_returns_version_string(self):
        with patch("geoparquet_io.core.overture._fetch_latest_release") as mock:
            mock.return_value = "2026-05-20.0"
            version = get_latest_overture_release()
            assert version == "2026-05-20.0"

    def test_falls_back_on_failure(self):
        with patch("geoparquet_io.core.overture._fetch_latest_release") as mock:
            mock.side_effect = Exception("network error")
            version = get_latest_overture_release()
            assert version is not None  # should return a fallback


class TestGetOvertureDivisionsUrl:
    """Test building the Overture divisions URL."""

    def test_url_contains_release(self):
        url = get_overture_divisions_url(release="2026-05-20.0")
        assert "2026-05-20.0" in url
        assert "theme=divisions" in url
        assert "type=division_area" in url

    def test_url_uses_latest_by_default(self):
        import geoparquet_io.core.overture as overture_mod

        overture_mod._cached_release = None
        with patch("geoparquet_io.core.overture._fetch_latest_release") as mock:
            mock.return_value = "2099-01-01.0"
            url = get_overture_divisions_url()
            assert "2099-01-01.0" in url
        overture_mod._cached_release = None
