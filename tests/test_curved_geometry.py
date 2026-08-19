"""Tests for non-linear geometry detection and error reporting (issue #643)."""

from pathlib import Path

import pytest

from geoparquet_io.core.curved_geometry import (
    find_non_linear_gpkg_types,
    unsupported_wkb_error_message,
)
from geoparquet_io.core.duckdb_metadata import GeoParquetError

TEST_DATA_DIR = Path(__file__).parent / "data"
CURVED_GPKG = TEST_DATA_DIR / "curved_geometry_test.gpkg"
LINEAR_GPKG = TEST_DATA_DIR / "buildings_test.gpkg"


class TestFindNonLinearGpkgTypes:
    def test_detects_curvepolygon(self):
        assert find_non_linear_gpkg_types(CURVED_GPKG) == ["CURVEPOLYGON"]

    def test_linear_gpkg_is_clean(self):
        if not LINEAR_GPKG.exists():
            pytest.skip("buildings_test.gpkg not available")
        assert find_non_linear_gpkg_types(LINEAR_GPKG) == []

    def test_unreadable_file_returns_empty(self, tmp_path):
        bogus = tmp_path / "not_a.gpkg"
        bogus.write_text("plain text")
        assert find_non_linear_gpkg_types(bogus) == []


class TestUnsupportedWkbError:
    def test_convert_raises_actionable_error(self):
        import geoparquet_io as gpio

        with pytest.raises(GeoParquetError) as exc:
            gpio.convert(str(CURVED_GPKG))
        message = str(exc.value)
        assert "CURVEPOLYGON" in message
        assert "CONVERT_TO_LINEAR" in message

    def test_message_for_non_gpkg_falls_back(self):
        message = unsupported_wkb_error_message(
            "data.fgb", None, "Invalid Input Error: Unsupported geometry type in WKB"
        )
        assert "non-linear geometries" in message
        assert "CONVERT_TO_LINEAR" in message
        assert "Unsupported geometry type in WKB" in message
