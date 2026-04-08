"""Tests for hilbert_order helper functions."""

import logging
from pathlib import Path
from unittest.mock import patch

import pytest

from geoparquet_io.core.hilbert_order import _cleanup_temp_file


class TestCleanupTempFile:
    """Tests for _cleanup_temp_file function."""

    def test_cleanup_nonexistent_file(self, tmp_path):
        """Test cleanup with non-existent file."""
        nonexistent = str(tmp_path / "nonexistent.parquet")
        # Should not raise
        _cleanup_temp_file(nonexistent, verbose=False)

    def test_cleanup_none_file(self):
        """Test cleanup with None file."""
        # Should not raise
        _cleanup_temp_file(None, verbose=False)

    def test_cleanup_existing_file(self, tmp_path):
        """Test cleanup with existing file."""
        temp_file = tmp_path / "temp.parquet"
        temp_file.write_text("test content")
        assert temp_file.exists()

        _cleanup_temp_file(str(temp_file), verbose=False)

        assert not temp_file.exists()

    def test_cleanup_with_verbose(self, tmp_path, capsys):
        """Test cleanup with verbose output."""
        temp_file = tmp_path / "temp.parquet"
        temp_file.write_text("test content")

        _cleanup_temp_file(str(temp_file), verbose=True)

        assert not temp_file.exists()


class TestHilbertV11Warning:
    """Tests for v1.1 Hilbert sorting warning."""

    def test_warns_when_version_is_1_1(self, tmp_path, places_test_file):
        """Hilbert sorting to v1.1 should warn about no filter pushdown benefit."""
        from unittest.mock import patch

        output = str(tmp_path / "out.parquet")
        with patch("geoparquet_io.core.hilbert_order.warn") as mock_warn:
            from geoparquet_io.core.hilbert_order import hilbert_order

            hilbert_order(places_test_file, output, geoparquet_version="1.1")

        mock_warn.assert_any_call(
            "Hilbert sorting to GeoParquet v1.1 provides no spatial filter pushdown benefit. "
            "Consider using --geoparquet-version 2.0 to enable native geo_bbox row group statistics."
        )

    def test_warns_when_version_is_default_none(self, tmp_path, places_test_file):
        """Hilbert sorting with default version (None, resolves to 1.1) should warn."""
        from unittest.mock import patch

        output = str(tmp_path / "out.parquet")
        with patch("geoparquet_io.core.hilbert_order.warn") as mock_warn:
            from geoparquet_io.core.hilbert_order import hilbert_order

            hilbert_order(places_test_file, output, geoparquet_version=None)

        mock_warn.assert_any_call(
            "Hilbert sorting to GeoParquet v1.1 provides no spatial filter pushdown benefit. "
            "Consider using --geoparquet-version 2.0 to enable native geo_bbox row group statistics."
        )

    def test_no_warning_when_version_is_2_0(self, tmp_path, places_test_file):
        """Hilbert sorting to v2.0 should NOT warn."""
        from unittest.mock import patch

        output = str(tmp_path / "out.parquet")
        with patch("geoparquet_io.core.hilbert_order.warn") as mock_warn:
            from geoparquet_io.core.hilbert_order import hilbert_order

            hilbert_order(places_test_file, output, geoparquet_version="2.0")

        # Check that warn was never called with the v1.1 message
        for call in mock_warn.call_args_list:
            assert "no spatial filter pushdown" not in str(call)


class TestHilbertRgSizeGuidance:
    """Tests for row group size guidance when Hilbert sorting with v2.0."""

    @patch("geoparquet_io.core.hilbert_order._hilbert_order_file_based")
    def test_large_rg_with_v20_shows_guidance(self, mock_file_based, caplog):
        """Large RG size + v2.0 should show spatial guidance."""
        from geoparquet_io.core.hilbert_order import hilbert_order

        with caplog.at_level(logging.INFO):
            hilbert_order(
                "input.parquet",
                "output.parquet",
                row_group_rows=100000,
                geoparquet_version="2.0",
            )
        assert "10000" in caplog.text or "10,000" in caplog.text
        assert "50000" in caplog.text or "50,000" in caplog.text

    @patch("geoparquet_io.core.hilbert_order._hilbert_order_file_based")
    def test_large_rg_with_pgo_shows_guidance(self, mock_file_based, caplog):
        """Large RG size + parquet-geo-only should show spatial guidance."""
        from geoparquet_io.core.hilbert_order import hilbert_order

        with caplog.at_level(logging.INFO):
            hilbert_order(
                "input.parquet",
                "output.parquet",
                row_group_rows=60000,
                geoparquet_version="parquet-geo-only",
            )
        assert "row group" in caplog.text.lower() or "row-group" in caplog.text.lower()

    @patch("geoparquet_io.core.hilbert_order._hilbert_order_file_based")
    def test_small_rg_with_v20_no_guidance(self, mock_file_based, caplog):
        """Small RG size + v2.0 should NOT show guidance."""
        from geoparquet_io.core.hilbert_order import hilbert_order

        with caplog.at_level(logging.INFO):
            hilbert_order(
                "input.parquet",
                "output.parquet",
                row_group_rows=10000,
                geoparquet_version="2.0",
            )
        assert "spatial filter pushdown" not in caplog.text

    @patch("geoparquet_io.core.hilbert_order._hilbert_order_file_based")
    def test_large_rg_with_v11_no_guidance(self, mock_file_based, caplog):
        """Large RG size + v1.1 should NOT show RG guidance (v1.1 has no geo stats anyway)."""
        from geoparquet_io.core.hilbert_order import hilbert_order

        with caplog.at_level(logging.INFO):
            hilbert_order(
                "input.parquet",
                "output.parquet",
                row_group_rows=100000,
                geoparquet_version="1.1",
            )
        # Should NOT show the row group size guidance (check for specific phrase)
        # Note: v1.1 warning about "no spatial filter pushdown benefit" IS expected
        assert "Smaller row groups" not in caplog.text

    @patch("geoparquet_io.core.hilbert_order._hilbert_order_file_based")
    def test_no_rg_rows_specified_no_guidance(self, mock_file_based, caplog):
        """No row_group_rows specified should NOT show guidance."""
        from geoparquet_io.core.hilbert_order import hilbert_order

        with caplog.at_level(logging.INFO):
            hilbert_order(
                "input.parquet",
                "output.parquet",
                geoparquet_version="2.0",
            )
        assert "spatial filter pushdown" not in caplog.text


class TestHilbertOrderIntegration:
    """Integration tests for hilbert_order."""

    @pytest.fixture
    def sample_file(self):
        """Return path to the sample file."""
        return str(Path(__file__).parent / "data" / "sample.parquet")

    def test_hilbert_order_help(self):
        """Test that hilbert sort command has help."""
        from click.testing import CliRunner

        from geoparquet_io.cli.main import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["sort", "hilbert", "--help"])
        assert result.exit_code == 0
        assert "hilbert" in result.output.lower()
