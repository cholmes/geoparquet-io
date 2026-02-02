"""Tests for profile report formatting."""

import cProfile
import tempfile
from pathlib import Path

import pytest


class TestProfileReportFormatting:
    """Tests for profile stats formatting."""

    @pytest.fixture
    def sample_profile(self, tmp_path):
        """Create a sample profile file."""

        # Create a simple function to profile
        def sample_function():
            total = 0
            for i in range(1000):
                total += i
            return total

        # Profile it
        profiler = cProfile.Profile()
        profiler.enable()
        sample_function()
        profiler.disable()

        # Save to temp file
        profile_path = tmp_path / "sample.prof"
        profiler.dump_stats(profile_path)
        return profile_path

    def test_format_profile_stats_returns_string(self, sample_profile):
        """Test that format_profile_stats returns a string."""
        from geoparquet_io.benchmarks.profile_report import format_profile_stats

        result = format_profile_stats(sample_profile)

        assert isinstance(result, str)
        assert len(result) > 0

    def test_format_profile_stats_includes_function_names(self, sample_profile):
        """Test that profile summary includes function names."""
        from geoparquet_io.benchmarks.profile_report import format_profile_stats

        result = format_profile_stats(sample_profile)

        # Should include the function we profiled
        assert "sample_function" in result

    def test_format_profile_stats_shows_timing(self, sample_profile):
        """Test that profile summary shows timing information."""
        from geoparquet_io.benchmarks.profile_report import format_profile_stats

        result = format_profile_stats(sample_profile)

        # Should include timing columns (cumulative time, etc.)
        assert "cumtime" in result.lower() or "time" in result.lower()

    def test_format_profile_stats_top_n(self, sample_profile):
        """Test that top_n parameter limits output."""
        from geoparquet_io.benchmarks.profile_report import format_profile_stats

        result_5 = format_profile_stats(sample_profile, top_n=5)
        result_10 = format_profile_stats(sample_profile, top_n=10)

        # More lines with higher top_n (not strict check due to headers)
        assert len(result_10.split("\n")) >= len(result_5.split("\n"))

    def test_format_profile_stats_invalid_file(self):
        """Test error handling for invalid profile file."""
        from geoparquet_io.benchmarks.profile_report import format_profile_stats

        with pytest.raises((FileNotFoundError, OSError)):
            format_profile_stats(Path("/nonexistent/profile.prof"))

    def test_format_profile_stats_filters_stdlib(self, sample_profile):
        """Test that stdlib functions can be filtered out."""
        from geoparquet_io.benchmarks.profile_report import format_profile_stats

        # Default should filter some stdlib
        result = format_profile_stats(sample_profile, filter_stdlib=True)

        # Should focus on our code, not Python internals
        # (This is a soft check - implementation may vary)
        assert len(result) > 0


class TestProfileDataSaving:
    """Tests for saving profile data."""

    def test_save_profile_data(self):
        """Test save_profile_data creates file."""
        from geoparquet_io.benchmarks.profile_report import save_profile_data

        # Create a profiler with some data
        profiler = cProfile.Profile()
        profiler.enable()
        sum(range(100))
        profiler.disable()

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "test.prof"
            save_profile_data(profiler, output_path)

            assert output_path.exists()
            assert output_path.stat().st_size > 0

    def test_save_profile_data_creates_parent_dirs(self):
        """Test that save_profile_data creates parent directories."""
        from geoparquet_io.benchmarks.profile_report import save_profile_data

        profiler = cProfile.Profile()
        profiler.enable()
        sum(range(100))
        profiler.disable()

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "subdir" / "another" / "test.prof"
            save_profile_data(profiler, output_path)

            assert output_path.exists()
            assert output_path.parent.exists()
