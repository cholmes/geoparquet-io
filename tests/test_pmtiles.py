"""Tests for PMTiles generation module."""

import shutil
import sys
from pathlib import Path

import pytest


def has_tippecanoe():
    """Check if tippecanoe is available."""
    return shutil.which("tippecanoe") is not None


def has_gpio():
    """Check if gpio is available."""
    return shutil.which("gpio") is not None


# Skip integration tests on Windows (tippecanoe has no native Windows build)
skip_windows = pytest.mark.skipif(
    sys.platform == "win32",
    reason="tippecanoe not available on Windows",
)


class TestTippecanoeNotFoundError:
    """Tests for TippecanoeNotFoundError exception."""

    def test_error_message_content(self):
        """Test error message contains installation instructions."""
        from geoparquet_io.core.pmtiles import TippecanoeNotFoundError

        error = TippecanoeNotFoundError()
        error_msg = str(error)

        assert "tippecanoe not found" in error_msg
        assert "brew install tippecanoe" in error_msg
        assert "sudo apt install tippecanoe" in error_msg


class TestGpioExecutableDetection:
    """Tests for gpio executable detection."""

    def test_returns_string(self):
        """Test that gpio executable detection returns a string."""
        from geoparquet_io.core.pmtiles import _get_gpio_executable

        gpio_exe = _get_gpio_executable()
        assert gpio_exe is not None
        assert isinstance(gpio_exe, str)
        assert len(gpio_exe) > 0


class TestBuildGpioCommands:
    """Tests for gpio command building."""

    def test_simple_command(self):
        """Test building simple gpio convert command."""
        from geoparquet_io.core.pmtiles import _build_gpio_commands

        commands = _build_gpio_commands(
            input_path="input.parquet",
            bbox=None,
            where=None,
            include_cols=None,
            precision=6,
            verbose=False,
            profile=None,
            src_crs=None,
        )

        assert len(commands) == 1
        assert "convert" in commands[0]
        assert "geojson" in commands[0]
        assert "input.parquet" in commands[0]
        assert "--precision" in commands[0]
        assert "6" in commands[0]

    def test_with_filters(self):
        """Test building gpio commands with filters."""
        from geoparquet_io.core.pmtiles import _build_gpio_commands

        commands = _build_gpio_commands(
            input_path="input.parquet",
            bbox="-122,37,-121,38",
            where="population > 1000",
            include_cols="name,type",
            precision=5,
            verbose=True,
            profile="my-profile",
            src_crs=None,
        )

        assert len(commands) == 2

        extract_cmd = commands[0]
        assert "extract" in extract_cmd
        assert "input.parquet" in extract_cmd
        assert "--bbox" in extract_cmd
        assert "-122,37,-121,38" in extract_cmd
        assert "--where" in extract_cmd
        assert "population > 1000" in extract_cmd
        assert "--include-cols" in extract_cmd
        assert "name,type" in extract_cmd
        assert "--verbose" in extract_cmd
        assert "--profile" in extract_cmd
        assert "my-profile" in extract_cmd

        convert_cmd = commands[1]
        assert "convert" in convert_cmd
        assert "geojson" in convert_cmd
        assert "-" in convert_cmd

    def test_with_reprojection(self):
        """Test building gpio commands with CRS reprojection."""
        from geoparquet_io.core.pmtiles import _build_gpio_commands

        commands = _build_gpio_commands(
            input_path="input.parquet",
            bbox=None,
            where=None,
            include_cols=None,
            precision=6,
            verbose=True,
            profile="my-profile",
            src_crs="EPSG:3857",
        )

        assert len(commands) == 2

        reproject_cmd = commands[0]
        assert "convert" in reproject_cmd
        assert "reproject" in reproject_cmd
        assert "input.parquet" in reproject_cmd
        assert "--dst-crs" in reproject_cmd
        assert "EPSG:4326" in reproject_cmd
        assert "--src-crs" in reproject_cmd
        assert "EPSG:3857" in reproject_cmd

        convert_cmd = commands[1]
        assert "convert" in convert_cmd
        assert "geojson" in convert_cmd
        assert "-" in convert_cmd

    def test_with_reprojection_and_filters(self):
        """Test building gpio commands with both reprojection and filters."""
        from geoparquet_io.core.pmtiles import _build_gpio_commands

        commands = _build_gpio_commands(
            input_path="input.parquet",
            bbox="-122,37,-121,38",
            where="type = 'building'",
            include_cols="name,height",
            precision=6,
            verbose=False,
            profile=None,
            src_crs="EPSG:3857",
        )

        assert len(commands) == 3

        reproject_cmd = commands[0]
        assert "convert" in reproject_cmd
        assert "reproject" in reproject_cmd
        assert "input.parquet" in reproject_cmd
        assert "--src-crs" in reproject_cmd
        assert "EPSG:3857" in reproject_cmd

        extract_cmd = commands[1]
        assert "extract" in extract_cmd
        assert "geoparquet" in extract_cmd
        assert "-" in extract_cmd
        assert "--bbox" in extract_cmd
        assert "--where" in extract_cmd
        assert "--include-cols" in extract_cmd

        convert_cmd = commands[2]
        assert "convert" in convert_cmd
        assert "geojson" in convert_cmd
        assert "-" in convert_cmd


class TestBuildTippecanoeCommand:
    """Tests for tippecanoe command building."""

    def test_basic_command(self):
        """Test building basic tippecanoe command."""
        from geoparquet_io.core.pmtiles import _build_tippecanoe_command

        cmd = _build_tippecanoe_command(
            output_path="output.pmtiles",
            layer="test_layer",
            min_zoom=None,
            max_zoom=None,
            verbose=False,
            attribution=None,
        )

        assert "tippecanoe" in cmd
        assert "-P" in cmd
        assert "-o" in cmd
        assert "output.pmtiles" in cmd
        assert "-l" in cmd
        assert "test_layer" in cmd
        assert "-zg" in cmd
        assert "--drop-densest-as-needed" in cmd

    def test_with_zoom_levels(self):
        """Test building tippecanoe command with explicit zoom levels."""
        from geoparquet_io.core.pmtiles import _build_tippecanoe_command

        cmd = _build_tippecanoe_command(
            output_path="output.pmtiles",
            layer="test_layer",
            min_zoom=0,
            max_zoom=14,
            verbose=True,
            attribution=None,
        )

        assert "-Z" in cmd
        assert "0" in cmd
        assert "-z" in cmd
        assert "14" in cmd
        assert "-zg" not in cmd
        assert "--progress-interval=1" in cmd

    def test_default_attribution(self):
        """Test that default attribution is included."""
        from geoparquet_io.core.pmtiles import _build_tippecanoe_command

        cmd = _build_tippecanoe_command(
            output_path="output.pmtiles",
            layer="test_layer",
            min_zoom=None,
            max_zoom=None,
            verbose=False,
            attribution=None,
        )

        assert any("--attribution=" in arg for arg in cmd)
        assert any("geoparquet.io" in arg for arg in cmd)

    def test_custom_attribution(self):
        """Test building tippecanoe command with custom attribution."""
        from geoparquet_io.core.pmtiles import _build_tippecanoe_command

        custom_attr = '<a href="https://example.com/">&copy; Example</a>'
        cmd = _build_tippecanoe_command(
            output_path="output.pmtiles",
            layer="test_layer",
            min_zoom=None,
            max_zoom=11,
            verbose=False,
            attribution=custom_attr,
        )

        assert any("--attribution=" in arg for arg in cmd)
        assert any("example.com" in arg for arg in cmd)

    def test_production_quality_flags(self):
        """Test that production-quality flags are included."""
        from geoparquet_io.core.pmtiles import _build_tippecanoe_command

        cmd = _build_tippecanoe_command(
            output_path="output.pmtiles",
            layer="test_layer",
            min_zoom=None,
            max_zoom=11,
            verbose=False,
            attribution=None,
        )

        assert "-P" in cmd
        assert "--simplify-only-low-zooms" in cmd
        assert "--no-simplification-of-shared-nodes" in cmd
        assert "--no-tile-size-limit" in cmd
        assert "--drop-densest-as-needed" in cmd

    def test_max_zoom_only(self):
        """Test that -z is used for max zoom without min zoom."""
        from geoparquet_io.core.pmtiles import _build_tippecanoe_command

        cmd = _build_tippecanoe_command(
            output_path="output.pmtiles",
            layer="test_layer",
            min_zoom=None,
            max_zoom=11,
            verbose=False,
            attribution=None,
        )

        assert "-z" in cmd
        assert "11" in cmd
        assert cmd.count("-Z") == 0

    def test_min_zoom_only(self):
        """Test that -Z and -zg are used for min zoom without max zoom."""
        from geoparquet_io.core.pmtiles import _build_tippecanoe_command

        cmd = _build_tippecanoe_command(
            output_path="output.pmtiles",
            layer="test_layer",
            min_zoom=5,
            max_zoom=None,
            verbose=False,
            attribution=None,
        )

        assert "-Z" in cmd
        assert "5" in cmd
        assert "-zg" in cmd
        assert cmd.count("-z") == 0  # lowercase -z should not be present


class TestPathValidation:
    """Tests for path validation security."""

    def test_valid_paths(self):
        """Test path validation with valid paths."""
        from geoparquet_io.core.pmtiles import _validate_path

        _validate_path("/path/to/file.parquet")
        _validate_path("relative/path.parquet")
        _validate_path("file_with_underscores.parquet")
        _validate_path("file-with-dashes.parquet")
        _validate_path("file.with.dots.parquet")
        _validate_path("/path with spaces/file.parquet")

    def test_rejects_shell_injection(self):
        """Test that path validation rejects shell metacharacters."""
        from geoparquet_io.core.pmtiles import _validate_path

        # Build dangerous paths without raw shell metacharacters in source
        backtick = chr(96)
        newline = chr(10)
        carriage_return = chr(13)
        dangerous_paths = [
            "file.parquet; rm -rf /",
            "file.parquet | cat",
            "file.parquet && echo pwned",
            "file.parquet" + "$" + "malicious",
            "file.parquet" + backtick + "whoami" + backtick,
            "file.parquet" + newline + "rm -rf /",
            "file.parquet" + carriage_return + "rm -rf /",
        ]

        for path in dangerous_paths:
            with pytest.raises(ValueError, match="dangerous character"):
                _validate_path(path)

    def test_create_pmtiles_rejects_dangerous_input_path(self):
        """Test that create_pmtiles rejects input paths with shell metacharacters."""
        from geoparquet_io.core.pmtiles import create_pmtiles_from_geoparquet

        with pytest.raises(ValueError, match="dangerous character"):
            create_pmtiles_from_geoparquet(
                input_path="input.parquet; rm -rf /",
                output_path="output.pmtiles",
            )

    def test_create_pmtiles_rejects_dangerous_output_path(self):
        """Test that create_pmtiles rejects output paths with shell metacharacters."""
        from geoparquet_io.core.pmtiles import create_pmtiles_from_geoparquet

        with pytest.raises(ValueError, match="dangerous character"):
            create_pmtiles_from_geoparquet(
                input_path="input.parquet",
                output_path="output.pmtiles | cat",
            )


class TestPMTilesIntegration:
    """Integration tests for PMTiles creation (require tippecanoe)."""

    @skip_windows
    @pytest.mark.skipif(not has_gpio(), reason="gpio not installed")
    @pytest.mark.skipif(not has_tippecanoe(), reason="tippecanoe not installed")
    @pytest.mark.slow
    def test_basic_creation(self, tmp_path):
        """Test basic PMTiles creation from test data."""
        test_data_dir = Path(__file__).parent / "data"
        input_file = test_data_dir / "places_test.parquet"

        if not input_file.exists():
            pytest.skip(f"Test file not found: {input_file}")

        output_file = tmp_path / "output.pmtiles"

        from geoparquet_io.core.pmtiles import create_pmtiles_from_geoparquet

        create_pmtiles_from_geoparquet(
            input_path=str(input_file),
            output_path=str(output_file),
            layer="places",
            verbose=True,
        )

        assert output_file.exists()
        assert output_file.stat().st_size > 0

    @skip_windows
    @pytest.mark.skipif(not has_gpio(), reason="gpio not installed")
    @pytest.mark.skipif(not has_tippecanoe(), reason="tippecanoe not installed")
    @pytest.mark.slow
    def test_with_filters(self, tmp_path):
        """Test PMTiles creation with filtering options."""
        test_data_dir = Path(__file__).parent / "data"
        input_file = test_data_dir / "places_test.parquet"

        if not input_file.exists():
            pytest.skip(f"Test file not found: {input_file}")

        output_file = tmp_path / "filtered.pmtiles"

        from geoparquet_io.core.pmtiles import create_pmtiles_from_geoparquet

        create_pmtiles_from_geoparquet(
            input_path=str(input_file),
            output_path=str(output_file),
            layer="filtered_places",
            bbox="-180,-90,180,90",
            precision=5,
            verbose=True,
        )

        assert output_file.exists()
        assert output_file.stat().st_size > 0

    @skip_windows
    @pytest.mark.skipif(not has_gpio(), reason="gpio not installed")
    @pytest.mark.skipif(not has_tippecanoe(), reason="tippecanoe not installed")
    @pytest.mark.slow
    def test_with_zoom_levels(self, tmp_path):
        """Test PMTiles creation with explicit zoom levels."""
        test_data_dir = Path(__file__).parent / "data"
        input_file = test_data_dir / "places_test.parquet"

        if not input_file.exists():
            pytest.skip(f"Test file not found: {input_file}")

        output_file = tmp_path / "zoomed.pmtiles"

        from geoparquet_io.core.pmtiles import create_pmtiles_from_geoparquet

        create_pmtiles_from_geoparquet(
            input_path=str(input_file),
            output_path=str(output_file),
            layer="zoomed_places",
            min_zoom=0,
            max_zoom=10,
            verbose=True,
        )

        assert output_file.exists()
        assert output_file.stat().st_size > 0


class TestRunPipelineErrorSurfacing:
    """Pipeline errors must surface upstream stderr.

    Regression for issue #421: a failing upstream gpio process had its stderr
    captured to PIPE, drained, then discarded — the raised RuntimeError only
    contained the exit code, leaving users debugging blind.
    """

    @pytest.mark.skipif(sys.platform == "win32", reason="needs POSIX shell")
    def test_pipeline_error_includes_upstream_stderr(self):
        from geoparquet_io.core.pmtiles import _run_pipeline

        sentinel = "GPIO_UPSTREAM_BOOM_a3f9"
        with pytest.raises(RuntimeError) as exc_info:
            _run_pipeline(
                gpio_commands=[
                    ["sh", "-c", f"echo {sentinel} >&2; exit 7"],
                ],
                tippecanoe_cmd=["cat"],
                verbose=False,
            )
        msg = str(exc_info.value)
        assert "exit code 7" in msg
        assert sentinel in msg, f"upstream stderr '{sentinel}' not surfaced in error: {msg!r}"
