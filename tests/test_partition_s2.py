"""Tests for partition_by_s2 module."""

import tempfile
import uuid
from pathlib import Path

import pytest
from click.testing import CliRunner

from geoparquet_io.core.partition.by_s2 import partition_by_s2
from tests.conftest import safe_unlink

# Mark all tests as network-dependent (requires DuckDB community extension)
pytestmark = pytest.mark.network


class TestPartitionByS2:
    """Tests for partition_by_s2 function."""

    @pytest.fixture
    def places_file(self):
        """Return path to the places test file."""
        return str(Path(__file__).parent / "data" / "places_test.parquet")

    @pytest.fixture
    def output_folder(self):
        """Create a temp output folder path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_partition_s2_{uuid.uuid4()}"
        yield str(tmp_path)
        # Cleanup
        if tmp_path.exists():
            import shutil

            shutil.rmtree(tmp_path)

    def test_partition_basic(self, places_file, output_folder):
        """Test basic partitioning by S2."""
        partition_by_s2(places_file, output_folder, level=10, verbose=False, force=True)
        assert Path(output_folder).exists()
        # Check that partition files were created
        parquet_files = list(Path(output_folder).glob("*.parquet"))
        assert len(parquet_files) > 0

    def test_partition_with_existing_column(self, places_file, output_folder):
        """Test partitioning when S2 column already exists."""
        # First add S2 column
        from geoparquet_io.core.add.s2 import add_s2_column

        temp_file = str(Path(tempfile.gettempdir()) / f"with_s2_{uuid.uuid4()}.parquet")
        try:
            add_s2_column(places_file, temp_file, s2_level=10, verbose=False)

            # Now partition
            partition_by_s2(temp_file, output_folder, level=10, verbose=False, force=True)
            assert Path(output_folder).exists()
            parquet_files = list(Path(output_folder).glob("*.parquet"))
            assert len(parquet_files) > 0
        finally:
            safe_unlink(Path(temp_file))

    def test_partition_custom_column_name(self, places_file, output_folder):
        """Test partitioning with custom S2 column name."""
        partition_by_s2(
            places_file,
            output_folder,
            s2_column_name="custom_s2",
            level=10,
            verbose=False,
            force=True,
        )
        assert Path(output_folder).exists()
        parquet_files = list(Path(output_folder).glob("*.parquet"))
        assert len(parquet_files) > 0

    def test_partition_hive_style(self, places_file, output_folder):
        """Test Hive-style partitioning."""
        partition_by_s2(places_file, output_folder, level=10, hive=True, verbose=False, force=True)
        assert Path(output_folder).exists()
        # Check for Hive-style directories (s2_cell=value format)
        subdirs = [d for d in Path(output_folder).iterdir() if d.is_dir()]
        assert len(subdirs) > 0
        # Check that subdirectory names contain "s2_cell="
        assert any("s2_cell=" in d.name for d in subdirs)

    def test_partition_preview(self, places_file, output_folder):
        """Test preview mode without creating files."""
        partition_by_s2(places_file, output_folder, level=10, preview=True, verbose=False)
        # Output folder should not be created in preview mode
        assert not Path(output_folder).exists()

    def test_partition_invalid_level(self, places_file, output_folder):
        """Test error with invalid level."""
        import click

        with pytest.raises(click.UsageError, match="level must be between"):
            partition_by_s2(places_file, output_folder, level=31)


class TestPartitionS2CLI:
    """Tests for partition s2 CLI command."""

    @pytest.fixture
    def places_file(self):
        """Return path to the places test file."""
        return str(Path(__file__).parent / "data" / "places_test.parquet")

    @pytest.fixture
    def output_folder(self):
        """Create a temp output folder path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_partition_s2_cli_{uuid.uuid4()}"
        yield str(tmp_path)
        # Cleanup
        if tmp_path.exists():
            import shutil

            shutil.rmtree(tmp_path)

    def test_partition_s2_cli_help(self):
        """Test that partition s2 command has help."""
        from geoparquet_io.cli.main import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["partition", "s2", "--help"])
        assert result.exit_code == 0
        assert "s2" in result.output.lower()

    def test_partition_s2_cli_basic(self, places_file, output_folder):
        """Test basic CLI invocation."""
        from geoparquet_io.cli.main import cli

        runner = CliRunner()
        result = runner.invoke(
            cli, ["partition", "s2", places_file, output_folder, "--level", "10", "--force"]
        )
        assert result.exit_code == 0
        assert Path(output_folder).exists()
        parquet_files = list(Path(output_folder).glob("*.parquet"))
        assert len(parquet_files) > 0

    def test_partition_s2_cli_preview(self, places_file, output_folder):
        """Test preview mode."""
        from geoparquet_io.cli.main import cli

        runner = CliRunner()
        result = runner.invoke(
            cli, ["partition", "s2", places_file, output_folder, "--level", "10", "--preview"]
        )
        assert result.exit_code == 0
        # Output folder should not be created in preview mode
        assert not Path(output_folder).exists()
