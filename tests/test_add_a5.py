"""Tests for add_a5_column module."""

import io
import sys
import tempfile
import uuid
from pathlib import Path
from unittest import mock

import pyarrow.ipc as ipc
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.core.add.a5 import add_a5_column, add_a5_table
from tests.conftest import safe_unlink

# Mark all tests as network-dependent (requires DuckDB community extension)
pytestmark = pytest.mark.network


class TestAddA5Table:
    """Tests for add_a5_table function."""

    @pytest.fixture
    def places_file(self):
        """Return path to the places test file."""
        return str(Path(__file__).parent / "data" / "places_test.parquet")

    @pytest.fixture
    def sample_table(self, places_file):
        """Create a sample table from places test data."""
        return pq.read_table(places_file)

    def test_add_a5_basic(self, sample_table):
        """Test basic A5 column addition."""
        result = add_a5_table(sample_table, resolution=15)
        assert "a5_cell" in result.column_names
        assert result.num_rows == sample_table.num_rows

    def test_add_a5_custom_column_name(self, sample_table):
        """Test with custom column name."""
        result = add_a5_table(sample_table, a5_column_name="my_a5", resolution=15)
        assert "my_a5" in result.column_names
        assert result.num_rows == sample_table.num_rows

    def test_add_a5_different_resolutions(self, sample_table):
        """Test different resolution levels."""
        for resolution in [5, 15, 25]:
            result = add_a5_table(sample_table, resolution=resolution)
            assert "a5_cell" in result.column_names
            assert result.num_rows == sample_table.num_rows

    def test_add_a5_invalid_resolution_low(self, sample_table):
        """Test error with resolution too low."""
        with pytest.raises(ValueError, match="resolution must be between"):
            add_a5_table(sample_table, resolution=-1)

    def test_add_a5_invalid_resolution_high(self, sample_table):
        """Test error with resolution too high."""
        with pytest.raises(ValueError, match="resolution must be between"):
            add_a5_table(sample_table, resolution=31)

    def test_add_a5_metadata_preserved(self, sample_table):
        """Test that GeoParquet metadata is preserved."""
        result = add_a5_table(sample_table, resolution=15)
        # Check that geo metadata is preserved
        if sample_table.schema.metadata and b"geo" in sample_table.schema.metadata:
            assert b"geo" in result.schema.metadata


class TestAddA5File:
    """Tests for file-based add_a5_column function."""

    @pytest.fixture
    def places_file(self):
        """Return path to the places test file."""
        return str(Path(__file__).parent / "data" / "places_test.parquet")

    @pytest.fixture
    def output_file(self):
        """Create a temp output file path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_add_a5_{uuid.uuid4()}.parquet"
        yield str(tmp_path)
        safe_unlink(tmp_path)

    def test_add_a5_file_basic(self, places_file, output_file):
        """Test basic file-to-file A5 addition."""
        add_a5_column(places_file, output_file, a5_resolution=15)
        assert Path(output_file).exists()
        result = pq.read_table(output_file)
        assert "a5_cell" in result.column_names
        assert result.num_rows == 766

    def test_add_a5_file_custom_name(self, places_file, output_file):
        """Test with custom column name."""
        add_a5_column(places_file, output_file, a5_column_name="custom_a5", a5_resolution=15)
        assert Path(output_file).exists()
        result = pq.read_table(output_file)
        assert "custom_a5" in result.column_names


class TestAddA5Streaming:
    """Tests for streaming mode."""

    @pytest.fixture
    def places_file(self):
        """Return path to the places test file."""
        return str(Path(__file__).parent / "data" / "places_test.parquet")

    @pytest.fixture
    def sample_geo_table(self, places_file):
        """Create a geo table from test data."""
        return pq.read_table(places_file)

    @pytest.fixture
    def output_file(self):
        """Create a temp output file path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_add_a5_stream_{uuid.uuid4()}.parquet"
        yield str(tmp_path)
        safe_unlink(tmp_path)

    def test_stdin_to_file(self, sample_geo_table, output_file, monkeypatch):
        """Test reading from mocked stdin."""
        # Create IPC buffer
        ipc_buffer = io.BytesIO()
        writer = ipc.RecordBatchStreamWriter(ipc_buffer, sample_geo_table.schema)
        writer.write_table(sample_geo_table)
        writer.close()
        ipc_buffer.seek(0)

        # Create a mock stdin with buffer attribute
        mock_stdin = mock.MagicMock()
        mock_stdin.isatty.return_value = False
        mock_stdin.buffer = ipc_buffer

        monkeypatch.setattr(sys, "stdin", mock_stdin)

        # Call function with "-" input
        add_a5_column("-", output_file, a5_resolution=15)

        # Verify output
        assert Path(output_file).exists()
        result = pq.read_table(output_file)
        assert "a5_cell" in result.column_names
        assert result.num_rows == sample_geo_table.num_rows

    def test_file_to_stdout(self, places_file, monkeypatch):
        """Test writing to mocked stdout."""
        output_buffer = io.BytesIO()
        mock_stdout = mock.MagicMock()
        mock_stdout.buffer = output_buffer
        mock_stdout.isatty.return_value = False
        monkeypatch.setattr(sys, "stdout", mock_stdout)

        # Call function with "-" output
        add_a5_column(places_file, "-", a5_resolution=15)

        # Verify stream
        output_buffer.seek(0)
        reader = ipc.RecordBatchStreamReader(output_buffer)
        result = reader.read_all()
        assert result.num_rows > 0
        assert "a5_cell" in result.column_names


class TestAddA5CLI:
    """Tests for add a5 CLI command."""

    @pytest.fixture
    def places_file(self):
        """Return path to the places test file."""
        return str(Path(__file__).parent / "data" / "places_test.parquet")

    @pytest.fixture
    def output_file(self):
        """Create a temp output file path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_add_a5_cli_{uuid.uuid4()}.parquet"
        yield str(tmp_path)
        safe_unlink(tmp_path)

    def test_add_a5_cli_help(self):
        """Test that add a5 command has help."""
        from geoparquet_io.cli.main import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["add", "a5", "--help"])
        assert result.exit_code == 0
        assert "a5" in result.output.lower()

    def test_add_a5_cli_basic(self, places_file, output_file):
        """Test basic CLI invocation."""
        from geoparquet_io.cli.main import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["add", "a5", places_file, output_file, "--resolution", "15"])
        assert result.exit_code == 0
        assert Path(output_file).exists()
        loaded = pq.read_table(output_file)
        assert "a5_cell" in loaded.column_names
