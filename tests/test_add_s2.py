"""Tests for add_s2_column module."""

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

from geoparquet_io.core.add.s2 import add_s2_column, add_s2_table
from tests.conftest import safe_unlink, skip_if_geography_unavailable


class TestAddS2Table:
    """Tests for add_s2_table function."""

    @pytest.fixture
    def places_file(self):
        """Return path to the places test file."""
        return str(Path(__file__).parent / "data" / "places_test.parquet")

    @pytest.fixture
    def sample_table(self, places_file):
        """Create a sample table from places test data."""
        return pq.read_table(places_file)

    def test_add_s2_basic(self, sample_table):
        """Test basic S2 column addition."""
        result = add_s2_table(sample_table, level=13)
        assert "s2_cell" in result.column_names
        assert result.num_rows == sample_table.num_rows

    def test_add_s2_custom_column_name(self, sample_table):
        """Test with custom column name."""
        result = add_s2_table(sample_table, s2_column_name="my_s2", level=13)
        assert "my_s2" in result.column_names
        assert result.num_rows == sample_table.num_rows

    def test_add_s2_different_levels(self, sample_table):
        """Test different S2 levels."""
        for level in [8, 13, 18]:
            result = add_s2_table(sample_table, level=level)
            assert "s2_cell" in result.column_names
            assert result.num_rows == sample_table.num_rows

    def test_add_s2_invalid_level_low(self, sample_table):
        """Test error with level too low."""
        with pytest.raises(ValueError, match="level must be between"):
            add_s2_table(sample_table, level=-1)

    def test_add_s2_invalid_level_high(self, sample_table):
        """Test error with level too high."""
        with pytest.raises(ValueError, match="level must be between"):
            add_s2_table(sample_table, level=31)

    def test_add_s2_metadata_preserved(self, sample_table):
        """Test that GeoParquet metadata is preserved."""
        result = add_s2_table(sample_table, level=13)
        # Check that geo metadata is preserved
        if sample_table.schema.metadata and b"geo" in sample_table.schema.metadata:
            assert b"geo" in result.schema.metadata

    def test_add_s2_values_are_strings(self, sample_table):
        """Test that S2 cell values are stored as strings (tokens)."""
        result = add_s2_table(sample_table, level=13)
        s2_col = result.column("s2_cell")
        # S2 tokens are hex strings
        assert s2_col.type == "string" or str(s2_col.type) in ["string", "large_string"]


class TestAddS2File:
    """Tests for file-based add_s2_column function."""

    @pytest.fixture
    def places_file(self):
        """Return path to the places test file."""
        return str(Path(__file__).parent / "data" / "places_test.parquet")

    @pytest.fixture
    def output_file(self):
        """Create a temp output file path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_add_s2_{uuid.uuid4()}.parquet"
        yield str(tmp_path)
        safe_unlink(tmp_path)

    def test_add_s2_file_basic(self, places_file, output_file):
        """Test basic file-to-file S2 addition."""
        add_s2_column(places_file, output_file, s2_level=13)
        assert Path(output_file).exists()
        result = pq.read_table(output_file)
        assert "s2_cell" in result.column_names
        assert result.num_rows == 766

    def test_add_s2_file_custom_name(self, places_file, output_file):
        """Test with custom column name."""
        add_s2_column(places_file, output_file, s2_column_name="custom_s2", s2_level=13)
        assert Path(output_file).exists()
        result = pq.read_table(output_file)
        assert "custom_s2" in result.column_names

    def test_add_s2_dry_run(self, places_file, output_file):
        """Test dry-run mode."""
        # Dry-run should not create output file
        add_s2_column(places_file, output_file, s2_level=13, dry_run=True)
        assert not Path(output_file).exists()


class TestAddS2Streaming:
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
        tmp_path = Path(tempfile.gettempdir()) / f"test_add_s2_stream_{uuid.uuid4()}.parquet"
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
        add_s2_column("-", output_file, s2_level=13)

        # Verify output
        assert Path(output_file).exists()
        result = pq.read_table(output_file)
        assert "s2_cell" in result.column_names
        assert result.num_rows == sample_geo_table.num_rows

    def test_file_to_stdout(self, places_file, monkeypatch):
        """Test writing to mocked stdout."""
        output_buffer = io.BytesIO()
        mock_stdout = mock.MagicMock()
        mock_stdout.buffer = output_buffer
        mock_stdout.isatty.return_value = False
        monkeypatch.setattr(sys, "stdout", mock_stdout)

        # Call function with "-" output
        add_s2_column(places_file, "-", s2_level=13)

        # Verify stream
        output_buffer.seek(0)
        reader = ipc.RecordBatchStreamReader(output_buffer)
        result = reader.read_all()
        assert result.num_rows > 0
        assert "s2_cell" in result.column_names


class TestAddS2CLI:
    """Tests for add s2 CLI command."""

    @pytest.fixture
    def places_file(self):
        """Return path to the places test file."""
        return str(Path(__file__).parent / "data" / "places_test.parquet")

    @pytest.fixture
    def output_file(self):
        """Create a temp output file path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_add_s2_cli_{uuid.uuid4()}.parquet"
        yield str(tmp_path)
        safe_unlink(tmp_path)

    def test_add_s2_cli_help(self):
        """Test that add s2 command has help."""
        from geoparquet_io.cli.main import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["add", "s2", "--help"])
        assert result.exit_code == 0
        assert "s2" in result.output.lower()

    def test_add_s2_cli_basic(self, places_file, output_file):
        """Test basic CLI invocation."""
        from geoparquet_io.cli.main import cli

        skip_if_geography_unavailable()

        runner = CliRunner()
        result = runner.invoke(cli, ["add", "s2", places_file, output_file, "--level", "13"])
        assert result.exit_code == 0
        assert Path(output_file).exists()
        loaded = pq.read_table(output_file)
        assert "s2_cell" in loaded.column_names
