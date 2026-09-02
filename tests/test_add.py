"""
Tests for add commands.
"""

import os

import duckdb
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import add


def _read_geo_metadata(parquet_file):
    """Parse the 'geo' key out of a Parquet file's schema metadata."""
    import json

    import pyarrow.parquet as pq

    metadata = pq.read_metadata(parquet_file).metadata
    assert metadata is not None and b"geo" in metadata, f"{parquet_file} has no geo metadata"
    return json.loads(metadata[b"geo"].decode("utf-8"))


class TestAddCommands:
    """Test suite for add commands."""

    def test_add_bbox_to_buildings(self, buildings_test_file, temp_output_file):
        """Test adding bbox column to buildings file (which doesn't have bbox)."""
        runner = CliRunner()
        result = runner.invoke(add, ["bbox", buildings_test_file, temp_output_file])
        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)

        # Verify bbox column was added
        conn = duckdb.connect()
        columns = conn.execute(f'DESCRIBE SELECT * FROM "{temp_output_file}"').fetchall()
        column_names = [col[0] for col in columns]
        assert "bbox" in column_names

        # Verify row count matches
        input_count = conn.execute(f'SELECT COUNT(*) FROM "{buildings_test_file}"').fetchone()[0]
        output_count = conn.execute(f'SELECT COUNT(*) FROM "{temp_output_file}"').fetchone()[0]
        assert input_count == output_count

        # Verify bbox structure
        bbox_col = conn.execute(f'DESCRIBE SELECT * FROM "{temp_output_file}"').fetchall()
        bbox_info = [col for col in bbox_col if col[0] == "bbox"][0]
        assert "STRUCT" in bbox_info[1]

    def test_add_bbox_to_places_copies_existing(self, places_with_covering_file, temp_output_file):
        """Existing bbox column: copy the input to OUTPUT_FILE and say so (#728).

        The requested end state -- a file at OUTPUT_FILE carrying a bbox column --
        is already satisfiable from the input, so gpio satisfies it by copying
        instead of leaving the caller with no output file at all.
        """
        runner = CliRunner()
        result = runner.invoke(add, ["bbox", places_with_covering_file, temp_output_file])
        assert result.exit_code == 0

        # The output the caller asked for exists.
        assert os.path.exists(temp_output_file)

        # The user is told what happened: nothing recomputed, a verbatim copy,
        # and how to force a recompute.
        assert "already has bbox column" in result.output
        assert "Copied" in result.output
        assert "not recomputed" in result.output
        assert "--force" in result.output

        # Output is a valid GeoParquet with the bbox column and covering metadata.
        geo = _read_geo_metadata(temp_output_file)
        primary = geo["primary_column"]
        assert geo["columns"][primary]["covering"]["bbox"]["xmin"][0] == "bbox"

        conn = duckdb.connect()
        columns = conn.execute(f'DESCRIBE SELECT * FROM "{temp_output_file}"').fetchall()
        column_names = [col[0] for col in columns]
        assert column_names.count("bbox") == 1
        assert "bbox_1" not in column_names

        # Row count matches the input.
        input_count = conn.execute(
            f'SELECT COUNT(*) FROM "{places_with_covering_file}"'
        ).fetchone()[0]
        output_count = conn.execute(f'SELECT COUNT(*) FROM "{temp_output_file}"').fetchone()[0]
        assert input_count == output_count

    def test_add_bbox_copies_when_covering_metadata_missing(
        self, places_v11_file, temp_output_file
    ):
        """Bbox column without covering metadata: still write OUTPUT_FILE (#728)."""
        runner = CliRunner()
        result = runner.invoke(add, ["bbox", places_v11_file, temp_output_file])
        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)

        # Distinct diagnostic kept, plus the copy notice.
        assert "lacks covering metadata" in result.output
        assert "add bbox-metadata" in result.output
        assert "Copied" in result.output
        assert "not recomputed" in result.output

        conn = duckdb.connect()
        columns = conn.execute(f'DESCRIBE SELECT * FROM "{temp_output_file}"').fetchall()
        column_names = [col[0] for col in columns]
        assert column_names.count("bbox") == 1
        input_count = conn.execute(f'SELECT COUNT(*) FROM "{places_v11_file}"').fetchone()[0]
        output_count = conn.execute(f'SELECT COUNT(*) FROM "{temp_output_file}"').fetchone()[0]
        assert input_count == output_count

    def test_add_bbox_custom_name_alongside_existing_writes_output(
        self, places_with_covering_file, temp_output_file
    ):
        """A different --bbox-name is not a conflict, so it is computed (#728).

        Copying the input would not satisfy this request -- the input has no
        'bounds' column -- so the column is computed and OUTPUT_FILE written,
        with the same "2 bbox columns" warning --force gives.
        """
        runner = CliRunner()
        result = runner.invoke(
            add, ["bbox", places_with_covering_file, temp_output_file, "--bbox-name", "bounds"]
        )
        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)
        assert "2 bbox columns" in result.output
        assert "Copied" not in result.output

        conn = duckdb.connect()
        columns = conn.execute(f'DESCRIBE SELECT * FROM "{temp_output_file}"').fetchall()
        column_names = [col[0] for col in columns]
        assert "bbox" in column_names  # existing column left alone
        assert "bounds" in column_names  # the requested column was computed

    def test_add_bbox_streaming_does_not_duplicate_existing_bbox(
        self, places_test_file, monkeypatch, caplog
    ):
        """Streaming to stdout must not silently append a second bbox column (#728)."""
        import io
        import logging
        import sys
        from unittest import mock

        import pyarrow.ipc as ipc

        from geoparquet_io.core.add.bbox import add_bbox_column

        output_buffer = io.BytesIO()
        mock_stdout = mock.MagicMock()
        mock_stdout.buffer = output_buffer
        mock_stdout.isatty.return_value = False
        monkeypatch.setattr(sys, "stdout", mock_stdout)

        with caplog.at_level(logging.INFO, logger="geoparquet_io"):
            add_bbox_column(places_test_file, "-")

        output_buffer.seek(0)
        table = ipc.RecordBatchStreamReader(output_buffer).read_all()
        assert table.schema.names.count("bbox") == 1
        assert "bbox_1" not in table.schema.names

        # The pass-through is announced, not silent.
        assert "already has bbox column" in caplog.text
        assert "not recomputed" in caplog.text

    def test_add_bbox_streaming_force_replaces_existing_bbox(self, places_test_file, monkeypatch):
        """--force still recomputes the bbox column when streaming."""
        import io
        import sys
        from unittest import mock

        import pyarrow.ipc as ipc

        from geoparquet_io.core.add.bbox import add_bbox_column

        output_buffer = io.BytesIO()
        mock_stdout = mock.MagicMock()
        mock_stdout.buffer = output_buffer
        mock_stdout.isatty.return_value = False
        monkeypatch.setattr(sys, "stdout", mock_stdout)

        add_bbox_column(places_test_file, "-", force=True)

        output_buffer.seek(0)
        table = ipc.RecordBatchStreamReader(output_buffer).read_all()
        assert table.schema.names.count("bbox") == 1
        assert "bbox_1" not in table.schema.names

    def test_add_bbox_without_output_path_still_only_reports(
        self, places_with_covering_file, monkeypatch, caplog
    ):
        """No OUTPUT_FILE: behaviour is unchanged -- report only, copy nothing (#728)."""
        import logging
        import sys
        from unittest import mock

        from geoparquet_io.core.add.bbox import add_bbox_column

        mock_stdout = mock.MagicMock()
        mock_stdout.isatty.return_value = True
        monkeypatch.setattr(sys, "stdout", mock_stdout)

        with caplog.at_level(logging.INFO, logger="geoparquet_io"):
            add_bbox_column(places_with_covering_file, None)

        assert "already has bbox column" in caplog.text
        assert "Copied" not in caplog.text

    def test_add_bbox_force_replaces_existing(self, places_test_file, temp_output_file):
        """Test --force flag replaces existing bbox column."""
        runner = CliRunner()
        result = runner.invoke(add, ["bbox", places_test_file, temp_output_file, "--force"])
        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)
        assert "Replacing existing bbox column" in result.output

        # Verify only 1 bbox column exists in output
        conn = duckdb.connect()
        columns = conn.execute(f'DESCRIBE SELECT * FROM "{temp_output_file}"').fetchall()
        bbox_columns = [col for col in columns if col[0] == "bbox"]
        assert len(bbox_columns) == 1

        # Verify row count preserved
        input_count = conn.execute(f'SELECT COUNT(*) FROM "{places_test_file}"').fetchone()[0]
        output_count = conn.execute(f'SELECT COUNT(*) FROM "{temp_output_file}"').fetchone()[0]
        assert input_count == output_count

    def test_add_bbox_force_with_custom_name(self, places_test_file, temp_output_file):
        """Test --force with custom name keeps both columns and warns."""
        runner = CliRunner()
        result = runner.invoke(
            add, ["bbox", places_test_file, temp_output_file, "--force", "--bbox-name", "bounds"]
        )
        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)
        # Should warn about 2 bbox columns
        assert "2 bbox columns" in result.output

        # Verify both bbox and bounds columns exist
        conn = duckdb.connect()
        columns = conn.execute(f'DESCRIBE SELECT * FROM "{temp_output_file}"').fetchall()
        column_names = [col[0] for col in columns]
        assert "bbox" in column_names  # Original kept
        assert "bounds" in column_names  # New one added

    def test_add_bbox_with_custom_name(self, buildings_test_file, temp_output_file):
        """Test adding bbox column with custom name."""
        runner = CliRunner()
        result = runner.invoke(
            add, ["bbox", buildings_test_file, temp_output_file, "--bbox-name", "bounds"]
        )
        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)

        # Verify custom bbox column name was used
        conn = duckdb.connect()
        columns = conn.execute(f'DESCRIBE SELECT * FROM "{temp_output_file}"').fetchall()
        column_names = [col[0] for col in columns]
        assert "bounds" in column_names

    def test_add_bbox_with_verbose(self, buildings_test_file, temp_output_file):
        """Test adding bbox column with verbose flag."""
        runner = CliRunner()
        result = runner.invoke(add, ["bbox", buildings_test_file, temp_output_file, "--verbose"])
        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)

    def test_add_bbox_preserves_columns(self, buildings_test_file, temp_output_file):
        """Test that add bbox preserves all original columns."""
        runner = CliRunner()
        result = runner.invoke(add, ["bbox", buildings_test_file, temp_output_file])
        assert result.exit_code == 0

        # Verify columns are preserved
        conn = duckdb.connect()
        input_columns = conn.execute(f'DESCRIBE SELECT * FROM "{buildings_test_file}"').fetchall()
        output_columns = conn.execute(f'DESCRIBE SELECT * FROM "{temp_output_file}"').fetchall()

        input_col_names = {col[0] for col in input_columns}
        output_col_names = {col[0] for col in output_columns}

        # All input columns should be in output
        assert input_col_names.issubset(output_col_names)
        # Output should have bbox column added
        assert "bbox" in output_col_names

    def test_add_bbox_nonexistent_file(self, temp_output_file):
        """Test add bbox on nonexistent file."""
        runner = CliRunner()
        result = runner.invoke(add, ["bbox", "nonexistent.parquet", temp_output_file])
        # Should fail with non-zero exit code
        assert result.exit_code != 0

    def test_add_bbox_with_metadata_always_added(self, buildings_test_file, temp_output_file):
        """Test that bbox metadata is automatically added."""
        import json

        import pyarrow.parquet as pq

        runner = CliRunner()
        result = runner.invoke(add, ["bbox", buildings_test_file, temp_output_file])
        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)

        # Verify bbox metadata was added automatically
        pf = pq.ParquetFile(temp_output_file)
        metadata = pf.schema_arrow.metadata
        assert b"geo" in metadata

        geo_meta = json.loads(metadata[b"geo"].decode("utf-8"))

        # Verify bbox covering metadata exists
        assert "columns" in geo_meta
        assert "geometry" in geo_meta["columns"]
        assert "covering" in geo_meta["columns"]["geometry"]
        assert "bbox" in geo_meta["columns"]["geometry"]["covering"]

    def test_add_bbox_metadata_on_v10_file_with_bbox_errors(self, temp_output_dir):
        """add bbox-metadata refuses a 1.0 file, whose version cannot carry covering.

        places_test.parquet declares GeoParquet 1.0.0 and has a bbox column but no
        covering. This used to "succeed" by writing the 1.1-only covering key into a
        1.0 file, producing output that `gpio check spec` rejects (gpio #686). The
        1.1 success path is covered by tests/test_v10_covering_gate.py.
        """
        import shutil

        # Use places file which has a bbox column
        from pathlib import Path

        places_path = Path(__file__).parent / "data" / "places_test.parquet"
        temp_file = os.path.join(temp_output_dir, "places_copy.parquet")
        shutil.copy2(places_path, temp_file)

        runner = CliRunner()
        result = runner.invoke(add, ["bbox-metadata", temp_file])
        assert result.exit_code != 0
        assert "1.1" in result.output

        # The file must be left untouched, not half-updated.
        with open(temp_file, "rb") as fh, open(places_path, "rb") as original:
            assert fh.read() == original.read()

    def test_add_bbox_metadata_no_bbox_column(self, buildings_test_file):
        """add bbox-metadata fails when there is no bbox column to describe.

        This used to print the error and still exit 0, the same "reports failure,
        exits 0" shape as gpio #713.
        """
        runner = CliRunner()
        result = runner.invoke(add, ["bbox-metadata", buildings_test_file])
        assert result.exit_code != 0
        assert "No valid bbox column found" in result.output

    # H3 tests
    def test_add_h3_to_buildings(self, buildings_test_file, temp_output_file):
        """Test adding H3 column to buildings file."""
        runner = CliRunner()
        result = runner.invoke(add, ["h3", buildings_test_file, temp_output_file])
        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)

        # Verify h3_cell column was added
        conn = duckdb.connect()
        conn.execute("INSTALL spatial; LOAD spatial;")
        conn.execute("INSTALL h3 FROM community; LOAD h3;")

        columns = conn.execute(f'DESCRIBE SELECT * FROM "{temp_output_file}"').fetchall()
        column_names = [col[0] for col in columns]
        assert "h3_cell" in column_names

        # Verify row count is preserved
        input_count = conn.execute(f'SELECT COUNT(*) FROM "{buildings_test_file}"').fetchone()[0]
        output_count = conn.execute(f'SELECT COUNT(*) FROM "{temp_output_file}"').fetchone()[0]
        assert input_count == output_count

        # Verify H3 column is VARCHAR
        h3_col = [col for col in columns if col[0] == "h3_cell"][0]
        assert "VARCHAR" in h3_col[1]

        # Verify H3 cells are valid
        valid_count = conn.execute(
            f'SELECT COUNT(*) FROM "{temp_output_file}" '
            f"WHERE h3_is_valid_cell(h3_string_to_h3(h3_cell))"
        ).fetchone()[0]
        assert valid_count == output_count

    def test_add_h3_default_resolution(self, buildings_test_file, temp_output_file):
        """Test that H3 uses resolution 9 by default."""
        runner = CliRunner()
        result = runner.invoke(add, ["h3", buildings_test_file, temp_output_file])
        assert result.exit_code == 0

        # Verify all cells are resolution 9
        conn = duckdb.connect()
        conn.execute("INSTALL spatial; LOAD spatial;")
        conn.execute("INSTALL h3 FROM community; LOAD h3;")

        resolutions = conn.execute(
            f'SELECT DISTINCT h3_get_resolution(h3_string_to_h3(h3_cell)) FROM "{temp_output_file}"'
        ).fetchall()
        assert len(resolutions) == 1
        assert resolutions[0][0] == 9

    def test_add_h3_custom_resolution(self, buildings_test_file, temp_output_file):
        """Test adding H3 column with custom resolution."""
        runner = CliRunner()
        result = runner.invoke(
            add, ["h3", buildings_test_file, temp_output_file, "--resolution", "13"]
        )
        assert result.exit_code == 0

        # Verify all cells are resolution 13
        conn = duckdb.connect()
        conn.execute("INSTALL spatial; LOAD spatial;")
        conn.execute("INSTALL h3 FROM community; LOAD h3;")

        resolutions = conn.execute(
            f'SELECT DISTINCT h3_get_resolution(h3_string_to_h3(h3_cell)) FROM "{temp_output_file}"'
        ).fetchall()
        assert len(resolutions) == 1
        assert resolutions[0][0] == 13

    def test_add_h3_with_custom_name(self, buildings_test_file, temp_output_file):
        """Test adding H3 column with custom name."""
        runner = CliRunner()
        result = runner.invoke(
            add, ["h3", buildings_test_file, temp_output_file, "--h3-name", "h3_building"]
        )
        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)

        # Verify custom H3 column name was used
        conn = duckdb.connect()
        columns = conn.execute(f'DESCRIBE SELECT * FROM "{temp_output_file}"').fetchall()
        column_names = [col[0] for col in columns]
        assert "h3_building" in column_names

    def test_add_h3_with_verbose(self, buildings_test_file, temp_output_file):
        """Test adding H3 column with verbose flag."""
        runner = CliRunner()
        result = runner.invoke(add, ["h3", buildings_test_file, temp_output_file, "--verbose"])
        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)
        assert "Loading DuckDB extension: h3" in result.output

    def test_add_h3_preserves_columns(self, buildings_test_file, temp_output_file):
        """Test that add H3 preserves all original columns."""
        runner = CliRunner()
        result = runner.invoke(add, ["h3", buildings_test_file, temp_output_file])
        assert result.exit_code == 0

        # Verify columns are preserved
        conn = duckdb.connect()
        input_columns = conn.execute(f'DESCRIBE SELECT * FROM "{buildings_test_file}"').fetchall()
        output_columns = conn.execute(f'DESCRIBE SELECT * FROM "{temp_output_file}"').fetchall()

        input_col_names = {col[0] for col in input_columns}
        output_col_names = {col[0] for col in output_columns}

        # All input columns should be in output
        assert input_col_names.issubset(output_col_names)
        # Output should have h3_cell column added
        assert "h3_cell" in output_col_names

    def test_add_h3_nonexistent_file(self, temp_output_file):
        """Test add H3 on nonexistent file."""
        runner = CliRunner()
        result = runner.invoke(add, ["h3", "nonexistent.parquet", temp_output_file])
        # Should fail with non-zero exit code
        assert result.exit_code != 0

    def test_add_h3_metadata(self, buildings_test_file, temp_output_file):
        """Test that H3 metadata is added to GeoParquet file."""
        import json

        import pyarrow.parquet as pq

        runner = CliRunner()
        result = runner.invoke(
            add, ["h3", buildings_test_file, temp_output_file, "--resolution", "13"]
        )
        assert result.exit_code == 0

        # Read metadata
        pf = pq.ParquetFile(temp_output_file)
        metadata = pf.schema_arrow.metadata
        assert b"geo" in metadata

        geo_meta = json.loads(metadata[b"geo"].decode("utf-8"))

        # Verify H3 covering metadata exists
        assert "columns" in geo_meta
        assert "geometry" in geo_meta["columns"]
        assert "covering" in geo_meta["columns"]["geometry"]
        assert "h3" in geo_meta["columns"]["geometry"]["covering"]

        # Verify H3 metadata content
        h3_meta = geo_meta["columns"]["geometry"]["covering"]["h3"]
        assert h3_meta["column"] == "h3_cell"
        assert h3_meta["resolution"] == 13

    def test_add_h3_invalid_resolution_too_low(self, buildings_test_file, temp_output_file):
        """Test adding H3 with invalid resolution (too low)."""
        runner = CliRunner()
        result = runner.invoke(
            add, ["h3", buildings_test_file, temp_output_file, "--resolution", "-1"]
        )
        # Should fail with error about invalid resolution
        assert result.exit_code != 0

    def test_add_h3_invalid_resolution_too_high(self, buildings_test_file, temp_output_file):
        """Test adding H3 with invalid resolution (too high)."""
        runner = CliRunner()
        result = runner.invoke(
            add, ["h3", buildings_test_file, temp_output_file, "--resolution", "16"]
        )
        # Should fail with error about invalid resolution
        assert result.exit_code != 0

    def test_add_h3_core_function_invalid_resolution(self, buildings_test_file, temp_output_file):
        """Test core add_h3_column function with invalid resolution (covers line 51)."""
        from geoparquet_io.core.add.h3 import add_h3_column
        from geoparquet_io.core.exceptions import InvalidParameterError

        # Test resolution too high (bypassing CLI validation)
        with pytest.raises(InvalidParameterError) as exc_info:
            add_h3_column(
                input_parquet=buildings_test_file,
                output_parquet=temp_output_file,
                h3_resolution=16,
                h3_column_name="h3_cell",
                verbose=False,
            )
        assert "must be between 0 and 15" in str(exc_info.value)

        # Test resolution too low
        with pytest.raises(InvalidParameterError) as exc_info:
            add_h3_column(
                input_parquet=buildings_test_file,
                output_parquet=temp_output_file,
                h3_resolution=-1,
                h3_column_name="h3_cell",
                verbose=False,
            )
        assert "must be between 0 and 15" in str(exc_info.value)

    # Note: add admin-divisions tests are skipped because they require a countries file
    # and network access. These should be tested separately with appropriate test data.
    @pytest.mark.skip(reason="Requires countries file and network access")
    def test_add_admin_divisions(self, places_test_file, temp_output_file):
        """Test adding admin divisions (skipped - requires countries file)."""
        pass


class TestRemoteWriteSupport:
    """Tests for remote write functionality."""

    def test_remote_url_detection(self):
        """Test that remote URLs are correctly detected."""
        from geoparquet_io.core.remote import is_remote_url

        # Test S3 URLs
        assert is_remote_url("s3://bucket/file.parquet")
        assert is_remote_url("s3://my-bucket/path/to/file.parquet")

        # Test GCS URLs
        assert is_remote_url("gs://bucket/file.parquet")

        # Test Azure URLs
        assert is_remote_url("az://container/file.parquet")

        # Test HTTP/HTTPS URLs
        assert is_remote_url("https://example.com/data.parquet")
        assert is_remote_url("http://example.com/data.parquet")

        # Test local paths (should return False)
        assert not is_remote_url("local.parquet")
        assert not is_remote_url("/path/to/file.parquet")
        assert not is_remote_url("./relative/path.parquet")

    def test_write_with_remote_output_creates_temp_file(self, buildings_test_file):
        """Test that remote outputs trigger temp file creation."""
        from unittest.mock import MagicMock, patch

        import duckdb

        from geoparquet_io.core.common import write_parquet_with_metadata

        # Mock the upload function to avoid actual upload
        mock_upload = MagicMock()

        with patch("geoparquet_io.core.upload.upload", mock_upload):
            # Create a mock DuckDB connection
            con = duckdb.connect()
            con.execute("INSTALL spatial; LOAD spatial;")

            # Simple query to read the test file
            from geoparquet_io.core.file_utils import safe_file_url

            input_url = safe_file_url(buildings_test_file, False)
            query = f"SELECT * FROM '{input_url}' LIMIT 10"

            # Remote S3 output
            remote_output = "s3://test-bucket/output.parquet"

            # This should create a temp file and attempt to upload
            write_parquet_with_metadata(
                con=con, query=query, output_file=remote_output, verbose=False
            )

            # Verify upload was called
            assert mock_upload.called
            # Check that the source was a local temp file
            call_args = mock_upload.call_args
            assert call_args is not None
            source_path = str(call_args[1]["source"])  # Keyword arg 'source'
            assert source_path.endswith(".parquet")
            # Destination should be the remote URL
            assert call_args[1]["destination"] == remote_output


class TestAddCommandErrorHandling:
    """Tests for user-friendly error handling in add commands."""

    def test_add_bbox_with_gpkg_shows_friendly_error(self, tmp_path):
        """Test that using a .gpkg file shows a friendly error, not a stack trace."""
        # Create a fake gpkg file (not a valid parquet)
        gpkg_file = tmp_path / "test.gpkg"
        gpkg_file.write_text("Not a parquet file")

        runner = CliRunner()
        result = runner.invoke(add, ["bbox", str(gpkg_file)])

        # Should fail with exit code 1
        assert result.exit_code == 1

        # Should show friendly error message, not a stack trace
        assert "Traceback" not in result.output
        assert "Not a valid Parquet file" in result.output
        assert "gpio convert" in result.output

    def test_add_bbox_with_nonexistent_file_shows_friendly_error(self):
        """Test that using a nonexistent file shows a friendly error."""
        runner = CliRunner()
        result = runner.invoke(add, ["bbox", "/nonexistent/path/file.parquet"])

        # Should fail
        assert result.exit_code != 0

        # Should show friendly error message
        assert "Traceback" not in result.output


class TestAddBboxCoveringMetadata:
    """Tests for bbox covering metadata (fixes #412)."""

    def test_add_bbox_includes_covering_metadata_for_v2(
        self, buildings_test_file, temp_output_file
    ):
        """Test that add bbox includes covering metadata for GeoParquet 2.0."""
        import json

        import pyarrow.parquet as pq

        runner = CliRunner()
        result = runner.invoke(
            add,
            ["bbox", buildings_test_file, temp_output_file, "--geoparquet-version", "2.0"],
        )
        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)

        # Check covering metadata in geo metadata
        pf = pq.ParquetFile(temp_output_file)
        geo_meta = json.loads(pf.schema_arrow.metadata.get(b"geo", b"{}"))

        # Find the geometry column's covering metadata
        primary_col = geo_meta.get("primary_column", "geometry")
        col_meta = geo_meta.get("columns", {}).get(primary_col, {})
        covering = col_meta.get("covering", {})

        # Verify bbox covering exists with correct structure
        assert "bbox" in covering, "covering.bbox should exist in geo metadata"
        bbox_covering = covering["bbox"]
        assert bbox_covering.get("xmin") == ["bbox", "xmin"]
        assert bbox_covering.get("ymin") == ["bbox", "ymin"]
        assert bbox_covering.get("xmax") == ["bbox", "xmax"]
        assert bbox_covering.get("ymax") == ["bbox", "ymax"]

    def test_add_bbox_includes_covering_metadata_for_v1_1(
        self, buildings_test_file, temp_output_file
    ):
        """Test that add bbox includes covering metadata for GeoParquet 1.1."""
        import json

        import pyarrow.parquet as pq

        runner = CliRunner()
        result = runner.invoke(
            add,
            ["bbox", buildings_test_file, temp_output_file, "--geoparquet-version", "1.1"],
        )
        assert result.exit_code == 0
        assert os.path.exists(temp_output_file)

        # Check covering metadata
        pf = pq.ParquetFile(temp_output_file)
        geo_meta = json.loads(pf.schema_arrow.metadata.get(b"geo", b"{}"))

        primary_col = geo_meta.get("primary_column", "geometry")
        col_meta = geo_meta.get("columns", {}).get(primary_col, {})
        covering = col_meta.get("covering", {})

        assert "bbox" in covering, "covering.bbox should exist for GeoParquet 1.1"

    def test_add_bbox_streaming_includes_covering_metadata(self, buildings_test_file, tmp_path):
        """Test that streaming add bbox also includes covering metadata via internal function."""
        import json

        import pyarrow.parquet as pq

        from geoparquet_io.core.add.bbox import _add_bbox_streaming

        output_file = str(tmp_path / "streaming_bbox.parquet")

        # Test the internal streaming function directly (simulates stdin->file path)
        _add_bbox_streaming(
            input_path=buildings_test_file,  # File as source (simulates stdin materialized)
            output_path=output_file,
            bbox_column_name="bbox",
            verbose=False,
            compression="ZSTD",
            compression_level=None,
            row_group_size_mb=None,
            row_group_rows=None,
            profile=None,
            force=False,
            geoparquet_version="2.0",
            memory_limit=None,
        )

        assert os.path.exists(output_file)

        # Check covering metadata exists in streaming output
        pf = pq.ParquetFile(output_file)
        geo_meta = json.loads(pf.schema_arrow.metadata.get(b"geo", b"{}"))

        primary_col = geo_meta.get("primary_column", "geometry")
        col_meta = geo_meta.get("columns", {}).get(primary_col, {})
        covering = col_meta.get("covering", {})

        assert "bbox" in covering, "covering.bbox should exist in streaming output"
        bbox_covering = covering["bbox"]
        assert bbox_covering.get("xmin") == ["bbox", "xmin"]
        assert bbox_covering.get("ymax") == ["bbox", "ymax"]


class TestAddBboxMetadataPreservesFileProperties:
    """Tests for issue #433: bbox-metadata must preserve bloom filters and GEOMETRY type."""

    def test_preserves_bloom_filters_and_geometry_type(self, tmp_path):
        """Test that add bbox-metadata preserves bloom filters and native GEOMETRY logical type.

        Regression test for issue #433: gpio add bbox-metadata was using PyArrow
        read_table/write_table which destroys bloom filters and drops the native
        GEOMETRY logical type from GeoParquet 2.0 files.
        """
        import json

        import pyarrow.parquet as pq

        # Create a GeoParquet 2.0 file with DuckDB
        # - 5000+ rows triggers bloom filter creation on VARCHAR columns
        # - GEOPARQUET_VERSION 'V2' writes native GEOMETRY logical type
        test_file = str(tmp_path / "test_v2_with_bloom.parquet")

        conn = duckdb.connect()
        conn.execute("INSTALL spatial; LOAD spatial;")
        conn.execute(f"""
            COPY (
                SELECT
                    'name_' || (i % 50) AS name,
                    STRUCT_PACK(
                        xmin := ST_XMin(geometry)::FLOAT,
                        ymin := ST_YMin(geometry)::FLOAT,
                        xmax := ST_XMax(geometry)::FLOAT,
                        ymax := ST_YMax(geometry)::FLOAT
                    ) AS bbox,
                    geometry
                FROM (
                    SELECT i, ST_Point(i * 0.001, i * 0.001) AS geometry
                    FROM range(5000) t(i)
                )
            ) TO '{test_file}' (FORMAT PARQUET, GEOPARQUET_VERSION 'V2')
        """)
        conn.close()

        # BEFORE: Verify bloom filter and GEOMETRY type exist
        # Check bloom filter on 'name' column
        conn = duckdb.connect()
        bloom_before = conn.execute(f"""
            SELECT bloom_filter_offset IS NOT NULL AS has_bloom
            FROM parquet_metadata('{test_file}')
            WHERE path_in_schema = 'name'
            LIMIT 1
        """).fetchone()[0]

        # Check GEOMETRY logical type
        schema_before = conn.execute(f"""
            SELECT logical_type
            FROM parquet_schema('{test_file}')
            WHERE name = 'geometry'
        """).fetchone()[0]
        conn.close()

        assert bloom_before, "BEFORE: Expected bloom filter on 'name' column"
        assert schema_before is not None, "BEFORE: Expected GEOMETRY logical type"
        assert "Geometry" in str(schema_before), (
            f"BEFORE: Expected GeometryType, got {schema_before}"
        )

        # Run add bbox-metadata
        runner = CliRunner()
        result = runner.invoke(add, ["bbox-metadata", test_file, "--verbose"])
        assert result.exit_code == 0, f"Command failed: {result.output}"
        assert "Added bbox covering metadata" in result.output

        # AFTER: Verify bloom filter and GEOMETRY type are preserved
        conn = duckdb.connect()
        bloom_after = conn.execute(f"""
            SELECT bloom_filter_offset IS NOT NULL AS has_bloom
            FROM parquet_metadata('{test_file}')
            WHERE path_in_schema = 'name'
            LIMIT 1
        """).fetchone()[0]

        schema_after = conn.execute(f"""
            SELECT logical_type
            FROM parquet_schema('{test_file}')
            WHERE name = 'geometry'
        """).fetchone()[0]
        conn.close()

        # These assertions will FAIL on current code (issue #433)
        assert bloom_after, "AFTER: Bloom filter on 'name' column was destroyed!"
        assert schema_after is not None, "AFTER: GEOMETRY logical type was dropped!"
        assert "Geometry" in str(schema_after), f"AFTER: Expected GeometryType, got {schema_after}"

        # Also verify the covering metadata was actually added
        pf = pq.ParquetFile(test_file)
        geo_meta = json.loads(pf.schema_arrow.metadata.get(b"geo", b"{}"))
        primary_col = geo_meta.get("primary_column", "geometry")
        col_meta = geo_meta.get("columns", {}).get(primary_col, {})
        covering = col_meta.get("covering", {})
        assert "bbox" in covering, "covering.bbox should exist in geo metadata"

    def test_preserves_existing_kv_metadata(self, tmp_path):
        """Test that add bbox-metadata preserves non-geo KV metadata.

        Regression test: DuckDB KV_METADATA replaces all metadata, so we must
        explicitly preserve existing keys like 'pandas', 'ARROW:schema', and
        custom application metadata.
        """
        import json

        test_file = str(tmp_path / "test_with_custom_metadata.parquet")

        # Create a GeoParquet file with custom metadata using DuckDB
        # Use GEOPARQUET_VERSION to handle geometry encoding properly
        conn = duckdb.connect()
        conn.execute("INSTALL spatial; LOAD spatial;")

        # First create with GEOPARQUET_VERSION to get proper geo metadata
        conn.execute(f"""
            COPY (
                SELECT
                    'test' AS name,
                    STRUCT_PACK(
                        xmin := -1.0::FLOAT,
                        ymin := -1.0::FLOAT,
                        xmax := 1.0::FLOAT,
                        ymax := 1.0::FLOAT
                    ) AS bbox,
                    ST_Point(0, 0) AS geometry
            ) TO '{test_file}' (FORMAT PARQUET, GEOPARQUET_VERSION 'V1')
        """)

        # Read the geo metadata that DuckDB created
        geo_meta_row = conn.execute(
            f"SELECT value FROM parquet_kv_metadata('{test_file}') WHERE key = 'geo'"
        ).fetchone()
        geo_meta_value = geo_meta_row[0].decode("utf-8") if geo_meta_row else "{}"

        # DuckDB's 'V1' declares GeoParquet 1.0.0, which cannot carry the 1.1-only
        # covering key (gpio #686). This test is about KV preservation during the
        # rewrite, so declare 1.1.0 to keep the rewrite reachable.
        geo_meta_value = geo_meta_value.replace('"version":"1.0.0"', '"version":"1.1.0"').replace(
            '"version": "1.0.0"', '"version": "1.1.0"'
        )
        assert '"1.1.0"' in geo_meta_value

        # Now rewrite with additional custom metadata
        temp_file = str(tmp_path / "temp_with_meta.parquet")
        escaped_geo = geo_meta_value.replace("'", "''")
        conn.execute(f"""
            COPY (SELECT * FROM '{test_file}')
            TO '{temp_file}' (
                FORMAT PARQUET,
                KV_METADATA {{
                    geo: '{escaped_geo}',
                    pandas: '{{"index_columns": [], "columns": []}}',
                    custom_app: 'important_value_123',
                    another_key: 'should_be_preserved'
                }}
            )
        """)
        conn.close()

        # Use the file with custom metadata
        import shutil

        shutil.move(temp_file, test_file)

        # BEFORE: Verify custom metadata exists
        conn = duckdb.connect()
        before_kv = conn.execute(
            f"SELECT key, value FROM parquet_kv_metadata('{test_file}')"
        ).fetchall()
        conn.close()

        before_keys = {k.decode("utf-8") if isinstance(k, bytes) else k for k, _ in before_kv}
        assert "pandas" in before_keys, "BEFORE: Expected 'pandas' metadata"
        assert "custom_app" in before_keys, "BEFORE: Expected 'custom_app' metadata"
        assert "another_key" in before_keys, "BEFORE: Expected 'another_key' metadata"

        # Run add bbox-metadata
        runner = CliRunner()
        result = runner.invoke(add, ["bbox-metadata", test_file, "--verbose"])
        assert result.exit_code == 0, f"Command failed: {result.output}"
        assert "Added bbox covering metadata" in result.output

        # AFTER: Verify all metadata keys are preserved
        conn = duckdb.connect()
        after_kv = conn.execute(
            f"SELECT key, value FROM parquet_kv_metadata('{test_file}')"
        ).fetchall()
        conn.close()

        after_dict = {
            (k.decode("utf-8") if isinstance(k, bytes) else k): (
                v.decode("utf-8") if isinstance(v, bytes) else v
            )
            for k, v in after_kv
        }

        # Verify all original keys are preserved
        assert "pandas" in after_dict, "AFTER: 'pandas' metadata was destroyed!"
        assert "custom_app" in after_dict, "AFTER: 'custom_app' metadata was destroyed!"
        assert "another_key" in after_dict, "AFTER: 'another_key' metadata was destroyed!"

        # Verify values are preserved correctly
        assert after_dict["custom_app"] == "important_value_123", (
            f"AFTER: 'custom_app' value changed: {after_dict['custom_app']}"
        )
        assert after_dict["another_key"] == "should_be_preserved", (
            f"AFTER: 'another_key' value changed: {after_dict['another_key']}"
        )

        # Verify geo metadata was updated with covering
        geo_meta = json.loads(after_dict["geo"])
        primary_col = geo_meta.get("primary_column", "geometry")
        covering = geo_meta.get("columns", {}).get(primary_col, {}).get("covering", {})
        assert "bbox" in covering, "covering.bbox should exist in updated geo metadata"

    def test_preserves_kv_key_containing_colon(self, tmp_path):
        """A sidecar key containing ':' must not break the rewrite (#756).

        ``add bbox-metadata`` built its own KV_METADATA clause with unquoted key
        names, so any key with a ':' in it -- ``stac:collection``, pyarrow's
        ``ARROW:schema`` -- made DuckDB's parser reject the whole COPY with
        "syntax error at or near \":\"". It now shares
        ``build_kv_metadata_clause()``, which quotes and escapes both halves.
        """
        import json

        test_file = str(tmp_path / "colon_key.parquet")
        # A value that exercises the escaping too: single quotes, a backslash,
        # a newline and non-ASCII must all round-trip byte-identically.
        payload = json.dumps(
            {"id": "o'brien\\place", "note": "line1\nline2", "name": "Ka\u0301rlsplatz"}
        )

        conn = duckdb.connect()
        conn.execute("INSTALL spatial; LOAD spatial;")
        conn.execute(f"""
            COPY (
                SELECT
                    'test' AS name,
                    STRUCT_PACK(
                        xmin := -1.0::FLOAT,
                        ymin := -1.0::FLOAT,
                        xmax := 1.0::FLOAT,
                        ymax := 1.0::FLOAT
                    ) AS bbox,
                    ST_Point(0, 0) AS geometry
            ) TO '{test_file}' (FORMAT PARQUET, GEOPARQUET_VERSION 'V1')
        """)

        geo_meta_row = conn.execute(
            f"SELECT value FROM parquet_kv_metadata('{test_file}') WHERE key = 'geo'"
        ).fetchone()
        geo_meta_value = geo_meta_row[0].decode("utf-8") if geo_meta_row else "{}"
        # 'V1' declares 1.0.0, which cannot carry the 1.1-only covering key.
        geo_meta_value = geo_meta_value.replace('"version":"1.0.0"', '"version":"1.1.0"').replace(
            '"version": "1.0.0"', '"version": "1.1.0"'
        )
        assert '"1.1.0"' in geo_meta_value

        temp_file = str(tmp_path / "colon_key_seed.parquet")
        escaped_geo = geo_meta_value.replace("'", "''")
        escaped_payload = payload.replace("'", "''")
        conn.execute(f"""
            COPY (SELECT * FROM '{test_file}')
            TO '{temp_file}' (
                FORMAT PARQUET,
                KV_METADATA {{
                    'geo': '{escaped_geo}',
                    'stac:collection': '{escaped_payload}'
                }}
            )
        """)
        conn.close()

        import shutil

        shutil.move(temp_file, test_file)

        runner = CliRunner()
        result = runner.invoke(add, ["bbox-metadata", test_file])
        assert result.exit_code == 0, f"Command failed: {result.output}"

        conn = duckdb.connect()
        after_kv = conn.execute(
            f"SELECT key, value FROM parquet_kv_metadata('{test_file}')"
        ).fetchall()
        conn.close()
        after = {
            (k.decode("utf-8") if isinstance(k, bytes) else k): (
                v.decode("utf-8") if isinstance(v, bytes) else v
            )
            for k, v in after_kv
        }

        assert "stac:collection" in after, f"colon key was dropped: {sorted(after)}"
        assert after["stac:collection"] == payload, "colon key's value did not round-trip"

        geo_meta = json.loads(after["geo"])
        primary_col = geo_meta.get("primary_column", "geometry")
        covering = geo_meta.get("columns", {}).get(primary_col, {}).get("covering", {})
        assert "bbox" in covering, "covering.bbox should exist in updated geo metadata"

    def test_rejects_remote_urls(self, tmp_path):
        """Test that add bbox-metadata rejects remote URLs with clear error."""
        from geoparquet_io.core.add.bbox_metadata import add_bbox_metadata
        from geoparquet_io.core.exceptions import GeoParquetError

        # Test various remote URL formats
        remote_urls = [
            "s3://bucket/file.parquet",
            "gs://bucket/file.parquet",
            "az://container/file.parquet",
            "https://example.com/file.parquet",
        ]

        for url in remote_urls:
            with pytest.raises(GeoParquetError) as exc_info:
                add_bbox_metadata(url)
            assert "Remote URLs are not supported" in str(exc_info.value), (
                f"Expected clear error for {url}, got: {exc_info.value}"
            )
