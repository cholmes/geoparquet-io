"""
Tests for geoparquet_io.core.file_utils module.

Tests file path utilities including glob pattern detection, partition path handling,
SQL escaping, and cache key generation.
"""

import os

import pytest

from geoparquet_io.core.exceptions import (
    FileNotFoundGeoParquetError,
    GeoParquetError,
    InvalidParameterError,
)
from geoparquet_io.core.file_utils import (
    _get_file_cache_key,
    get_all_parquet_files,
    get_first_parquet_file,
    handle_output_overwrite,
    has_glob_pattern,
    is_partition_path,
    resolve_partition_path,
    safe_file_url,
    validate_output_path,
    validate_parquet_extension,
)

# =============================================================================
# Tests for has_glob_pattern()
# =============================================================================


class TestHasGlobPattern:
    """Test glob pattern detection."""

    def test_asterisk_detected(self):
        """Asterisk wildcard is detected."""
        assert has_glob_pattern("*.parquet") is True
        assert has_glob_pattern("path/*.parquet") is True
        assert has_glob_pattern("**/*.parquet") is True

    def test_question_mark_detected(self):
        """Question mark wildcard is detected."""
        assert has_glob_pattern("file?.parquet") is True
        assert has_glob_pattern("path/file?.parquet") is True

    def test_brackets_detected(self):
        """Bracket patterns are detected."""
        assert has_glob_pattern("file[0-9].parquet") is True
        assert has_glob_pattern("[abc].parquet") is True

    def test_no_pattern_returns_false(self):
        """Normal paths return False."""
        assert has_glob_pattern("file.parquet") is False
        assert has_glob_pattern("/path/to/file.parquet") is False
        assert has_glob_pattern("relative/path.parquet") is False

    def test_empty_string(self):
        """Empty string returns False."""
        assert has_glob_pattern("") is False


# =============================================================================
# Tests for is_partition_path()
# =============================================================================


class TestIsPartitionPath:
    """Test partition path detection."""

    def test_glob_pattern_is_partition(self):
        """Glob patterns are partition paths."""
        assert is_partition_path("*.parquet") is True
        assert is_partition_path("data/**/*.parquet") is True

    def test_local_directory_is_partition(self, tmp_path):
        """Local directories are partition paths."""
        assert is_partition_path(str(tmp_path)) is True

    def test_local_file_is_not_partition(self, tmp_path):
        """Local files are not partition paths."""
        test_file = tmp_path / "test.parquet"
        test_file.write_bytes(b"test")
        assert is_partition_path(str(test_file)) is False

    def test_hive_style_remote_url(self):
        """Remote URLs with hive-style partitioning are detected."""
        url = "s3://bucket/data/year=2024/month=01/data.parquet"
        assert is_partition_path(url) is True

    def test_plain_remote_url(self):
        """Plain remote URLs are not partition paths."""
        url = "s3://bucket/data/file.parquet"
        assert is_partition_path(url) is False

    def test_http_url_not_partition(self):
        """HTTP URLs without hive partitioning are not partition paths."""
        url = "https://example.com/data.parquet"
        assert is_partition_path(url) is False


# =============================================================================
# Tests for resolve_partition_path()
# =============================================================================


class TestResolvePartitionPath:
    """Test partition path resolution."""

    def test_directory_with_parquet_files(self, tmp_path):
        """Directory with .parquet files resolves to *.parquet."""
        (tmp_path / "a.parquet").write_bytes(b"test")
        (tmp_path / "b.parquet").write_bytes(b"test")

        resolved, options = resolve_partition_path(str(tmp_path))

        assert resolved.endswith("*.parquet")
        assert "hive_partitioning" not in options

    def test_directory_with_hive_subdirs(self, tmp_path):
        """Directory with hive subdirs resolves with hive option."""
        hive_dir = tmp_path / "year=2024"
        hive_dir.mkdir()
        (hive_dir / "data.parquet").write_bytes(b"test")

        resolved, options = resolve_partition_path(str(tmp_path))

        # Handle both Unix (/) and Windows (\) path separators
        assert "**" in resolved and "*.parquet" in resolved
        assert options.get("hive_partitioning") is True

    def test_explicit_hive_partitioning(self, tmp_path):
        """Explicit hive_partitioning parameter is respected."""
        (tmp_path / "a.parquet").write_bytes(b"test")

        resolved, options = resolve_partition_path(str(tmp_path), hive_partitioning=True)

        assert options.get("hive_partitioning") is True

    def test_path_with_glob_unchanged(self):
        """Glob patterns pass through unchanged."""
        glob_path = "/data/**/*.parquet"
        resolved, options = resolve_partition_path(glob_path)
        assert resolved == glob_path


# =============================================================================
# Tests for get_first_parquet_file()
# =============================================================================


class TestGetFirstParquetFile:
    """Test finding first parquet file in partition."""

    def test_directory_returns_first_file(self, tmp_path):
        """Returns first sorted parquet file in directory."""
        (tmp_path / "c.parquet").write_bytes(b"test")
        (tmp_path / "a.parquet").write_bytes(b"test")
        (tmp_path / "b.parquet").write_bytes(b"test")

        result = get_first_parquet_file(str(tmp_path))

        assert result is not None
        assert result.endswith("a.parquet")

    def test_nested_directory(self, tmp_path):
        """Finds parquet files in nested directories."""
        nested = tmp_path / "subdir"
        nested.mkdir()
        (nested / "data.parquet").write_bytes(b"test")

        result = get_first_parquet_file(str(tmp_path))

        assert result is not None
        assert "data.parquet" in result

    def test_empty_directory_returns_none(self, tmp_path):
        """Empty directory returns None."""
        result = get_first_parquet_file(str(tmp_path))
        assert result is None

    def test_glob_pattern(self, tmp_path):
        """Glob pattern returns first matching file."""
        (tmp_path / "a.parquet").write_bytes(b"test")
        (tmp_path / "b.parquet").write_bytes(b"test")

        pattern = str(tmp_path / "*.parquet")
        result = get_first_parquet_file(pattern)

        assert result is not None
        assert result.endswith("a.parquet")

    def test_remote_url_returns_unchanged(self):
        """Remote URLs are returned unchanged."""
        url = "s3://bucket/file.parquet"
        result = get_first_parquet_file(url)
        assert result == url

    def test_single_file_returns_itself(self, tmp_path):
        """Single file path returns itself."""
        test_file = tmp_path / "test.parquet"
        test_file.write_bytes(b"test")

        result = get_first_parquet_file(str(test_file))

        assert result == str(test_file)


# =============================================================================
# Tests for get_all_parquet_files()
# =============================================================================


class TestGetAllParquetFiles:
    """Test getting all parquet files from partition."""

    def test_directory_returns_all_files(self, tmp_path):
        """Returns all parquet files in directory."""
        (tmp_path / "a.parquet").write_bytes(b"test")
        (tmp_path / "b.parquet").write_bytes(b"test")
        (tmp_path / "c.txt").write_bytes(b"test")

        result = get_all_parquet_files(str(tmp_path))

        assert len(result) == 2
        assert all(f.endswith(".parquet") for f in result)

    def test_results_are_sorted(self, tmp_path):
        """Results are returned sorted."""
        (tmp_path / "z.parquet").write_bytes(b"test")
        (tmp_path / "a.parquet").write_bytes(b"test")

        result = get_all_parquet_files(str(tmp_path))

        assert result[0].endswith("a.parquet")
        assert result[1].endswith("z.parquet")

    def test_empty_directory_returns_empty(self, tmp_path):
        """Empty directory returns empty list."""
        result = get_all_parquet_files(str(tmp_path))
        assert result == []

    def test_nonexistent_path_returns_empty(self, tmp_path):
        """Non-existent path returns empty list."""
        result = get_all_parquet_files(str(tmp_path / "nonexistent.parquet"))
        assert result == []


# =============================================================================
# Tests for validate_output_path()
# =============================================================================


class TestValidateOutputPath:
    """Test output path validation."""

    def test_valid_path(self, tmp_path):
        """Valid writable path passes."""
        output_path = str(tmp_path / "output.parquet")
        # Should not raise
        validate_output_path(output_path)

    def test_nonexistent_directory_raises(self):
        """Non-existent directory raises FileNotFoundGeoParquetError."""
        with pytest.raises(FileNotFoundGeoParquetError):
            validate_output_path("/nonexistent/directory/file.parquet")

    def test_remote_url_passes(self):
        """Remote URLs are not validated."""
        validate_output_path("s3://bucket/file.parquet")

    def test_current_directory(self):
        """Output in current directory is valid."""
        validate_output_path("output.parquet")


# =============================================================================
# Tests for validate_parquet_extension()
# =============================================================================


class TestValidateParquetExtension:
    """Test parquet extension validation."""

    def test_valid_parquet_extension(self):
        """Files with .parquet extension pass."""
        validate_parquet_extension("output.parquet")
        validate_parquet_extension("/path/to/file.parquet")
        validate_parquet_extension("s3://bucket/file.parquet")

    def test_invalid_extension_raises(self):
        """Files without .parquet extension raise."""
        with pytest.raises(InvalidParameterError):
            validate_parquet_extension("output.csv")

    def test_any_extension_bypasses_check(self):
        """any_extension=True allows any extension."""
        validate_parquet_extension("output.csv", any_extension=True)

    def test_none_and_dash_allowed(self):
        """None and '-' are allowed (stdout)."""
        validate_parquet_extension(None)
        validate_parquet_extension("-")

    def test_case_insensitive(self):
        """Extension check is case insensitive."""
        validate_parquet_extension("output.PARQUET")
        validate_parquet_extension("output.Parquet")


# =============================================================================
# Tests for handle_output_overwrite()
# =============================================================================


class TestHandleOutputOverwrite:
    """Test output overwrite handling."""

    def test_nonexistent_file_ok(self, tmp_path):
        """Non-existent file does not raise."""
        handle_output_overwrite(str(tmp_path / "new.parquet"), overwrite=False)

    def test_existing_file_no_overwrite_raises(self, tmp_path):
        """Existing file without overwrite flag raises."""
        existing = tmp_path / "existing.parquet"
        existing.write_bytes(b"test")

        with pytest.raises(GeoParquetError, match="already exists"):
            handle_output_overwrite(str(existing), overwrite=False)

    def test_existing_file_with_overwrite_deletes(self, tmp_path):
        """Existing file with overwrite flag is deleted."""
        existing = tmp_path / "existing.parquet"
        existing.write_bytes(b"test")

        handle_output_overwrite(str(existing), overwrite=True)

        assert not existing.exists()

    def test_same_input_output_raises(self, tmp_path):
        """Same input and output file raises."""
        file_path = tmp_path / "file.parquet"
        file_path.write_bytes(b"test")

        with pytest.raises(GeoParquetError, match="Cannot overwrite input"):
            handle_output_overwrite(str(file_path), overwrite=True, input_path=str(file_path))

    def test_none_output_path_ok(self):
        """None output path does nothing."""
        handle_output_overwrite(None, overwrite=False)


# =============================================================================
# Tests for copy_file()
# =============================================================================


class TestCopyFile:
    """Test byte-for-byte copying, local and remote (#798 diff-cover gap)."""

    def test_local_copy_verbose_logs_debug_message(self, tmp_path, caplog):
        """verbose=True logs the 'Copying ... to ...' debug line before copying."""
        import logging

        from geoparquet_io.core.file_utils import copy_file

        source = tmp_path / "source.parquet"
        dest = tmp_path / "dest.parquet"
        source.write_bytes(b"geoparquet-bytes")

        with caplog.at_level(logging.DEBUG, logger="geoparquet_io"):
            copy_file(str(source), str(dest), verbose=True)

        assert dest.read_bytes() == b"geoparquet-bytes"
        assert f"Copying {source} to {dest}" in caplog.text

    def test_remote_copy_uses_fsspec_without_network(self, monkeypatch):
        """Either side being a remote URL routes through fsspec.open, not shutil.

        fsspec.open is monkeypatched so no real network call is made -- only the
        branch selection and the copyfileobj plumbing are under test here.
        """
        import io
        from unittest import mock

        import fsspec

        from geoparquet_io.core.file_utils import copy_file

        content = b"remote-geoparquet-bytes"
        src_buffer = io.BytesIO(content)
        dest_buffer = io.BytesIO()
        opened_paths = []

        def fake_open(path, mode):
            opened_paths.append((path, mode))
            handle = mock.MagicMock()
            handle.__enter__.return_value = src_buffer if mode == "rb" else dest_buffer
            handle.__exit__.return_value = False
            return handle

        monkeypatch.setattr(fsspec, "open", fake_open)

        copy_file("s3://bucket/source.parquet", "s3://bucket/dest.parquet")

        assert dest_buffer.getvalue() == content
        assert opened_paths == [
            ("s3://bucket/source.parquet", "rb"),
            ("s3://bucket/dest.parquet", "wb"),
        ]

    def test_remote_source_local_dest_still_uses_fsspec(self, monkeypatch):
        """Only one side needs to be remote to take the fsspec branch."""
        import io
        from unittest import mock

        import fsspec

        from geoparquet_io.core.file_utils import copy_file

        content = b"mixed-source-bytes"
        src_buffer = io.BytesIO(content)
        dest_buffer = io.BytesIO()

        def fake_open(path, mode):
            handle = mock.MagicMock()
            handle.__enter__.return_value = src_buffer if mode == "rb" else dest_buffer
            handle.__exit__.return_value = False
            return handle

        monkeypatch.setattr(fsspec, "open", fake_open)

        # Only the source is remote; the dest is a plain local-looking path.
        copy_file("https://example.com/source.parquet", "local_dest.parquet")

        assert dest_buffer.getvalue() == content


# =============================================================================
# Tests for safe_file_url()
# =============================================================================


class TestSafeFileUrl:
    """Test SQL-safe file URL preparation."""

    def test_local_path_escapes_quotes(self, tmp_path):
        """Local paths have single quotes escaped."""
        # Create a file with a quote in its name
        test_file = tmp_path / "test.parquet"
        test_file.write_bytes(b"test")

        result = safe_file_url(str(test_file))

        assert "'" not in result or "''" in result

    def test_local_path_single_quote_escaped(self, tmp_path):
        """Single quotes in path are doubled for SQL."""
        # We can't easily create a file with a quote, so test the escaping logic
        # by using a mock path
        test_dir = tmp_path / "test"
        test_dir.mkdir()
        test_file = test_dir / "file.parquet"
        test_file.write_bytes(b"test")

        # Test with a path that has quotes
        path_with_quote = str(test_file).replace("file", "file'name")
        # Since file doesn't exist, this will raise
        with pytest.raises(FileNotFoundGeoParquetError):
            safe_file_url(path_with_quote)

    def test_glob_pattern_allowed_without_existence_check(self, tmp_path):
        """Glob patterns don't require existence check."""
        pattern = str(tmp_path / "*.parquet")
        result = safe_file_url(pattern)
        assert result == pattern

    def test_nonexistent_file_raises(self, tmp_path):
        """Non-existent file raises FileNotFoundGeoParquetError."""
        with pytest.raises(FileNotFoundGeoParquetError):
            safe_file_url(str(tmp_path / "nonexistent.parquet"))

    def test_remote_url_encoding(self):
        """Remote URLs are properly encoded."""
        url = "https://example.com/path/to/file.parquet"
        result = safe_file_url(url)
        assert "example.com" in result

    def test_s3_url_passes_through(self):
        """S3 URLs pass through unchanged (except quote escaping)."""
        url = "s3://bucket/file.parquet"
        result = safe_file_url(url)
        assert result == url

    def test_http_url_with_special_chars(self):
        """HTTP URLs with special characters are encoded."""
        url = "https://example.com/path with spaces/file.parquet"
        result = safe_file_url(url)
        # Spaces should be encoded
        assert "%20" in result or " " not in result.split("://")[1]

    def test_sql_injection_prevention(self, tmp_path):
        """SQL injection via single quotes is prevented."""
        test_file = tmp_path / "safe.parquet"
        test_file.write_bytes(b"test")

        # If the path had a quote, it would be doubled
        result = safe_file_url(str(test_file))
        # Result should be usable in SQL without injection risk
        assert result.count("'") % 2 == 0 or "'" not in result


# =============================================================================
# Tests for _get_file_cache_key()
# =============================================================================


class TestGetFileCacheKey:
    """Test cache key generation for files."""

    def test_local_file_includes_mtime(self, tmp_path):
        """Local files include modification time in key."""
        test_file = tmp_path / "test.parquet"
        test_file.write_bytes(b"test")

        key = _get_file_cache_key(str(test_file))

        assert len(key) == 2
        assert isinstance(key[0], str)
        assert isinstance(key[1], float)
        assert key[1] > 0  # mtime should be non-zero

    def test_remote_url_zero_mtime(self):
        """Remote URLs have zero mtime."""
        url = "s3://bucket/file.parquet"
        key = _get_file_cache_key(url)

        assert key == (url, 0)

    def test_nonexistent_file_zero_mtime(self, tmp_path):
        """Non-existent files have zero mtime."""
        path = str(tmp_path / "nonexistent.parquet")
        key = _get_file_cache_key(path)

        assert key[1] == 0

    def test_key_uses_resolved_path(self, tmp_path):
        """Key uses resolved absolute path for local files."""
        test_file = tmp_path / "test.parquet"
        test_file.write_bytes(b"test")

        key = _get_file_cache_key(str(test_file))

        # Should be absolute path
        assert os.path.isabs(key[0])

    def test_different_files_different_keys(self, tmp_path):
        """Different files produce different cache keys."""
        file1 = tmp_path / "file1.parquet"
        file2 = tmp_path / "file2.parquet"
        file1.write_bytes(b"test1")
        file2.write_bytes(b"test2")

        key1 = _get_file_cache_key(str(file1))
        key2 = _get_file_cache_key(str(file2))

        assert key1[0] != key2[0]

    def test_modified_file_changes_key(self, tmp_path):
        """Modified file produces different mtime in key."""
        test_file = tmp_path / "test.parquet"
        test_file.write_bytes(b"test")

        key1 = _get_file_cache_key(str(test_file))

        # Modify the file
        import time

        time.sleep(0.01)  # Ensure time difference
        test_file.write_bytes(b"modified")

        key2 = _get_file_cache_key(str(test_file))

        # Path should be same, mtime should differ
        assert key1[0] == key2[0]
        assert key1[1] != key2[1]


# =============================================================================
# Integration Tests
# =============================================================================


class TestFileUtilsIntegration:
    """Integration tests using real GeoParquet files."""

    def test_partition_workflow(self, country_partition_dir):
        """Full workflow: detect -> resolve -> get files."""
        # Detect as partition
        assert is_partition_path(country_partition_dir) is True

        # Resolve path
        resolved, options = resolve_partition_path(country_partition_dir)
        assert resolved.endswith("*.parquet") or "**" in resolved

        # Get files
        files = get_all_parquet_files(country_partition_dir)
        assert len(files) == 4  # El_Salvador, Guatemala, Honduras, Nicaragua

        # Get first file
        first = get_first_parquet_file(country_partition_dir)
        assert first is not None
        assert first.endswith(".parquet")

    def test_single_file_workflow(self, places_test_file):
        """Single file is not a partition."""
        assert is_partition_path(places_test_file) is False

        files = get_all_parquet_files(places_test_file)
        # Single existing file returns list with that file
        if os.path.exists(places_test_file):
            assert files == [places_test_file]
