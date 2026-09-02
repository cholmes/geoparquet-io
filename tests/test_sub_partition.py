"""Tests for sub-partition functionality."""

import os
import shutil
import tempfile

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import partition
from tests.conftest import skip_if_geography_unavailable


@pytest.fixture
def cli_runner():
    return CliRunner()


@pytest.fixture
def temp_partition_dir():
    """Create a temp directory with parquet files of varying sizes."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir, ignore_errors=True)


class TestMinSizeOption:
    """Test --min-size option parsing."""

    def test_min_size_option_exists(self, cli_runner):
        """Verify --min-size option is recognized."""
        result = cli_runner.invoke(partition, ["h3", "--help"])
        assert result.exit_code == 0
        assert "--min-size" in result.output

    def test_in_place_option_exists(self, cli_runner):
        """Verify --in-place option is recognized."""
        result = cli_runner.invoke(partition, ["h3", "--help"])
        assert result.exit_code == 0
        assert "--in-place" in result.output


class TestSubPartitionCore:
    """Test sub_partition core functionality."""

    def test_find_large_files_filters_by_size(self, temp_partition_dir):
        """Test that find_large_files correctly filters by size threshold."""
        from geoparquet_io.core.sub_partition import find_large_files

        # Create test files of different sizes
        # Small file: 1KB
        small_data = pa.table({"id": [1], "geometry": [b"POINT(0 0)"]})
        small_path = os.path.join(temp_partition_dir, "small.parquet")
        pq.write_table(small_data, small_path)

        # Large file: create with more rows to exceed threshold
        large_data = pa.table({"id": list(range(10000)), "geometry": [b"POINT(0 0)" * 100] * 10000})
        large_path = os.path.join(temp_partition_dir, "large.parquet")
        pq.write_table(large_data, large_path)

        # Threshold that should only match the large file
        large_size = os.path.getsize(large_path)
        small_size = os.path.getsize(small_path)
        threshold = (large_size + small_size) // 2  # Middle value

        result = find_large_files(temp_partition_dir, min_size_bytes=threshold)

        assert len(result) == 1
        assert result[0] == large_path

    def test_find_large_files_returns_empty_for_no_matches(self, temp_partition_dir):
        """Test that find_large_files returns empty list when no files exceed threshold."""
        from geoparquet_io.core.sub_partition import find_large_files

        # Create small file
        small_data = pa.table({"id": [1]})
        small_path = os.path.join(temp_partition_dir, "small.parquet")
        pq.write_table(small_data, small_path)

        # Threshold larger than any file
        result = find_large_files(temp_partition_dir, min_size_bytes=1000000000)

        assert result == []

    def test_find_large_files_recursive(self, temp_partition_dir):
        """Test that find_large_files searches subdirectories."""
        from geoparquet_io.core.sub_partition import find_large_files

        # Create nested file
        subdir = os.path.join(temp_partition_dir, "subdir")
        os.makedirs(subdir)
        data = pa.table({"id": list(range(1000))})
        nested_path = os.path.join(subdir, "nested.parquet")
        pq.write_table(data, nested_path)

        result = find_large_files(temp_partition_dir, min_size_bytes=1)

        assert len(result) == 1
        assert result[0] == nested_path


class TestSubPartitionExecution:
    """Test sub_partition_directory function."""

    def test_sub_partition_creates_subdirectories(self, temp_partition_dir):
        """Test that sub_partition_directory creates sub-partitions for large files."""
        from pathlib import Path

        from geoparquet_io.core.sub_partition import sub_partition_directory

        # Copy the buildings test file to our temp directory
        buildings_file = Path(__file__).parent / "data" / "buildings_test.parquet"
        large_path = os.path.join(temp_partition_dir, "large.parquet")
        shutil.copy(buildings_file, large_path)

        # Get file size and use threshold just below it
        file_size = os.path.getsize(large_path)
        threshold = file_size - 100

        result = sub_partition_directory(
            directory=temp_partition_dir,
            partition_type="h3",
            min_size_bytes=threshold,
            resolution=4,
            in_place=True,
            verbose=False,
        )

        # Original file should be gone
        assert not os.path.exists(large_path)

        # Sub-partition directory should exist
        subdir = os.path.join(temp_partition_dir, "large_h3")
        assert os.path.isdir(subdir)

        # Should have some partition files
        partition_files = list(Path(subdir).glob("*.parquet"))
        assert len(partition_files) > 0

        assert result["processed"] == 1
        assert result["skipped"] == 0

    def test_sub_partition_skips_small_files(self, temp_partition_dir):
        """Test that sub_partition_directory skips files below threshold."""
        from geoparquet_io.core.sub_partition import sub_partition_directory

        # Create small file
        data = pa.table({"id": [1], "geometry": [b"POINT(0 0)"]})
        small_path = os.path.join(temp_partition_dir, "small.parquet")
        pq.write_table(data, small_path)

        result = sub_partition_directory(
            directory=temp_partition_dir,
            partition_type="h3",
            min_size_bytes=1000000000,  # 1GB - way bigger than file
            resolution=4,
            in_place=True,
            verbose=False,
        )

        # File should still exist
        assert os.path.exists(small_path)
        assert result["processed"] == 0

    def test_sub_partition_handles_errors(self, temp_partition_dir, monkeypatch):
        """Test that sub_partition_directory captures errors and preserves files on failure."""
        from pathlib import Path

        from geoparquet_io.core.sub_partition import sub_partition_directory

        # Copy the buildings test file to our temp directory
        buildings_file = Path(__file__).parent / "data" / "buildings_test.parquet"
        large_path = os.path.join(temp_partition_dir, "large.parquet")
        shutil.copy(buildings_file, large_path)

        # Get file size and use threshold just below it
        file_size = os.path.getsize(large_path)
        threshold = file_size - 100

        # Mock the partition function to raise an error
        def mock_partition_fail(*args, **kwargs):
            raise ValueError("Simulated partition failure")

        # Patch the h3 partition function to fail - patch where it's imported
        monkeypatch.setattr(
            "geoparquet_io.core.partition.by_h3.partition_by_h3", mock_partition_fail
        )

        result = sub_partition_directory(
            directory=temp_partition_dir,
            partition_type="h3",
            min_size_bytes=threshold,
            resolution=4,
            in_place=True,
            verbose=False,
        )

        # Original file should still exist (not deleted due to error)
        assert os.path.exists(large_path)

        # Should have captured the error
        assert result["processed"] == 0
        assert len(result["errors"]) == 1
        assert result["errors"][0]["file"] == large_path
        assert "Simulated partition failure" in result["errors"][0]["error"]


class TestSubPartitionCLI:
    """Test CLI integration for sub-partitioning."""

    def test_partition_h3_with_directory_and_min_size(self, cli_runner, temp_partition_dir):
        """Test gpio partition h3 with directory input and --min-size."""
        from pathlib import Path

        # Copy the buildings test file to our temp directory
        buildings_file = Path(__file__).parent / "data" / "buildings_test.parquet"
        test_file = os.path.join(temp_partition_dir, "test.parquet")
        shutil.copy(buildings_file, test_file)

        file_size = os.path.getsize(test_file)

        # Run with --min-size just below file size (use B suffix for bytes)
        # Use --force to bypass small partition warnings (test file only has 42 rows)
        result = cli_runner.invoke(
            partition,
            [
                "h3",
                temp_partition_dir,
                "--min-size",
                f"{file_size - 100}B",
                "--resolution",
                "4",
                "--in-place",
                "--force",
            ],
        )

        assert result.exit_code == 0, f"Failed: {result.output}"

        # Original should be gone
        assert not os.path.exists(test_file)

        # Sub-partition dir should exist
        subdir = os.path.join(temp_partition_dir, "test_h3")
        assert os.path.isdir(subdir)

    def test_partition_h3_directory_requires_min_size(self, cli_runner, temp_partition_dir):
        """Test that directory input without --min-size gives error."""
        result = cli_runner.invoke(
            partition,
            ["h3", temp_partition_dir, "--resolution", "4"],
        )
        assert result.exit_code != 0
        assert "min-size" in result.output.lower() or "directory" in result.output.lower()

    def test_partition_s2_with_directory_and_min_size(self, cli_runner, temp_partition_dir):
        """Test gpio partition s2 with directory input and --min-size."""
        from pathlib import Path

        skip_if_geography_unavailable()

        # Copy the buildings test file to our temp directory
        buildings_file = Path(__file__).parent / "data" / "buildings_test.parquet"
        test_file = os.path.join(temp_partition_dir, "test.parquet")
        shutil.copy(buildings_file, test_file)

        file_size = os.path.getsize(test_file)

        result = cli_runner.invoke(
            partition,
            [
                "s2",
                temp_partition_dir,
                "--min-size",
                f"{file_size - 100}B",
                "--level",
                "8",
                "--in-place",
                "--force",
            ],
        )

        assert result.exit_code == 0, f"Failed: {result.output}"
        assert not os.path.exists(test_file)
        assert os.path.isdir(os.path.join(temp_partition_dir, "test_s2"))

    def test_partition_quadkey_with_directory_and_min_size(self, cli_runner, temp_partition_dir):
        """Test gpio partition quadkey with directory input and --min-size."""
        from pathlib import Path

        # Copy the buildings test file to our temp directory
        buildings_file = Path(__file__).parent / "data" / "buildings_test.parquet"
        test_file = os.path.join(temp_partition_dir, "test.parquet")
        shutil.copy(buildings_file, test_file)

        file_size = os.path.getsize(test_file)

        result = cli_runner.invoke(
            partition,
            [
                "quadkey",
                temp_partition_dir,
                "--min-size",
                f"{file_size - 100}B",
                "--auto",
                "--in-place",
                "--force",
            ],
        )

        assert result.exit_code == 0, f"Failed: {result.output}"
        assert not os.path.exists(test_file)
        assert os.path.isdir(os.path.join(temp_partition_dir, "test_quadkey"))


class TestSubPartitionFailuresAreReported:
    """A directory sub-partition that partitions nothing must not exit 0 (#778)."""

    @staticmethod
    def _seed(directory) -> int:
        from pathlib import Path

        buildings = Path(__file__).parent / "data" / "buildings_test.parquet"
        target = os.path.join(directory, "test.parquet")
        shutil.copy(buildings, target)
        return os.path.getsize(target)

    def test_a_failing_file_makes_the_command_exit_non_zero(
        self, cli_runner, temp_partition_dir, monkeypatch
    ):
        """Every per-file error was caught and warned about, then the exit code
        said success -- so a run that partitioned nothing looked like a clean one."""
        size = self._seed(temp_partition_dir)

        def _boom(**kwargs):
            raise RuntimeError("simulated partition failure")

        # sub_partition_directory imports it inside the function, so patch the source.
        monkeypatch.setattr("geoparquet_io.core.partition.by_quadkey.partition_by_quadkey", _boom)

        result = cli_runner.invoke(
            partition,
            [
                "quadkey",
                temp_partition_dir,
                "--min-size",
                f"{size - 100}B",
                "--auto",
                "--force",
            ],
        )

        assert result.exit_code != 0, f"failures exited 0: {result.output}"
        assert "simulated partition failure" in result.output

    def test_an_unavailable_extension_is_reported_once_not_once_per_file(
        self, cli_runner, temp_partition_dir, monkeypatch
    ):
        """The preflight sits above the file loop, so N files get one message.

        Before this, `gpio partition s2 <dir>/ --min-size` printed the whole
        extension paragraph once per file and still exited 0.
        """
        from geoparquet_io.core.exceptions import ExtensionUnavailableError

        size = self._seed(temp_partition_dir)
        for extra in ("second.parquet", "third.parquet"):
            shutil.copy(
                os.path.join(temp_partition_dir, "test.parquet"),
                os.path.join(temp_partition_dir, extra),
            )

        calls = []

        def _unavailable(name, feature=None):
            calls.append(name)
            raise ExtensionUnavailableError(name, "1.5.5", "HTTP 404", feature=feature)

        monkeypatch.setattr(
            "geoparquet_io.core.duckdb_utils.require_community_extension", _unavailable
        )

        result = cli_runner.invoke(
            partition,
            ["s2", temp_partition_dir, "--min-size", f"{size - 100}B", "--level", "8", "--force"],
        )

        assert result.exit_code != 0, f"unavailable extension exited 0: {result.output}"
        assert len(calls) == 1, f"preflight ran {len(calls)} times for 3 files"
        assert result.output.count("paleolimbot/duckdb-geography#34") == 1


class TestA5SubPartitioning:
    """A5 was the one hierarchical index that could not sub-partition (#733).

    S2 is unavailable in this release, so ``gpio partition s2``'s own error
    tells users to switch to A5 -- which made A5's missing ``--min-size`` /
    ``--in-place`` the gap that mattered most.
    """

    def test_min_size_option_exists_on_a5(self, cli_runner):
        result = cli_runner.invoke(partition, ["a5", "--help"])
        assert result.exit_code == 0
        assert "--min-size" in result.output

    def test_in_place_option_exists_on_a5(self, cli_runner):
        result = cli_runner.invoke(partition, ["a5", "--help"])
        assert result.exit_code == 0
        assert "--in-place" in result.output

    def test_sub_partition_directory_supports_a5(self, temp_partition_dir):
        """The registry in core/sub_partition.py used to raise for 'a5'."""
        from pathlib import Path

        from geoparquet_io.core.sub_partition import sub_partition_directory

        buildings_file = Path(__file__).parent / "data" / "buildings_test.parquet"
        large_path = os.path.join(temp_partition_dir, "large.parquet")
        shutil.copy(buildings_file, large_path)

        threshold = os.path.getsize(large_path) - 100

        result = sub_partition_directory(
            directory=temp_partition_dir,
            partition_type="a5",
            min_size_bytes=threshold,
            resolution=4,
            in_place=True,
            force=True,
            verbose=False,
        )

        assert result["errors"] == []
        assert result["processed"] == 1
        assert not os.path.exists(large_path)

        subdir = os.path.join(temp_partition_dir, "large_a5")
        assert os.path.isdir(subdir)
        assert list(Path(subdir).glob("*.parquet"))

    def test_partition_a5_with_directory_and_min_size(self, cli_runner, temp_partition_dir):
        """End to end: gpio partition a5 <dir>/ --min-size ... --in-place."""
        from pathlib import Path

        buildings_file = Path(__file__).parent / "data" / "buildings_test.parquet"
        test_file = os.path.join(temp_partition_dir, "test.parquet")
        shutil.copy(buildings_file, test_file)

        file_size = os.path.getsize(test_file)

        result = cli_runner.invoke(
            partition,
            [
                "a5",
                temp_partition_dir,
                "--min-size",
                f"{file_size - 100}B",
                "--resolution",
                "4",
                "--in-place",
                "--force",
            ],
        )

        assert result.exit_code == 0, f"Failed: {result.output}"
        assert not os.path.exists(test_file)
        assert os.path.isdir(os.path.join(temp_partition_dir, "test_a5"))

    def test_partition_a5_directory_requires_min_size(self, cli_runner, temp_partition_dir):
        result = cli_runner.invoke(
            partition,
            ["a5", temp_partition_dir, "--resolution", "4"],
        )
        assert result.exit_code != 0
        assert "min-size" in result.output.lower() or "directory" in result.output.lower()

    def test_partition_a5_directory_with_auto_resolution(self, cli_runner, temp_partition_dir):
        """--auto has to reach the a5 branch of calculate_auto_resolution."""
        from pathlib import Path

        buildings_file = Path(__file__).parent / "data" / "buildings_test.parquet"
        test_file = os.path.join(temp_partition_dir, "test.parquet")
        shutil.copy(buildings_file, test_file)

        file_size = os.path.getsize(test_file)

        result = cli_runner.invoke(
            partition,
            [
                "a5",
                temp_partition_dir,
                "--min-size",
                f"{file_size - 100}B",
                "--auto",
                "--in-place",
                "--force",
            ],
        )

        assert result.exit_code == 0, f"Failed: {result.output}"
        assert not os.path.exists(test_file)
        assert os.path.isdir(os.path.join(temp_partition_dir, "test_a5"))
