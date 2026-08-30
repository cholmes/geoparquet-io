"""
Integration tests for Arrow IPC piping between CLI commands.

Tests multi-stage pipelines like:
    gpio add bbox input.parquet | gpio sort hilbert - output.parquet
    gpio extract input.parquet | gpio add bbox - | gpio add quadkey - output.parquet
"""

from __future__ import annotations

import subprocess
import tempfile
import uuid
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from tests.conftest import safe_rmtree, safe_unlink

TEST_DATA_DIR = Path(__file__).parent / "data"
PLACES_PARQUET = TEST_DATA_DIR / "places_test.parquet"


def run_pipeline(commands: list[str], timeout: int = 60) -> subprocess.CompletedProcess:
    """Run a shell pipeline and return the result."""
    pipeline = " | ".join(commands)
    return subprocess.run(
        pipeline,
        shell=True,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


@pytest.fixture
def output_file():
    """Create a temporary output file path."""
    tmp_path = Path(tempfile.gettempdir()) / f"test_pipe_{uuid.uuid4()}.parquet"
    yield str(tmp_path)
    safe_unlink(tmp_path)


@pytest.fixture
def output_dir():
    """Create a temporary output directory."""
    tmp_path = Path(tempfile.gettempdir()) / f"test_pipe_dir_{uuid.uuid4()}"
    tmp_path.mkdir(exist_ok=True)
    yield str(tmp_path)
    safe_rmtree(tmp_path)


class TestTwoStagePipelines:
    """Tests for two-stage command pipelines."""

    @pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
    def test_add_bbox_to_sort_hilbert(self, output_file):
        """Test: gpio add bbox input | gpio sort hilbert - output."""
        # Use --bbox-name to avoid conflict with existing bbox column in test file
        result = run_pipeline(
            [
                f"gpio add bbox --bbox-name bbox_test {PLACES_PARQUET} -",
                f"gpio sort hilbert - {output_file}",
            ]
        )

        assert result.returncode == 0, f"Pipeline failed: {result.stderr}"
        assert Path(output_file).exists()

        # Verify output has bbox_test and is sorted
        table = pq.read_table(output_file)
        assert "bbox_test" in table.column_names
        assert table.num_rows == 766

    @pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
    def test_extract_to_add_bbox(self, output_file):
        """Test: gpio extract --limit 100 input | gpio add bbox - output."""
        # Use --bbox-name to avoid conflict with existing bbox column in test file
        result = run_pipeline(
            [
                f"gpio extract --limit 100 {PLACES_PARQUET} -",
                f"gpio add bbox --bbox-name bbox_test - {output_file}",
            ]
        )

        assert result.returncode == 0, f"Pipeline failed: {result.stderr}"
        assert Path(output_file).exists()

        table = pq.read_table(output_file)
        assert "bbox_test" in table.column_names
        assert table.num_rows == 100

    @pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
    def test_add_bbox_to_add_quadkey(self, output_file):
        """Test: gpio add bbox input | gpio add quadkey - output."""
        # Use --bbox-name to avoid conflict with existing bbox column in test file
        result = run_pipeline(
            [
                f"gpio add bbox --bbox-name bbox_test {PLACES_PARQUET} -",
                f"gpio add quadkey - {output_file}",
            ]
        )

        assert result.returncode == 0, f"Pipeline failed: {result.stderr}"
        assert Path(output_file).exists()

        table = pq.read_table(output_file)
        assert "bbox_test" in table.column_names
        assert "quadkey" in table.column_names
        assert table.num_rows == 766


class TestThreeStagePipelines:
    """Tests for three-stage command pipelines."""

    @pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
    def test_extract_add_bbox_add_quadkey(self, output_file):
        """Test: extract | add bbox | add quadkey."""
        # Use --bbox-name to avoid conflict with existing bbox column in test file
        result = run_pipeline(
            [
                f"gpio extract --limit 50 {PLACES_PARQUET} -",
                "gpio add bbox --bbox-name bbox_test - -",
                f"gpio add quadkey - {output_file}",
            ]
        )

        assert result.returncode == 0, f"Pipeline failed: {result.stderr}"
        assert Path(output_file).exists()

        table = pq.read_table(output_file)
        assert "bbox_test" in table.column_names
        assert "quadkey" in table.column_names
        assert table.num_rows == 50

    @pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
    def test_add_bbox_add_quadkey_sort_hilbert(self, output_file):
        """Test: add bbox | add quadkey | sort hilbert."""
        # Use --bbox-name to avoid conflict with existing bbox column in test file
        result = run_pipeline(
            [
                f"gpio add bbox --bbox-name bbox_test {PLACES_PARQUET} -",
                "gpio add quadkey - -",
                f"gpio sort hilbert - {output_file}",
            ]
        )

        assert result.returncode == 0, f"Pipeline failed: {result.stderr}"
        assert Path(output_file).exists()

        table = pq.read_table(output_file)
        assert "bbox_test" in table.column_names
        assert "quadkey" in table.column_names
        assert table.num_rows == 766


class TestPartitionWithPipes:
    """Tests for partition command with stdin input."""

    @pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
    def test_add_quadkey_to_partition(self, output_dir):
        """Test: add quadkey | partition string (stdin to directory)."""
        result = run_pipeline(
            [
                f"gpio add quadkey {PLACES_PARQUET} -",
                f"gpio partition string --column quadkey --chars 2 - {output_dir}",
            ]
        )

        assert result.returncode == 0, f"Pipeline failed: {result.stderr}"

        # Check that partitioned files were created
        output_path = Path(output_dir)
        parquet_files = list(output_path.glob("**/*.parquet"))
        assert len(parquet_files) > 0, "No partitioned files created"


class TestFullPipeline:
    """Tests for full multi-stage pipelines."""

    @pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
    def test_full_transform_pipeline(self, output_file):
        """Test: extract | add bbox | add quadkey | sort hilbert."""
        # Use --bbox-name to avoid conflict with existing bbox column in test file
        result = run_pipeline(
            [
                f"gpio extract --limit 100 {PLACES_PARQUET} -",
                "gpio add bbox --bbox-name bbox_test - -",
                "gpio add quadkey - -",
                f"gpio sort hilbert - {output_file}",
            ]
        )

        assert result.returncode == 0, f"Pipeline failed: {result.stderr}"
        assert Path(output_file).exists()

        table = pq.read_table(output_file)
        assert "bbox_test" in table.column_names
        assert "quadkey" in table.column_names
        assert table.num_rows == 100


class TestEdgeCases:
    """Tests for edge cases and error handling."""

    @pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
    def test_single_row_pipeline(self, output_file):
        """Test pipeline with single row extract."""
        # Use --bbox-name to avoid conflict with existing bbox column in test file
        result = run_pipeline(
            [
                f"gpio extract --limit 1 {PLACES_PARQUET} -",
                f"gpio add bbox --bbox-name bbox_test - {output_file}",
            ]
        )

        assert result.returncode == 0, f"Pipeline failed: {result.stderr}"

        table = pq.read_table(output_file)
        assert table.num_rows == 1
        assert "bbox_test" in table.column_names

    @pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
    def test_column_selection_through_pipe(self, output_file):
        """Test that column selection works through pipe."""
        # Use --bbox-name to avoid conflict with existing bbox column in test file
        result = run_pipeline(
            [
                f"gpio extract --include-cols name,address {PLACES_PARQUET} -",
                f"gpio add bbox --bbox-name bbox_test - {output_file}",
            ]
        )

        assert result.returncode == 0, f"Pipeline failed: {result.stderr}"

        table = pq.read_table(output_file)
        # Should have: name, address, geometry (auto-included), bbox_test (added)
        assert "name" in table.column_names
        assert "address" in table.column_names
        assert "geometry" in table.column_names
        assert "bbox_test" in table.column_names


class TestStdinToNamedGeoJsonOutput:
    """#723: `gpio convert geojson - out.geojson` failed with "File not found: -".

    The message was wrong about what happened -- `-` is understood a moment
    earlier in the redirect form, and the named path is the *output*. The
    streaming converter already writes a FeatureCollection to a named path, so
    the pipeline works once the CLI stops routing stdin through the file-mode
    writer that can only open a path.
    """

    @pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
    def test_stdin_to_named_geojson_file(self, tmp_path):
        import json

        output = tmp_path / "out.geojson"
        result = run_pipeline(
            [
                f"gpio extract --limit 5 {PLACES_PARQUET} -",
                f"gpio convert geojson - {output}",
            ]
        )

        assert result.returncode == 0, f"Pipeline failed: {result.stderr}"

        data = json.loads(output.read_text())
        assert data["type"] == "FeatureCollection"
        assert len(data["features"]) == 5
        assert all(f["geometry"] is not None for f in data["features"])

    @pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
    def test_stdin_redirect_form_still_works(self, tmp_path):
        """The documented form must keep working."""
        import json

        output = tmp_path / "out.geojson"
        result = subprocess.run(
            f"gpio extract --limit 5 {PLACES_PARQUET} - | "
            f"gpio convert geojson - --feature-collection --no-rs > {output}",
            shell=True,
            capture_output=True,
            text=True,
            timeout=60,
        )

        assert result.returncode == 0, f"Pipeline failed: {result.stderr}"
        assert json.loads(output.read_text())["type"] == "FeatureCollection"

    @pytest.mark.skipif(not PLACES_PARQUET.exists(), reason="Test data not available")
    def test_stdin_to_named_file_with_bbox_and_id_field(self, tmp_path):
        """The combination neither #723 nor #726 covered on its own.

        Routing stdin into file mode sends it through `_build_feature_query`,
        the function #726 fixed. Before that fix this pipeline did not fail --
        it silently wrote truncated, unparsable GeoJSON, which is worse than
        the `File not found: -` it replaced. Guard the intersection.
        """
        import json

        output = tmp_path / "out.geojson"
        result = run_pipeline(
            [
                f"gpio extract --limit 5 {PLACES_PARQUET} -",
                f"gpio convert geojson - {output} --write-bbox --id-field name",
            ]
        )

        assert result.returncode == 0, f"Pipeline failed: {result.stderr}"

        data = json.loads(output.read_text())
        assert data["type"] == "FeatureCollection"
        assert len(data["features"]) == 5
        for feature in data["features"]:
            assert len(feature["bbox"]) == 4
            assert feature["geometry"] is not None
            assert "properties" in feature
