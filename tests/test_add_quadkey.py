"""Tests for add_quadkey_column module."""

import tempfile
import uuid
from pathlib import Path

import pytest
from click.testing import CliRunner

from geoparquet_io.core.add.quadkey import (
    _lat_lon_to_quadkey,
)
from geoparquet_io.core.crs_utils import get_crs_display_name
from tests.conftest import safe_unlink


class TestLatLonToQuadkey:
    """Tests for _lat_lon_to_quadkey function."""

    def test_known_location(self):
        """Test quadkey generation for a known location."""
        # San Francisco area at zoom level 10
        quadkey = _lat_lon_to_quadkey(37.7749, -122.4194, 10)
        assert isinstance(quadkey, str)
        assert len(quadkey) == 10

    def test_equator_prime_meridian(self):
        """Test quadkey at equator/prime meridian."""
        quadkey = _lat_lon_to_quadkey(0.0, 0.0, 5)
        assert isinstance(quadkey, str)
        assert len(quadkey) == 5

    def test_different_resolutions(self):
        """Test that higher resolution produces longer quadkeys."""
        lat, lon = 40.7128, -74.0060  # New York
        qk_low = _lat_lon_to_quadkey(lat, lon, 5)
        qk_high = _lat_lon_to_quadkey(lat, lon, 15)
        assert len(qk_low) == 5
        assert len(qk_high) == 15
        # Higher resolution should start with the lower resolution key
        assert qk_high.startswith(qk_low)


class TestGetCrsDisplayName:
    """Tests for get_crs_display_name function (shared from common.py)."""

    def test_none_crs(self):
        """Test with None CRS."""
        assert get_crs_display_name(None) == "None (OGC:CRS84)"

    def test_string_crs(self):
        """Test with string CRS."""
        assert get_crs_display_name("EPSG:4326") == "EPSG:4326"

    def test_dict_with_name_and_code(self):
        """Test dict with name and code."""
        crs_dict = {"name": "WGS 84", "id": {"authority": "EPSG", "code": 4326}}
        result = get_crs_display_name(crs_dict)
        assert "WGS 84" in result
        assert "4326" in result

    def test_dict_with_only_code(self):
        """Test dict with only code."""
        crs_dict = {"id": {"authority": "EPSG", "code": 4326}}
        assert get_crs_display_name(crs_dict) == "EPSG:4326"

    def test_empty_dict(self):
        """Test with empty dict."""
        assert get_crs_display_name({}) == "PROJJSON object"


class TestAddQuadkeyCommand:
    """Tests for the add quadkey CLI command."""

    @pytest.fixture
    def sample_file(self):
        """Return path to the sample file."""
        return str(Path(__file__).parent / "data" / "sample.parquet")

    @pytest.fixture
    def output_file(self):
        """Create a temp output file path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_quadkey_{uuid.uuid4()}.parquet"
        yield str(tmp_path)
        safe_unlink(tmp_path)

    def test_add_quadkey_help(self):
        """Test that add quadkey command has help."""
        from geoparquet_io.cli.main import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["add", "quadkey", "--help"])
        assert result.exit_code == 0
        assert "quadkey" in result.output.lower()

    def test_add_quadkey_invalid_resolution_via_cli(self, sample_file, output_file):
        """Test with invalid resolution via CLI."""
        from geoparquet_io.cli.main import cli

        runner = CliRunner()
        result = runner.invoke(
            cli, ["add", "quadkey", sample_file, output_file, "--resolution", "25"]
        )
        # Should fail - resolution out of range
        assert result.exit_code != 0


# Column names that are legal in Parquet but are not bare SQL identifiers.
# 'weird name' broke the file-based path with a parser error; 'has"quote'
# broke it with an unterminated-quoted-identifier error (issue #680).
HOSTILE_QUADKEY_NAMES = ["weird name", 'has"quote', "quad-key", "SELECT"]


def _expected_bbox_quadkeys(table, resolution):
    """Golden quadkeys computed in Python from the bbox struct midpoints."""
    return [
        _lat_lon_to_quadkey(
            (b["ymin"] + b["ymax"]) / 2.0,
            (b["xmin"] + b["xmax"]) / 2.0,
            resolution,
        )
        for b in table["bbox"].to_pylist()
    ]


class TestQuadkeyColumnNameQuoting:
    """Regression tests for issue #680: non-identifier --quadkey-name values."""

    @pytest.fixture
    def places_file(self):
        """Points fixture that carries a 'bbox' struct column."""
        return str(Path(__file__).parent / "data" / "places_test.parquet")

    def test_bbox_path_golden_values_with_default_name(self, places_file, tmp_path):
        """Pin the golden quadkey values the bbox fast path must produce."""
        import pyarrow.parquet as pq

        from geoparquet_io.core.add.quadkey import add_quadkey_column

        out = tmp_path / "default.parquet"
        add_quadkey_column(places_file, str(out), resolution=13)
        result = pq.read_table(str(out))
        source = pq.read_table(places_file)

        assert result["quadkey"].to_pylist() == _expected_bbox_quadkeys(source, 13)
        # Row 0 is POINT (-0.9247532486915588 9.85634708404541); the literal below
        # was derived from the Slippy-tile formula independently of this module.
        assert result["quadkey"][0].as_py() == "0333311123230"

    @pytest.mark.parametrize("name", HOSTILE_QUADKEY_NAMES)
    def test_file_based_bbox_path_hostile_name(self, places_file, tmp_path, name):
        """File-based bbox path: hostile names keep the correct quadkey values."""
        import pyarrow.parquet as pq

        from geoparquet_io.core.add.quadkey import add_quadkey_column

        out = tmp_path / "hostile.parquet"
        add_quadkey_column(places_file, str(out), quadkey_column_name=name, resolution=13)

        result = pq.read_table(str(out))
        assert name in result.column_names
        source = pq.read_table(places_file)
        assert result[name].to_pylist() == _expected_bbox_quadkeys(source, 13)
        assert result.num_rows == source.num_rows

    @pytest.mark.parametrize("name", ["weird name", 'has"quote'])
    def test_file_based_centroid_path_hostile_name(self, places_file, tmp_path, name):
        """File-based centroid path: hostile names keep the correct quadkey values."""
        import pyarrow.parquet as pq

        from geoparquet_io.core.add.quadkey import add_quadkey_column

        default_out = tmp_path / "centroid_default.parquet"
        hostile_out = tmp_path / "centroid_hostile.parquet"
        add_quadkey_column(places_file, str(default_out), resolution=13, use_centroid=True)
        add_quadkey_column(
            places_file,
            str(hostile_out),
            quadkey_column_name=name,
            resolution=13,
            use_centroid=True,
        )

        expected = pq.read_table(str(default_out))["quadkey"].to_pylist()
        actual = pq.read_table(str(hostile_out))[name].to_pylist()
        # Points: centroid keying must agree with bbox-midpoint keying.
        assert expected == _expected_bbox_quadkeys(pq.read_table(places_file), 13)
        assert actual == expected

    @pytest.mark.parametrize("name", ["weird name", 'has"quote'])
    def test_cli_hostile_name_end_to_end(self, places_file, tmp_path, name):
        """CLI e2e: --quadkey-name accepts hostile names and writes correct values."""
        import pyarrow.parquet as pq

        from geoparquet_io.cli.main import cli

        out = tmp_path / "cli.parquet"
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "add",
                "quadkey",
                places_file,
                str(out),
                "--resolution",
                "13",
                "--quadkey-name",
                name,
            ],
        )
        assert result.exit_code == 0, result.output

        table = pq.read_table(str(out))
        assert name in table.column_names
        assert table[name].to_pylist() == _expected_bbox_quadkeys(pq.read_table(places_file), 13)

    @pytest.mark.parametrize("name", ["weird name", 'has"quote'])
    def test_geo_metadata_primary_column_survives_hostile_name(self, places_file, tmp_path, name):
        """Writing a hostile-named quadkey column must not corrupt geo metadata.

        This deliberately does NOT assert that the quadkey `covering` entry
        survives: on inputs that carry a bbox column the quadkey/h3/s2/a5
        covering is dropped before the file is written, for every column name
        including the default. That is a separate pre-existing bug (see issue
        #694); pin covering survival there once it is fixed.
        """
        import json

        import pyarrow.parquet as pq

        from geoparquet_io.core.add.quadkey import add_quadkey_column

        out = tmp_path / "meta.parquet"
        add_quadkey_column(places_file, str(out), quadkey_column_name=name, resolution=13)

        meta = pq.ParquetFile(str(out)).metadata.metadata or {}
        geo = json.loads(meta[b"geo"].decode())
        assert geo["primary_column"] == "geometry"
        assert "geometry" in geo["columns"]

    @pytest.mark.parametrize("name", ["weird name", 'has"quote'])
    def test_table_path_hostile_name(self, places_file, name):
        """Sibling table path already quotes — pin it against regressions."""
        import pyarrow.parquet as pq

        from geoparquet_io.core.add.quadkey import add_quadkey_table

        source = pq.read_table(places_file)
        result = add_quadkey_table(source, quadkey_column_name=name, resolution=13)

        assert name in result.column_names
        assert result[name].to_pylist() == _expected_bbox_quadkeys(source, 13)

    @pytest.mark.parametrize("name", ["weird name", 'has"quote'])
    def test_streaming_path_hostile_name(self, places_file, tmp_path, monkeypatch, name):
        """Sibling streaming path already quotes — pin it against regressions."""
        import io
        import sys
        from unittest import mock

        import pyarrow.ipc as ipc
        import pyarrow.parquet as pq

        from geoparquet_io.core.add.quadkey import add_quadkey_column

        source = pq.read_table(places_file)
        ipc_buffer = io.BytesIO()
        writer = ipc.RecordBatchStreamWriter(ipc_buffer, source.schema)
        writer.write_table(source)
        writer.close()
        ipc_buffer.seek(0)

        mock_stdin = mock.MagicMock()
        mock_stdin.isatty.return_value = False
        mock_stdin.buffer = ipc_buffer
        monkeypatch.setattr(sys, "stdin", mock_stdin)

        out = tmp_path / "streamed.parquet"
        add_quadkey_column("-", str(out), quadkey_column_name=name, resolution=13)

        result = pq.read_table(str(out))
        assert name in result.column_names
        assert result[name].to_pylist() == _expected_bbox_quadkeys(source, 13)
