"""
Tests for the Python API (fluent Table class and ops module).
"""

from __future__ import annotations

import struct
import tempfile
import uuid
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geoparquet_io.api import Table, convert, ops, pipe, read
from tests.conftest import safe_unlink, skip_if_geography_available

TEST_DATA_DIR = Path(__file__).parent / "data"
PLACES_PARQUET = TEST_DATA_DIR / "places_test.parquet"

#: Little-endian WKB POINTs, for building geometry columns without a fixture file.
_WKB_POINT_0 = b"\x01\x01\x00\x00\x00" + struct.pack("<dd", 0.0, 0.0)
_WKB_POINT_1 = b"\x01\x01\x00\x00\x00" + struct.pack("<dd", 1.0, 1.0)


class TestRead:
    """Tests for gpio.read() entry point."""

    def test_read_returns_table(self):
        """Test that read() returns a Table instance."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")

        table = read(PLACES_PARQUET)
        assert isinstance(table, Table)

    def test_read_preserves_rows(self):
        """Test that read() preserves row count."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")

        table = read(PLACES_PARQUET)
        assert table.num_rows == 766

    def test_read_detects_geometry(self):
        """Test that read() detects geometry column."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")

        table = read(PLACES_PARQUET)
        assert table.geometry_column == "geometry"


class TestTable:
    """Tests for the Table class."""

    @pytest.fixture
    def sample_table(self):
        """Create a sample Table from test data."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")
        return read(PLACES_PARQUET)

    @pytest.fixture
    def output_file(self):
        """Create a temporary output file path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_api_{uuid.uuid4()}.parquet"
        yield str(tmp_path)
        safe_unlink(tmp_path)

    def test_table_repr(self, sample_table):
        """Test Table string representation."""
        repr_str = repr(sample_table)
        assert "Table(" in repr_str
        assert "rows=766" in repr_str
        assert "geometry='geometry'" in repr_str

    def test_to_arrow(self, sample_table):
        """Test converting to PyArrow Table."""
        arrow_table = sample_table.to_arrow()
        assert isinstance(arrow_table, pa.Table)
        assert arrow_table.num_rows == 766

    def test_column_names(self, sample_table):
        """Test getting column names."""
        names = sample_table.column_names
        assert "geometry" in names
        assert "name" in names

    def test_add_bbox(self, sample_table):
        """Test add_bbox() method."""
        result = sample_table.add_bbox()
        assert isinstance(result, Table)
        assert "bbox" in result.column_names
        assert result.num_rows == 766

    def test_add_bbox_custom_name(self, sample_table):
        """Test add_bbox() with custom column name."""
        result = sample_table.add_bbox(column_name="bounds")
        assert "bounds" in result.column_names

    def test_add_quadkey(self, sample_table):
        """Test add_quadkey() method."""
        result = sample_table.add_quadkey(resolution=10)
        assert isinstance(result, Table)
        assert "quadkey" in result.column_names
        assert result.num_rows == 766

    def test_sort_hilbert(self, sample_table):
        """Test sort_hilbert() method."""
        result = sample_table.sort_hilbert()
        assert isinstance(result, Table)
        assert result.num_rows == 766

    def test_sort_str(self, sample_table):
        """Test sort_str() method."""
        result = sample_table.sort_str(tile_size=100)
        assert isinstance(result, Table)
        assert result.num_rows == 766

    def test_extract_columns(self, sample_table):
        """Test extract() with column selection."""
        result = sample_table.extract(columns=["name", "address"])
        assert "name" in result.column_names
        assert "address" in result.column_names
        # geometry is auto-included
        assert "geometry" in result.column_names

    def test_extract_limit(self, sample_table):
        """Test extract() with row limit."""
        result = sample_table.extract(limit=10)
        assert result.num_rows == 10

    def test_extract_excluding_geometry_is_writable(self, sample_table):
        """Dropping geometry yields an attribute table that write() can emit.

        extract() used to keep pointing at the excluded geometry column, so
        write() failed deep in the writer with a KeyError naming it (#731).
        """
        result = sample_table.extract(columns=["name", "address"], exclude_columns=["geometry"])
        assert "geometry" not in result.column_names

        with tempfile.TemporaryDirectory() as tmpdir:
            out = Path(tmpdir) / "attributes.parquet"
            result.write(out)
            written = pq.read_table(out)
            assert "geometry" not in written.column_names
            assert "name" in written.column_names
            # No geometry column left, so the file is plain Parquet: advertising
            # geo metadata here would name a column the schema does not have.
            assert b"geo" not in (pq.ParquetFile(out).schema_arrow.metadata or {})

    def test_extract_repoints_at_a_surviving_geometry_column(self):
        """Excluding the primary geometry keeps the file GeoParquet if another remains.

        The result used to report ``geometry_column=None`` whenever the primary
        was dropped, which silently threw away the geo metadata of a secondary
        geometry column that was still there.
        """
        import json

        table = pa.table(
            {
                "id": [1, 2],
                "geom_a": [_WKB_POINT_0, _WKB_POINT_1],
                "geom_b": [_WKB_POINT_1, _WKB_POINT_0],
            }
        )
        geo = {
            "version": "1.1.0",
            "primary_column": "geom_a",
            "columns": {"geom_a": {"encoding": "WKB"}, "geom_b": {"encoding": "WKB"}},
        }
        table = table.replace_schema_metadata({b"geo": json.dumps(geo).encode("utf-8")})

        result = Table(table, geometry_column="geom_a").extract(exclude_columns=["geom_a"])

        assert result.geometry_column == "geom_b"

    def test_extract_unknown_column_names_the_missing_one(self, sample_table):
        """Unknown names fail early and name themselves, not another column (#731)."""
        from geoparquet_io.core.exceptions import GeoParquetError

        with pytest.raises(GeoParquetError) as exc_info:
            sample_table.extract(columns=["name", "population"])

        assert "population" in str(exc_info.value)

    def test_chaining(self, sample_table):
        """Test chaining multiple operations."""
        result = sample_table.add_bbox().add_quadkey(resolution=10)
        assert "bbox" in result.column_names
        assert "quadkey" in result.column_names
        assert result.num_rows == 766

    def test_write(self, sample_table, output_file):
        """Test write() method."""
        sample_table.add_bbox().write(output_file)
        assert Path(output_file).exists()

        # Verify output
        loaded = pq.read_table(output_file)
        assert "bbox" in loaded.column_names

    def test_add_h3(self, sample_table):
        """Test add_h3() method."""
        result = sample_table.add_h3()
        assert isinstance(result, Table)
        assert "h3_cell" in result.column_names
        assert result.num_rows == 766

    def test_add_h3_custom_resolution(self, sample_table):
        """Test add_h3() with custom resolution."""
        result = sample_table.add_h3(resolution=5)
        assert "h3_cell" in result.column_names
        assert result.num_rows == 766

    def test_add_h3_custom_column_name(self, sample_table):
        """Test add_h3() with custom column name."""
        result = sample_table.add_h3(column_name="my_h3")
        assert "my_h3" in result.column_names
        assert result.num_rows == 766

    def test_add_s2(self, sample_table):
        """Test add_s2() method."""
        result = sample_table.add_s2()
        assert isinstance(result, Table)
        assert "s2_cell" in result.column_names
        assert result.num_rows == 766

    def test_add_s2_custom_level(self, sample_table):
        """Test add_s2() with custom level."""
        result = sample_table.add_s2(level=10)
        assert "s2_cell" in result.column_names
        assert result.num_rows == 766

    def test_add_s2_custom_column_name(self, sample_table):
        """Test add_s2() with custom column name."""
        result = sample_table.add_s2(column_name="my_s2")
        assert "my_s2" in result.column_names
        assert result.num_rows == 766

    def test_add_kdtree(self, sample_table):
        """Test add_kdtree() method."""
        result = sample_table.add_kdtree()
        assert isinstance(result, Table)
        assert "kdtree_cell" in result.column_names
        assert result.num_rows == 766

    def test_add_kdtree_custom_params(self, sample_table):
        """Test add_kdtree() with custom parameters."""
        result = sample_table.add_kdtree(iterations=5, sample_size=1000)
        assert "kdtree_cell" in result.column_names
        assert result.num_rows == 766

    def test_sort_column(self, sample_table):
        """Test sort_column() method."""
        result = sample_table.sort_column("name")
        assert isinstance(result, Table)
        assert result.num_rows == 766

    def test_sort_column_descending(self, sample_table):
        """Test sort_column() in descending order."""
        result = sample_table.sort_column("name", descending=True)
        assert isinstance(result, Table)
        assert result.num_rows == 766

    def test_sort_quadkey(self, sample_table):
        """Test sort_quadkey() method."""
        result = sample_table.sort_quadkey(resolution=10)
        assert isinstance(result, Table)
        assert result.num_rows == 766
        # Quadkey column should be auto-added
        assert "quadkey" in result.column_names

    def test_sort_quadkey_remove_column(self, sample_table):
        """Test sort_quadkey() with remove_column=True."""
        result = sample_table.sort_quadkey(resolution=10, remove_column=True)
        assert isinstance(result, Table)
        assert result.num_rows == 766
        # Quadkey column should be removed after sorting
        assert "quadkey" not in result.column_names

    def test_reproject(self, sample_table):
        """Test reproject() method."""
        # Reproject to Web Mercator and back to WGS84
        result = sample_table.reproject(target_crs="EPSG:3857")
        assert isinstance(result, Table)
        assert result.num_rows == 766


class TestOps:
    """Tests for the ops module (pure functions)."""

    @pytest.fixture
    def arrow_table(self):
        """Get an Arrow table from test data."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")
        return pq.read_table(PLACES_PARQUET)

    def test_add_bbox(self, arrow_table):
        """Test ops.add_bbox()."""
        result = ops.add_bbox(arrow_table)
        assert isinstance(result, pa.Table)
        assert "bbox" in result.column_names

    def test_add_quadkey(self, arrow_table):
        """Test ops.add_quadkey()."""
        result = ops.add_quadkey(arrow_table, resolution=10)
        assert isinstance(result, pa.Table)
        assert "quadkey" in result.column_names

    def test_sort_hilbert(self, arrow_table):
        """Test ops.sort_hilbert()."""
        result = ops.sort_hilbert(arrow_table)
        assert isinstance(result, pa.Table)
        assert result.num_rows == 766

    def test_sort_str(self, arrow_table):
        """Test ops.sort_str()."""
        result = ops.sort_str(arrow_table, tile_size=100)
        assert isinstance(result, pa.Table)
        assert result.num_rows == 766

    def test_extract(self, arrow_table):
        """Test ops.extract()."""
        result = ops.extract(arrow_table, limit=10)
        assert isinstance(result, pa.Table)
        assert result.num_rows == 10


class TestPipe:
    """Tests for the pipe() composition helper."""

    @pytest.fixture
    def sample_table(self):
        """Create a sample Table from test data."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")
        return read(PLACES_PARQUET)

    def test_pipe_empty(self, sample_table):
        """Test pipe with no operations."""
        transform = pipe()
        result = transform(sample_table)
        assert result is sample_table

    def test_pipe_single(self, sample_table):
        """Test pipe with single operation."""
        transform = pipe(lambda t: t.add_bbox())
        result = transform(sample_table)
        assert "bbox" in result.column_names

    def test_pipe_multiple(self, sample_table):
        """Test pipe with multiple operations."""
        transform = pipe(
            lambda t: t.add_bbox(),
            lambda t: t.add_quadkey(resolution=10),
        )
        result = transform(sample_table)
        assert "bbox" in result.column_names
        assert "quadkey" in result.column_names

    def test_pipe_with_ops(self):
        """Test pipe with ops functions on Arrow table."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")

        arrow_table = pq.read_table(PLACES_PARQUET)
        transform = pipe(
            lambda t: ops.add_bbox(t),
            lambda t: ops.extract(t, limit=10),
        )
        result = transform(arrow_table)
        assert "bbox" in result.column_names
        assert result.num_rows == 10


class TestConvert:
    """Tests for gpio.convert() entry point."""

    @pytest.fixture
    def gpkg_file(self):
        """Get path to test GeoPackage file."""
        path = TEST_DATA_DIR / "buildings_test.gpkg"
        if not path.exists():
            pytest.skip("GeoPackage test data not available")
        return str(path)

    @pytest.fixture
    def geojson_file(self):
        """Get path to test GeoJSON file."""
        path = TEST_DATA_DIR / "buildings_test.geojson"
        if not path.exists():
            pytest.skip("GeoJSON test data not available")
        return str(path)

    @pytest.fixture
    def csv_wkt_file(self):
        """Get path to test CSV file with WKT geometry."""
        path = TEST_DATA_DIR / "points_wkt.csv"
        if not path.exists():
            pytest.skip("CSV WKT test data not available")
        return str(path)

    @pytest.fixture
    def output_file(self):
        """Create a temporary output file path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_convert_{uuid.uuid4()}.parquet"
        yield str(tmp_path)
        safe_unlink(tmp_path)

    def test_convert_geopackage_returns_table(self, gpkg_file):
        """Test that convert() returns a Table for GeoPackage input."""
        table = convert(gpkg_file)
        assert isinstance(table, Table)
        assert table.num_rows > 0

    def test_convert_geojson_returns_table(self, geojson_file):
        """Test that convert() returns a Table for GeoJSON input."""
        table = convert(geojson_file)
        assert isinstance(table, Table)
        assert table.num_rows > 0

    def test_convert_csv_with_wkt(self, csv_wkt_file):
        """Test converting CSV with WKT column."""
        table = convert(csv_wkt_file)
        assert isinstance(table, Table)
        assert "geometry" in table.column_names

    def test_convert_detects_geometry_column(self, gpkg_file):
        """Test that convert() detects geometry column."""
        table = convert(gpkg_file)
        assert table.geometry_column == "geometry"

    def test_convert_with_write(self, csv_wkt_file, output_file):
        """Test writing converted data."""
        # Test that convert -> write chain works (CSV has simpler geometry)
        convert(csv_wkt_file).write(output_file)
        assert Path(output_file).exists()

        # Verify output
        loaded = pq.read_table(output_file)
        assert loaded.num_rows > 0
        assert "geometry" in loaded.column_names


class TestTableUpload:
    """Tests for Table.upload() method."""

    @pytest.fixture
    def sample_table(self):
        """Create a sample Table from test data."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")
        return read(PLACES_PARQUET)

    def test_upload_writes_temp_and_calls_upload(self, sample_table):
        """Test that upload() writes to temp file and calls core upload."""
        with patch("geoparquet_io.core.upload.upload") as mock_upload:
            # Make upload a no-op
            mock_upload.return_value = None

            sample_table.upload("s3://test-bucket/test.parquet")

            # Verify upload was called
            mock_upload.assert_called_once()
            call_args = mock_upload.call_args
            assert call_args.kwargs["destination"] == "s3://test-bucket/test.parquet"

    def test_upload_with_s3_endpoint(self, sample_table):
        """Test upload() with custom S3 endpoint."""
        with patch("geoparquet_io.core.upload.upload") as mock_upload:
            mock_upload.return_value = None

            sample_table.upload(
                "s3://test-bucket/test.parquet",
                s3_endpoint="minio.example.com:9000",
                s3_use_ssl=False,
            )

            call_args = mock_upload.call_args
            assert call_args.kwargs["s3_endpoint"] == "minio.example.com:9000"
            assert call_args.kwargs["s3_use_ssl"] is False

    def test_upload_cleans_up_temp_file(self, sample_table):
        """Test that upload() cleans up temp file even on error."""
        captured_paths = []

        def capture_and_raise(**kwargs):
            captured_paths.append(kwargs["source"])
            raise Exception("Upload failed")

        with patch("geoparquet_io.core.upload.upload") as mock_upload:
            mock_upload.side_effect = capture_and_raise

            with pytest.raises(Exception, match="Upload failed"):
                sample_table.upload("s3://test-bucket/test.parquet")

            # Verify the temp file path was captured and cleaned up
            assert len(captured_paths) == 1
            temp_path = captured_paths[0]
            assert not Path(temp_path).exists(), "Temp file should be deleted after error"


class TestTableMetadataProperties:
    """Tests for the new metadata properties on Table."""

    @pytest.fixture
    def sample_table(self):
        """Create a sample Table from test data."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")
        return read(PLACES_PARQUET)

    def test_crs_property(self, sample_table):
        """Test crs property returns CRS or None."""
        crs = sample_table.crs
        # Can be None (OGC:CRS84 default) or a dict/string
        assert crs is None or isinstance(crs, dict | str)

    def test_bounds_property(self, sample_table):
        """Test bounds property returns tuple."""
        bounds = sample_table.bounds
        assert bounds is not None
        assert isinstance(bounds, tuple)
        assert len(bounds) == 4
        xmin, ymin, xmax, ymax = bounds
        assert xmin < xmax
        assert ymin < ymax

    def test_schema_property(self, sample_table):
        """Test schema property returns PyArrow Schema."""
        import pyarrow as pa

        schema = sample_table.schema
        assert isinstance(schema, pa.Schema)
        assert "geometry" in [field.name for field in schema]

    def test_geoparquet_version_property(self, sample_table):
        """Test geoparquet_version property returns version string."""
        version = sample_table.geoparquet_version
        # Should be a version string like "1.1" or "1.1.0" or None
        assert version is None or isinstance(version, str)
        if version:
            # Accept patched versions like "1.1.0" by checking major.minor
            major_minor = ".".join(version.split(".")[:2])
            assert major_minor in ["1.0", "1.1", "2.0"]


class TestTableInfo:
    """Tests for the info() method."""

    @pytest.fixture
    def sample_table(self):
        """Create a sample Table from test data."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")
        return read(PLACES_PARQUET)

    def test_info_verbose_returns_none(self, sample_table, capsys):
        """Test info(verbose=True) prints output and returns None."""
        result = sample_table.info(verbose=True)
        assert result is None

        captured = capsys.readouterr()
        assert "Table:" in captured.out
        assert "766" in captured.out
        assert "Geometry:" in captured.out

    def test_info_dict_mode(self, sample_table):
        """Test info(verbose=False) returns dict."""
        info = sample_table.info(verbose=False)
        assert isinstance(info, dict)
        assert info["rows"] == 766
        assert "geometry_column" in info
        assert "crs" in info
        assert "bounds" in info
        assert "geoparquet_version" in info
        assert "column_names" in info


class TestWriteReturnsPath:
    """Tests for write() returning Path."""

    @pytest.fixture
    def sample_table(self):
        """Create a sample Table from test data."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")
        return read(PLACES_PARQUET)

    @pytest.fixture
    def output_file(self):
        """Create a temporary output file path."""
        tmp_path = Path(tempfile.gettempdir()) / f"test_write_{uuid.uuid4()}.parquet"
        yield str(tmp_path)
        safe_unlink(tmp_path)

    def test_write_returns_path(self, sample_table, output_file):
        """Test that write() returns a Path object."""
        result = sample_table.write(output_file)
        assert isinstance(result, Path)
        assert result.exists()
        assert str(result) == output_file


class TestOpsNewFunctions:
    """Tests for the new ops module functions."""

    @pytest.fixture
    def arrow_table(self):
        """Get an Arrow table from test data."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")
        return pq.read_table(PLACES_PARQUET)

    def test_add_h3(self, arrow_table):
        """Test ops.add_h3()."""
        result = ops.add_h3(arrow_table, resolution=7)
        assert isinstance(result, pa.Table)
        assert "h3_cell" in result.column_names

    def test_add_s2(self, arrow_table):
        """Test ops.add_s2()."""
        result = ops.add_s2(arrow_table, level=13)
        assert isinstance(result, pa.Table)
        assert "s2_cell" in result.column_names

    def test_add_kdtree(self, arrow_table):
        """Test ops.add_kdtree()."""
        result = ops.add_kdtree(arrow_table, iterations=5)
        assert isinstance(result, pa.Table)
        assert "kdtree_cell" in result.column_names

    def test_sort_column(self, arrow_table):
        """Test ops.sort_column()."""
        result = ops.sort_column(arrow_table, column="name")
        assert isinstance(result, pa.Table)
        assert result.num_rows == 766

    def test_sort_quadkey(self, arrow_table):
        """Test ops.sort_quadkey()."""
        result = ops.sort_quadkey(arrow_table, resolution=10)
        assert isinstance(result, pa.Table)
        assert result.num_rows == 766

    def test_reproject(self, arrow_table):
        """Test ops.reproject()."""
        result = ops.reproject(arrow_table, target_crs="EPSG:3857")
        assert isinstance(result, pa.Table)
        assert result.num_rows == 766


class TestTablePartitionByA5:
    """Tests for Table.partition_by_a5() method."""

    @pytest.fixture
    def sample_table(self):
        """Create a sample Table from test data."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")
        return read(PLACES_PARQUET)

    @pytest.fixture
    def output_dir(self):
        """Create a temporary output directory."""
        tmp_dir = Path(tempfile.gettempdir()) / f"test_part_a5_{uuid.uuid4()}"
        yield tmp_dir
        # Cleanup
        if tmp_dir.exists():
            import shutil

            shutil.rmtree(tmp_dir)

    def test_partition_by_a5_basic(self, sample_table, output_dir):
        """Test basic A5 partitioning."""
        # Use low resolution (4) to ensure partitions have enough rows
        # Higher resolutions create too many tiny partitions with test data
        result = sample_table.partition_by_a5(output_dir, resolution=4, overwrite=True)

        assert isinstance(result, dict)
        assert "file_count" in result
        assert result["file_count"] > 0
        assert output_dir.exists()
        parquet_files = list(output_dir.rglob("*.parquet"))
        assert len(parquet_files) > 0

    def test_partition_by_a5_hive_style(self, sample_table, output_dir):
        """Test Hive-style A5 partitioning."""
        # Use low resolution (4) to ensure partitions have enough rows
        result = sample_table.partition_by_a5(output_dir, resolution=4, hive=True, overwrite=True)

        assert result["file_count"] > 0
        # Check for Hive-style directories
        subdirs = [d for d in output_dir.iterdir() if d.is_dir()]
        assert len(subdirs) > 0
        assert any("a5_cell=" in d.name for d in subdirs)


class TestPartitionKeepColumn:
    """The index column gpio generates must survive a non-Hive partition run.

    Without ``--hive`` / ``hive=True`` the partition value is encoded only in
    the file name, and the generating column is excluded from the output. The
    CLI offers ``--keep-<scheme>-column`` to override that; the Python API must
    offer the same escape hatch or an API caller cannot produce the column at
    all.
    """

    @pytest.fixture
    def sample_table(self):
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")
        return read(PLACES_PARQUET)

    @pytest.fixture
    def output_dir(self):
        tmp_dir = Path(tempfile.gettempdir()) / f"test_part_keep_{uuid.uuid4()}"
        yield tmp_dir
        if tmp_dir.exists():
            import shutil

            shutil.rmtree(tmp_dir)

    @staticmethod
    def _column_names(output_dir):
        files = sorted(Path(output_dir).rglob("*.parquet"))
        assert files, f"no parquet files written to {output_dir}"
        return set(pq.ParquetFile(files[0]).schema_arrow.names)

    def test_non_hive_drops_the_quadkey_column_by_default(self, sample_table, output_dir):
        sample_table.partition_by_quadkey(
            output_dir, resolution=13, partition_resolution=3, overwrite=True
        )
        assert "quadkey" not in self._column_names(output_dir)

    def test_keep_quadkey_column_restores_it_without_hive(self, sample_table, output_dir):
        sample_table.partition_by_quadkey(
            output_dir,
            resolution=13,
            partition_resolution=3,
            overwrite=True,
            keep_quadkey_column=True,
        )
        assert "quadkey" in self._column_names(output_dir)
        # Still flat files, not key=value/ directories.
        assert not [d for d in Path(output_dir).iterdir() if d.is_dir()]

    def test_hive_keeps_the_quadkey_column(self, sample_table, output_dir):
        sample_table.partition_by_quadkey(
            output_dir, resolution=13, partition_resolution=3, overwrite=True, hive=True
        )
        assert "quadkey" in self._column_names(output_dir)


class TestPartitionCompressionLevel:
    """Partition methods must let each codec resolve its own default level.

    A pinned ``compression_level=15`` is out of range for every codec but ZSTD
    (GZIP accepts 1-9), so it turned a valid CLI invocation into an API error.
    """

    @pytest.fixture
    def sample_table(self):
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")
        return read(PLACES_PARQUET)

    @pytest.fixture
    def output_dir(self):
        tmp_dir = Path(tempfile.gettempdir()) / f"test_part_codec_{uuid.uuid4()}"
        yield tmp_dir
        if tmp_dir.exists():
            import shutil

            shutil.rmtree(tmp_dir)

    @pytest.mark.parametrize("compression", ["GZIP", "ZSTD"])
    def test_partition_by_kdtree_accepts_any_codec(self, sample_table, output_dir, compression):
        result = sample_table.partition_by_kdtree(
            output_dir, iterations=2, overwrite=True, compression=compression
        )
        assert isinstance(result, dict)
        assert list(Path(output_dir).rglob("*.parquet"))


class TestPartitionAutoResolution:
    """``auto=True`` sizes the resolution from the data, exactly as ``--auto`` does.

    Before #762 the API applied a hardcoded resolution (H3 9, quadkey 13/6, S2 13,
    A5 15) where the CLI refuses to guess at all, so the same operation through the
    two front doors produced different partitions from the same bytes. The API now
    mirrors the CLI: name a resolution, or ask for ``auto=True``.
    """

    @pytest.fixture
    def sample_table(self):
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")
        return read(PLACES_PARQUET)

    @staticmethod
    def _partition_names(output_dir):
        files = sorted(Path(output_dir).rglob("*.parquet"))
        assert files, f"no parquet files written to {output_dir}"
        return sorted(f.name for f in files)

    @staticmethod
    def _h3_resolution(cell_hex):
        """H3 stores the resolution in bits 52-55 of its 64-bit index."""
        return (int(cell_hex, 16) >> 52) & 0xF

    @staticmethod
    def _run_cli(args):
        from click.testing import CliRunner

        from geoparquet_io.cli.main import cli

        result = CliRunner().invoke(cli, [str(a) for a in args])
        assert result.exit_code == 0, result.output
        return result

    # -- auto=True reproduces what `--auto` writes -------------------------

    def test_h3_auto_matches_the_cli(self, sample_table, tmp_path):
        self._run_cli(["partition", "h3", PLACES_PARQUET, tmp_path / "cli", "--auto"])
        stats = sample_table.partition_by_h3(tmp_path / "api", auto=True)

        assert stats["file_count"] > 0
        assert self._partition_names(tmp_path / "api") == self._partition_names(tmp_path / "cli")

    def test_h3_auto_uses_the_calculated_resolution_not_the_old_default(
        self, sample_table, tmp_path
    ):
        from geoparquet_io.core.partition.auto_resolution import calculate_auto_resolution

        expected = calculate_auto_resolution(
            input_parquet=str(PLACES_PARQUET),
            spatial_index_type="h3",
            target_rows_per_partition=100000,
            max_partitions=10000,
        )

        sample_table.partition_by_h3(tmp_path / "api", auto=True)
        written = {
            self._h3_resolution(Path(name).stem) for name in self._partition_names(tmp_path / "api")
        }

        assert expected != 9, (
            "fixture precondition: the calculated resolution must differ from the old "
            "hardcoded 9, or this test cannot tell the two apart"
        )
        assert written == {expected}

    def test_quadkey_auto_matches_the_cli(self, sample_table, tmp_path):
        """A target small enough to split the 766-row sample into several partitions.

        At the default target_rows the sample collapses to a single resolution-0
        partition, which two different implementations would agree on by accident.
        """
        self._run_cli(
            [
                "partition",
                "quadkey",
                PLACES_PARQUET,
                tmp_path / "cli",
                "--auto",
                "--target-rows",
                "100",
            ]
        )
        stats = sample_table.partition_by_quadkey(tmp_path / "api", auto=True, target_rows=100)

        assert stats["file_count"] > 1, "target_rows=100 should split the 766-row sample"
        assert self._partition_names(tmp_path / "api") == self._partition_names(tmp_path / "cli")

    def test_a5_auto_matches_the_cli(self, sample_table, tmp_path):
        self._run_cli(["partition", "a5", PLACES_PARQUET, tmp_path / "cli", "--auto"])
        stats = sample_table.partition_by_a5(tmp_path / "api", auto=True)

        assert stats["file_count"] > 0
        assert self._partition_names(tmp_path / "api") == self._partition_names(tmp_path / "cli")

    def test_target_rows_is_forwarded(self, sample_table, tmp_path):
        """A smaller target must buy more partitions, through the API too."""
        default_target = sample_table.partition_by_h3(tmp_path / "default", auto=True)
        small_target = sample_table.partition_by_h3(tmp_path / "small", auto=True, target_rows=100)

        assert small_target["file_count"] > default_target["file_count"]

    def test_s2_forwards_auto_to_core(self, sample_table, tmp_path):
        """`gpio partition s2` cannot run without the 'geography' extension (#737).

        The wiring is still checked: patch core and assert the arguments arrive.
        """
        from geoparquet_io.core.partition import by_s2 as core_by_s2

        with patch.object(core_by_s2, "partition_by_s2") as fake:
            sample_table.partition_by_s2(
                tmp_path / "s2", auto=True, target_rows=5000, max_partitions=50
            )

        kwargs = fake.call_args.kwargs
        assert kwargs["level"] is None
        assert kwargs["auto"] is True
        assert kwargs["target_rows"] == 5000
        assert kwargs["max_partitions"] == 50

    # -- with neither a resolution nor auto, refuse exactly as the CLI does --

    @pytest.mark.parametrize(
        "method",
        ["partition_by_h3", "partition_by_a5", "partition_by_s2", "partition_by_quadkey"],
    )
    def test_refuses_to_guess_a_resolution(self, sample_table, tmp_path, method):
        from geoparquet_io.core.exceptions import InvalidParameterError

        with pytest.raises(InvalidParameterError, match="auto"):
            getattr(sample_table, method)(tmp_path / method)

    def test_quadkey_refuses_a_lone_partition_resolution(self, sample_table, tmp_path):
        from geoparquet_io.core.exceptions import InvalidParameterError

        with pytest.raises(InvalidParameterError, match="auto"):
            sample_table.partition_by_quadkey(tmp_path / "out", partition_resolution=3)

    @pytest.mark.parametrize(
        ("method", "kwargs"),
        [
            ("partition_by_h3", {"resolution": 5}),
            ("partition_by_a5", {"resolution": 5}),
            ("partition_by_s2", {"level": 5}),
            ("partition_by_quadkey", {"resolution": 5, "partition_resolution": 3}),
        ],
    )
    def test_auto_and_an_explicit_resolution_are_mutually_exclusive(
        self, sample_table, tmp_path, method, kwargs
    ):
        from geoparquet_io.core.exceptions import InvalidParameterError

        with pytest.raises(InvalidParameterError, match="auto"):
            getattr(sample_table, method)(tmp_path / method, auto=True, **kwargs)

    # -- and it refuses before it has spent any I/O on the table --------------

    @pytest.mark.parametrize(
        ("method", "kwargs"),
        [
            ("partition_by_h3", {}),
            ("partition_by_a5", {}),
            ("partition_by_s2", {}),
            ("partition_by_quadkey", {}),
            ("partition_by_quadkey", {"partition_resolution": 3}),
            ("partition_by_h3", {"auto": True, "resolution": 5}),
            ("partition_by_a5", {"auto": True, "resolution": 5}),
            ("partition_by_s2", {"auto": True, "level": 5}),
            ("partition_by_quadkey", {"auto": True, "resolution": 5, "partition_resolution": 3}),
        ],
    )
    def test_an_invalid_call_never_serializes_the_table(
        self, sample_table, tmp_path, method, kwargs
    ):
        """The gate belongs in front of the temp-file write, not behind it.

        Every auto method routes through a temp parquet; letting core raise means
        paying a full serialization of the table for a call that was never going
        to run.
        """
        from geoparquet_io.api import table as table_module
        from geoparquet_io.core.exceptions import InvalidParameterError

        with patch.object(table_module, "write_geoparquet_table") as write:
            with pytest.raises(InvalidParameterError, match="auto"):
                getattr(sample_table, method)(tmp_path / "out", **kwargs)

        write.assert_not_called()


class TestPublicExceptionExports:
    """The errors the docs tell users to catch must be importable from the package.

    ``docs/api/python-api.md`` tells callers a partition method with neither a
    resolution nor ``auto`` raises ``InvalidParameterError``; that is only
    actionable if the name is reachable without reaching into ``core``.
    """

    def test_invalid_parameter_error_is_importable_from_the_package(self):
        import geoparquet_io
        from geoparquet_io import InvalidParameterError
        from geoparquet_io.core.exceptions import (
            InvalidParameterError as CoreInvalidParameterError,
        )

        assert InvalidParameterError is CoreInvalidParameterError
        assert "InvalidParameterError" in geoparquet_io.__all__

    def test_invalid_parameter_error_is_importable_from_the_api_package(self):
        from geoparquet_io import api
        from geoparquet_io.core.exceptions import InvalidParameterError

        assert api.InvalidParameterError is InvalidParameterError
        assert "InvalidParameterError" in api.__all__

    def test_the_documented_error_is_what_a_partition_call_raises(self, tmp_path):
        from geoparquet_io import InvalidParameterError

        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")

        with pytest.raises(InvalidParameterError):
            read(PLACES_PARQUET).partition_by_h3(tmp_path / "out")


class TestReadPartition:
    """Tests for the read_partition() function."""

    @pytest.fixture
    def sample_table(self):
        """Create a sample Table from test data."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")
        return read(PLACES_PARQUET)

    @pytest.fixture(params=[False, True], ids=["flat", "hive"])
    def partition_dir(self, request, sample_table):
        """Create a temporary partitioned directory, flat and Hive-style."""
        tmp_dir = Path(tempfile.gettempdir()) / f"test_partition_{uuid.uuid4()}"
        tmp_dir.mkdir(exist_ok=True)

        # Use the full table (766 rows) which is above the minimum threshold
        sample_table.partition_by_quadkey(
            tmp_dir, overwrite=True, resolution=13, partition_resolution=3, hive=request.param
        )

        yield tmp_dir

        # Cleanup with retry for Windows file locking
        import shutil
        import time

        for attempt in range(3):
            try:
                shutil.rmtree(tmp_dir)
                break
            except OSError:
                time.sleep(0.1 * (attempt + 1))

    def test_read_partition_from_directory(self, partition_dir):
        """Test reading a partitioned directory."""
        from geoparquet_io import read_partition

        table = read_partition(partition_dir)
        assert isinstance(table, Table)
        assert table.num_rows > 0
        assert table.geometry_column == "geometry"

    def test_read_partition_then_sort_hilbert(self, partition_dir):
        """Test reading a partitioned directory and then sorting it by Hilbert curve."""
        from geoparquet_io import read_partition

        sorted_table = read_partition(partition_dir).sort_hilbert()
        assert sorted_table.num_rows > 0
        assert sorted_table.geometry_column == "geometry"
        assert sorted_table.check_spatial().passed()


class TestTableHeadTail:
    """Tests for head() and tail() methods."""

    @pytest.fixture
    def sample_table(self):
        """Create a sample Table from test data."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")
        return read(PLACES_PARQUET)

    def test_head_default(self, sample_table):
        """Test head() returns first 10 rows by default."""
        result = sample_table.head()
        assert isinstance(result, Table)
        assert result.num_rows == 10
        assert result.geometry_column == sample_table.geometry_column

    def test_head_custom_n(self, sample_table):
        """Test head() with custom n value."""
        result = sample_table.head(25)
        assert result.num_rows == 25

    def test_head_larger_than_table(self, sample_table):
        """Test head() with n larger than table size."""
        result = sample_table.head(10000)
        assert result.num_rows == sample_table.num_rows

    def test_tail_default(self, sample_table):
        """Test tail() returns last 10 rows by default."""
        result = sample_table.tail()
        assert isinstance(result, Table)
        assert result.num_rows == 10
        assert result.geometry_column == sample_table.geometry_column

    def test_tail_custom_n(self, sample_table):
        """Test tail() with custom n value."""
        result = sample_table.tail(25)
        assert result.num_rows == 25

    def test_tail_larger_than_table(self, sample_table):
        """Test tail() with n larger than table size."""
        result = sample_table.tail(10000)
        assert result.num_rows == sample_table.num_rows

    def test_head_zero(self, sample_table):
        """Test head(0) returns empty Table."""
        result = sample_table.head(0)
        assert isinstance(result, Table)
        assert result.num_rows == 0

    def test_head_negative_raises(self, sample_table):
        """Test head(-1) raises ValueError."""
        with pytest.raises(ValueError, match="n must be non-negative"):
            sample_table.head(-1)

    def test_tail_zero(self, sample_table):
        """Test tail(0) returns empty Table."""
        result = sample_table.tail(0)
        assert isinstance(result, Table)
        assert result.num_rows == 0

    def test_tail_negative_raises(self, sample_table):
        """Test tail(-1) raises ValueError."""
        with pytest.raises(ValueError, match="n must be non-negative"):
            sample_table.tail(-1)


class TestTableStats:
    """Tests for stats() method."""

    @pytest.fixture
    def sample_table(self):
        """Create a sample Table from test data."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")
        return read(PLACES_PARQUET)

    def test_stats_returns_dict(self, sample_table):
        """Test stats() returns a dictionary."""
        result = sample_table.stats()
        assert isinstance(result, dict)

    def test_stats_has_all_columns(self, sample_table):
        """Test stats() includes all columns."""
        result = sample_table.stats()
        for col_name in sample_table.column_names:
            assert col_name in result

    def test_stats_structure(self, sample_table):
        """Test stats() returns expected structure per column."""
        result = sample_table.stats()
        for _col_name, col_stats in result.items():
            assert "nulls" in col_stats
            assert "min" in col_stats
            assert "max" in col_stats
            assert "unique" in col_stats

    def test_stats_geometry_column(self, sample_table):
        """Test stats() handles geometry columns correctly."""
        result = sample_table.stats()
        geom_col = sample_table.geometry_column
        if geom_col:
            assert result[geom_col]["min"] is None
            assert result[geom_col]["max"] is None


class TestTableMetadata:
    """Tests for metadata() method."""

    @pytest.fixture
    def sample_table(self):
        """Create a sample Table from test data."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")
        return read(PLACES_PARQUET)

    def test_metadata_returns_dict(self, sample_table):
        """Test metadata() returns a dictionary."""
        result = sample_table.metadata()
        assert isinstance(result, dict)

    def test_metadata_has_basic_fields(self, sample_table):
        """Test metadata() includes basic fields."""
        result = sample_table.metadata()
        assert "rows" in result
        assert "columns_count" in result
        assert "geometry_column" in result
        assert "columns" in result

    def test_metadata_rows_match(self, sample_table):
        """Test metadata() rows matches table."""
        result = sample_table.metadata()
        assert result["rows"] == sample_table.num_rows

    def test_metadata_columns_structure(self, sample_table):
        """Test metadata() columns have expected structure."""
        result = sample_table.metadata()
        for col in result["columns"]:
            assert "name" in col
            assert "type" in col
            assert "is_geometry" in col

    def test_metadata_includes_geo_metadata(self, sample_table):
        """Test metadata() includes geo_metadata for GeoParquet files."""
        result = sample_table.metadata()
        # The test file should have geo metadata
        if result.get("geoparquet_version"):
            assert "geo_metadata" in result

    def test_metadata_with_parquet_metadata(self, sample_table):
        """Test metadata() includes parquet metadata when requested."""
        result = sample_table.metadata(include_parquet_metadata=True)
        assert isinstance(result, dict)
        # When include_parquet_metadata=True, the key should be present
        # It will be a dict (possibly empty if only 'geo' metadata exists)
        if result.get("geo_metadata"):
            # If geo metadata exists, schema has metadata, so parquet_metadata should be present
            assert "parquet_metadata" in result
            assert isinstance(result["parquet_metadata"], dict)


class TestTableToGeojson:
    """Tests for to_geojson() method."""

    @pytest.fixture
    def sample_table(self):
        """Create a sample Table from test data."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")
        # Use a smaller subset for faster tests
        return read(PLACES_PARQUET).head(10)

    @pytest.fixture
    def output_file(self, tmp_path):
        """Create a temporary output file path using pytest's tmp_path fixture."""
        file_path = tmp_path / f"test_geojson_{uuid.uuid4()}.geojson"
        yield str(file_path)

    def test_to_geojson_to_file(self, sample_table, output_file):
        """Test to_geojson() writes to file."""
        result = sample_table.to_geojson(output_file)
        assert result == output_file
        assert Path(output_file).exists()

    def test_to_geojson_file_is_valid_json(self, sample_table, output_file):
        """Test to_geojson() produces valid JSON."""
        import json

        sample_table.to_geojson(output_file)
        with open(output_file) as f:
            data = json.load(f)
        assert "type" in data
        assert data["type"] == "FeatureCollection"

    def test_metadata_preserved_in_format_conversion(self, sample_table, output_file):
        """Test that GeoParquet metadata is preserved when converting to GeoJSON.

        This verifies that _table_to_temp_parquet() preserves CRS and geometry
        metadata needed for format conversions like GeoJSON reprojection.
        """
        # Convert to GeoJSON (requires CRS metadata for WGS84 reprojection)
        # If metadata was lost, reprojection would fail
        sample_table.to_geojson(output_file)

        # Verify GeoJSON was created successfully (if metadata was missing, this would fail)
        assert Path(output_file).exists()

        # Verify the GeoJSON has proper CRS (should be WGS84)
        import json

        with open(output_file) as f:
            data = json.load(f)

        # GeoJSON spec mandates WGS84, so coordinates should be in lon/lat
        assert "type" in data
        assert data["type"] == "FeatureCollection"
        assert "features" in data
        assert len(data["features"]) > 0

        # Verify feature has geometry
        first_feature = data["features"][0]
        assert "geometry" in first_feature
        assert "coordinates" in first_feature["geometry"]


class TestCheckResult:
    """Tests for CheckResult class."""

    def test_check_result_import(self):
        """Test CheckResult can be imported."""
        from geoparquet_io.api.check import CheckResult

        result = CheckResult({"passed": True}, check_type="test")
        assert result.passed()

    def test_check_result_passed(self):
        """Test CheckResult.passed() method."""
        from geoparquet_io.api.check import CheckResult

        passing = CheckResult({"passed": True}, check_type="test")
        assert passing.passed()

        failing = CheckResult({"passed": False}, check_type="test")
        assert not failing.passed()

    def test_check_result_failures(self):
        """Test CheckResult.failures() method."""
        from geoparquet_io.api.check import CheckResult

        result = CheckResult({"passed": False, "issues": ["Issue 1", "Issue 2"]}, check_type="test")
        failures = result.failures()
        assert len(failures) == 2
        assert "Issue 1" in failures

    def test_check_result_to_dict(self):
        """Test CheckResult.to_dict() method."""
        from geoparquet_io.api.check import CheckResult

        raw = {"passed": True, "some_data": 123}
        result = CheckResult(raw, check_type="test")
        assert result.to_dict() == raw

    def test_check_result_bool(self):
        """Test CheckResult bool conversion."""
        from geoparquet_io.api.check import CheckResult

        passing = CheckResult({"passed": True}, check_type="test")
        assert bool(passing)

        failing = CheckResult({"passed": False}, check_type="test")
        assert not bool(failing)

    def test_check_result_warnings_empty(self):
        """Test CheckResult.warnings() returns empty list when no warnings."""
        from geoparquet_io.api.check import CheckResult

        result = CheckResult({"passed": True}, check_type="compression")
        assert result.warnings() == []

    def test_check_result_warnings_single_check(self):
        """Test CheckResult.warnings() returns warnings from single check."""
        from geoparquet_io.api.check import CheckResult

        result = CheckResult(
            {"passed": True, "warnings": ["Warning 1", "Warning 2"]}, check_type="compression"
        )
        assert len(result.warnings()) == 2
        assert "Warning 1" in result.warnings()
        assert "Warning 2" in result.warnings()

    def test_check_result_warnings_includes_issues_when_passed(self):
        """Test CheckResult.warnings() includes issues as warnings when check passed."""
        from geoparquet_io.api.check import CheckResult

        result = CheckResult(
            {"passed": True, "issues": ["Info message 1", "Info message 2"]}, check_type="test"
        )
        # For single checks, issues are NOT included in warnings (only in failures when failed)
        # This behavior is only for "all" checks
        assert result.warnings() == []

    def test_check_result_warnings_all_check_aggregates(self):
        """Test CheckResult.warnings() aggregates warnings from all check categories."""
        from geoparquet_io.api.check import CheckResult

        result = CheckResult(
            {
                "compression": {"passed": True, "warnings": ["SNAPPY used"]},
                "bbox": {"passed": True, "issues": ["No bbox"]},
                "spatial": {"passed": True},
            },
            check_type="all",
        )
        warnings = result.warnings()
        assert len(warnings) == 2
        assert "[compression] SNAPPY used" in warnings
        assert "[bbox] No bbox" in warnings

    def test_check_result_warnings_all_check_empty(self):
        """Test CheckResult.warnings() returns empty list for all checks with no warnings."""
        from geoparquet_io.api.check import CheckResult

        result = CheckResult(
            {
                "compression": {"passed": True},
                "bbox": {"passed": True},
            },
            check_type="all",
        )
        assert result.warnings() == []

    def test_check_result_recommendations_empty(self):
        """Test CheckResult.recommendations() returns empty list when none."""
        from geoparquet_io.api.check import CheckResult

        result = CheckResult({"passed": True}, check_type="test")
        assert result.recommendations() == []

    def test_check_result_recommendations_single_check(self):
        """Test CheckResult.recommendations() returns recommendations from single check."""
        from geoparquet_io.api.check import CheckResult

        result = CheckResult(
            {"passed": False, "recommendations": ["Add bbox column", "Use ZSTD"]},
            check_type="bbox",
        )
        assert len(result.recommendations()) == 2
        assert "Add bbox column" in result.recommendations()
        assert "Use ZSTD" in result.recommendations()

    def test_check_result_recommendations_all_check_aggregates(self):
        """Test CheckResult.recommendations() aggregates with category prefixes."""
        from geoparquet_io.api.check import CheckResult

        result = CheckResult(
            {
                "compression": {"passed": False, "recommendations": ["Use ZSTD compression"]},
                "bbox": {"passed": False, "recommendations": ["Add bbox column"]},
                "spatial": {"passed": True},
            },
            check_type="all",
        )
        recs = result.recommendations()
        assert len(recs) == 2
        assert "[compression] Use ZSTD compression" in recs
        assert "[bbox] Add bbox column" in recs

    def test_check_result_check_type_property(self):
        """Test CheckResult.check_type property returns the check type."""
        from geoparquet_io.api.check import CheckResult

        result = CheckResult({"passed": True}, check_type="compression")
        assert result.check_type == "compression"

        result_all = CheckResult({}, check_type="all")
        assert result_all.check_type == "all"

    def test_check_result_repr(self):
        """Test CheckResult.__repr__() string representation."""
        from geoparquet_io.api.check import CheckResult

        # Passing check
        result = CheckResult({"passed": True}, check_type="test")
        repr_str = repr(result)
        assert "test" in repr_str
        assert "passed" in repr_str
        assert "failures=0" in repr_str
        assert "warnings=0" in repr_str

        # Failing check with issues and warnings
        result = CheckResult(
            {"passed": False, "issues": ["Issue 1"], "warnings": ["Warn 1"]}, check_type="bbox"
        )
        repr_str = repr(result)
        assert "bbox" in repr_str
        assert "failed" in repr_str
        assert "failures=1" in repr_str
        assert "warnings=1" in repr_str


class TestTableCheck:
    """Tests for Table check methods."""

    @pytest.fixture
    def sample_table(self):
        """Create a sample Table from test data."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")
        return read(PLACES_PARQUET)

    def test_check_returns_check_result(self, sample_table):
        """Test check() returns a CheckResult."""
        from geoparquet_io.api.check import CheckResult

        result = sample_table.check()
        assert isinstance(result, CheckResult)
        assert result.check_type == "all"

    def test_check_has_results(self, sample_table):
        """Test check() returns results dict."""
        result = sample_table.check()
        results_dict = result.to_dict()
        assert isinstance(results_dict, dict)

    def test_check_compression_returns_check_result(self, sample_table):
        """Test check_compression() returns a CheckResult."""
        from geoparquet_io.api.check import CheckResult

        result = sample_table.check_compression()
        assert isinstance(result, CheckResult)
        assert result.check_type == "compression"

    def test_check_bbox_returns_check_result(self, sample_table):
        """Test check_bbox() returns a CheckResult."""
        from geoparquet_io.api.check import CheckResult

        result = sample_table.check_bbox()
        assert isinstance(result, CheckResult)
        assert result.check_type == "bbox"

    def test_check_row_groups_returns_check_result(self, sample_table):
        """Test check_row_groups() returns a CheckResult."""
        from geoparquet_io.api.check import CheckResult

        result = sample_table.check_row_groups()
        assert isinstance(result, CheckResult)
        assert result.check_type == "row_groups"

    def test_check_spatial_returns_check_result(self, sample_table):
        """Test check_spatial() returns a CheckResult."""
        from geoparquet_io.api.check import CheckResult

        result = sample_table.check_spatial()
        assert isinstance(result, CheckResult)
        assert result.check_type == "spatial"
        assert isinstance(result.to_dict(), dict)

    def test_validate_returns_check_result(self, sample_table):
        """Test validate() returns a CheckResult."""
        from geoparquet_io.api.check import CheckResult

        result = sample_table.validate()
        assert isinstance(result, CheckResult)
        assert result.check_type == "validate"
        assert isinstance(result.to_dict(), dict)
        # The result dict should have expected validation fields
        result_dict = result.to_dict()
        assert "passed" in result_dict
        assert "detected_version" in result_dict


class TestTableAddBboxMetadata:
    """Tests for add_bbox_metadata() method."""

    @pytest.fixture
    def sample_table(self):
        """Create a sample Table from test data."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")
        return read(PLACES_PARQUET)

    def test_add_bbox_metadata_requires_bbox_column(self, sample_table):
        """Test add_bbox_metadata() raises error if bbox column missing."""
        # Use a non-existent column name to trigger the error
        with pytest.raises(ValueError, match="not found"):
            sample_table.add_bbox_metadata(bbox_column="nonexistent_bbox")

    def test_add_bbox_metadata_with_bbox_column(self, sample_table, tmp_path):
        """Test add_bbox_metadata() works with bbox column."""
        # The places fixture declares GeoParquet 1.0.0, which cannot carry the
        # 1.1-only covering key (gpio #686) — restate it at 1.1 first.
        v11_path = tmp_path / "places_v11.parquet"
        sample_table.write(str(v11_path), geoparquet_version="1.1")
        table_v11 = read(str(v11_path))

        # First add the bbox column, then add metadata
        with_bbox = table_v11.add_bbox()
        with_meta = with_bbox.add_bbox_metadata()
        assert isinstance(with_meta, Table)

        # Check metadata was added
        meta = with_meta.metadata()
        geo_meta = meta.get("geo_metadata", {})
        columns = geo_meta.get("columns", {})
        geom_col = with_meta.geometry_column

        assert geom_col in columns, "Geometry column should be in geo metadata columns"
        covering = columns[geom_col].get("covering")

        # Verify covering structure
        assert covering is not None, "Covering metadata should be present"
        assert isinstance(covering, dict), "Covering should be a dict"
        assert "bbox" in covering, "Covering should have 'bbox' key"

        bbox_paths = covering["bbox"]
        assert isinstance(bbox_paths, dict), "Covering bbox should be a dict"
        assert "xmin" in bbox_paths, "Covering should have xmin path"
        assert "ymin" in bbox_paths, "Covering should have ymin path"
        assert "xmax" in bbox_paths, "Covering should have xmax path"
        assert "ymax" in bbox_paths, "Covering should have ymax path"

        # Each path should be a list like ["bbox", "xmin"]
        for key in ["xmin", "ymin", "xmax", "ymax"]:
            path = bbox_paths[key]
            assert isinstance(path, list), f"Path for {key} should be a list"
            assert len(path) == 2, f"Path for {key} should have 2 elements"


class TestTopLevelExports:
    """Tests for top-level module exports."""

    def test_check_result_exported(self):
        """Test CheckResult is exported from top-level module."""
        from geoparquet_io import CheckResult

        assert CheckResult is not None

    def test_stac_functions_exported(self):
        """Test STAC functions are exported from top-level module."""
        from geoparquet_io import generate_stac, validate_stac

        assert generate_stac is not None
        assert validate_stac is not None


class TestTableWriteFormats:
    """Tests for Table.write() with multiple output formats."""

    @pytest.fixture
    def sample_table(self):
        """Create a sample Table from test data."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")
        return read(PLACES_PARQUET)

    def test_write_geopackage(self, sample_table):
        """Test Table.write() with GeoPackage format."""
        output_path = Path(tempfile.gettempdir()) / f"test_{uuid.uuid4()}.gpkg"
        try:
            sample_table.write(output_path)
            assert output_path.exists()
            assert output_path.stat().st_size > 0
        finally:
            safe_unlink(output_path)

    def test_write_flatgeobuf(self, sample_table):
        """Test Table.write() with FlatGeobuf format."""
        output_path = Path(tempfile.gettempdir()) / f"test_{uuid.uuid4()}.fgb"
        try:
            sample_table.write(output_path)
            assert output_path.exists()
            assert output_path.stat().st_size > 0
        finally:
            safe_unlink(output_path)

    def test_write_csv(self, sample_table):
        """Test Table.write() with CSV format."""
        output_path = Path(tempfile.gettempdir()) / f"test_{uuid.uuid4()}.csv"
        try:
            sample_table.write(output_path)
            assert output_path.exists()
            assert output_path.stat().st_size > 0

            # Verify CSV has WKT column
            import csv

            with open(output_path, encoding="utf-8") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                assert len(rows) > 0
                assert "wkt" in rows[0]
        finally:
            safe_unlink(output_path)

    def test_write_shapefile(self, sample_table):
        """Test Table.write() with Shapefile format."""
        output_path = Path(tempfile.gettempdir()) / f"test_{uuid.uuid4()}.shp"
        try:
            sample_table.write(output_path)
            assert output_path.exists()
            # Check sidecar files
            assert output_path.with_suffix(".shx").exists()
            assert output_path.with_suffix(".dbf").exists()
        finally:
            # Clean up all shapefile files
            for ext in [".shp", ".shx", ".dbf", ".prj", ".cpg"]:
                safe_unlink(output_path.with_suffix(ext))

    def test_write_explicit_format(self, sample_table):
        """Test Table.write() with explicit format parameter."""
        output_path = Path(tempfile.gettempdir()) / f"test_{uuid.uuid4()}.dat"
        try:
            # Write as CSV even though extension is .dat
            sample_table.write(output_path, format="csv")
            assert output_path.exists()
        finally:
            safe_unlink(output_path)

    def test_write_format_options(self, sample_table):
        """Test Table.write() with format-specific options."""
        output_path = Path(tempfile.gettempdir()) / f"test_{uuid.uuid4()}.gpkg"
        try:
            sample_table.write(
                output_path,
                layer_name="custom_layer",
                overwrite=True,
            )
            assert output_path.exists()
        finally:
            safe_unlink(output_path)


class TestOpsConversionFunctions:
    """Tests for ops.convert_to_*() functions."""

    @pytest.fixture
    def sample_table(self):
        """Create a sample Arrow table."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")
        return pq.read_table(str(PLACES_PARQUET))

    def test_convert_to_geopackage(self, sample_table):
        """Test ops.convert_to_geopackage()."""
        output_path = Path(tempfile.gettempdir()) / f"test_{uuid.uuid4()}.gpkg"
        try:
            result = ops.convert_to_geopackage(sample_table, str(output_path))
            assert result == str(output_path)
            assert output_path.exists()
        finally:
            safe_unlink(output_path)

    def test_convert_to_flatgeobuf(self, sample_table):
        """Test ops.convert_to_flatgeobuf()."""
        output_path = Path(tempfile.gettempdir()) / f"test_{uuid.uuid4()}.fgb"
        try:
            result = ops.convert_to_flatgeobuf(sample_table, str(output_path))
            assert result == str(output_path)
            assert output_path.exists()
        finally:
            safe_unlink(output_path)

    def test_convert_to_csv(self, sample_table):
        """Test ops.convert_to_csv()."""
        output_path = Path(tempfile.gettempdir()) / f"test_{uuid.uuid4()}.csv"
        try:
            result = ops.convert_to_csv(sample_table, str(output_path))
            assert result == str(output_path)
            assert output_path.exists()
        finally:
            safe_unlink(output_path)

    def test_convert_to_shapefile(self, sample_table):
        """Test ops.convert_to_shapefile()."""
        output_path = Path(tempfile.gettempdir()) / f"test_{uuid.uuid4()}.shp"
        try:
            result = ops.convert_to_shapefile(sample_table, str(output_path))
            assert result == str(output_path)
            assert output_path.exists()
        finally:
            for ext in [".shp", ".shx", ".dbf", ".prj", ".cpg"]:
                safe_unlink(output_path.with_suffix(ext))

    def test_convert_with_options(self, sample_table):
        """Test ops conversion functions with format-specific options."""
        output_path = Path(tempfile.gettempdir()) / f"test_{uuid.uuid4()}.gpkg"
        try:
            ops.convert_to_geopackage(
                sample_table,
                str(output_path),
                layer_name="test_layer",
                overwrite=True,
            )
            assert output_path.exists()
        finally:
            safe_unlink(output_path)


class TestReadPartitionS3:
    """Test read_partition() S3 kwargs."""

    def test_read_partition_accepts_s3_kwargs(self, tmp_path):
        """read_partition() accepts S3 kwargs without TypeError."""
        from geoparquet_io.api.table import read_partition

        table = pa.table({"id": [1, 2], "geometry": [b"\x01\x00", b"\x01\x00"]})
        path = tmp_path / "test.parquet"
        pq.write_table(table, path)

        try:
            read_partition(
                str(path),
                s3_endpoint="minio.local:9000",
                s3_region="us-east-1",
                s3_use_ssl=False,
                aws_profile="test",
            )
        except TypeError:
            raise
        except Exception:
            pass


class TestOpsPartition:
    """`ops.partition_by_*` is the function-style twin of every partition subcommand.

    Partitioning used to be reachable from Python only as a ``Table`` method
    (#799), which made it the one command group without the ``ops`` front door
    that ``add``, ``sort``, ``convert``, ``extract`` and ``process`` all have.
    These functions take a plain ``pa.Table`` plus an output directory and must
    behave exactly like the method they mirror -- including the refusal to guess
    a resolution that #762 gave the four spatial-index methods.
    """

    @pytest.fixture
    def arrow_table(self):
        """A plain PyArrow table -- the input an ops caller actually holds."""
        if not PLACES_PARQUET.exists():
            pytest.skip("Test data not available")
        return pq.read_table(PLACES_PARQUET)

    @staticmethod
    def _partition_names(output_dir):
        files = sorted(Path(output_dir).rglob("*.parquet"))
        assert files, f"no parquet files written to {output_dir}"
        return sorted(f.name for f in files)

    # -- each function writes partitions from a bare pa.Table ---------------

    def test_partition_by_h3(self, arrow_table, tmp_path):
        stats = ops.partition_by_h3(arrow_table, tmp_path / "out", resolution=2)

        assert stats["file_count"] > 0
        assert self._partition_names(tmp_path / "out")

    def test_partition_by_a5(self, arrow_table, tmp_path):
        stats = ops.partition_by_a5(arrow_table, tmp_path / "out", resolution=4)

        assert stats["file_count"] > 0
        assert self._partition_names(tmp_path / "out")

    def test_partition_by_quadkey(self, arrow_table, tmp_path):
        stats = ops.partition_by_quadkey(
            arrow_table, tmp_path / "out", resolution=13, partition_resolution=3
        )

        assert stats["file_count"] > 0
        assert self._partition_names(tmp_path / "out")

    def test_partition_by_kdtree(self, arrow_table, tmp_path):
        """kdtree/string/admin return the core result, not the file-count stats."""
        result = ops.partition_by_kdtree(arrow_table, tmp_path / "out", iterations=2)

        assert isinstance(result, dict)
        assert len(self._partition_names(tmp_path / "out")) == 4

    def test_partition_by_string(self, arrow_table, tmp_path):
        # A column with two values: the partition-size guard rejects the tiny
        # partitions a per-name split of 766 rows would produce.
        regions = pa.array(["north" if i % 2 else "south" for i in range(arrow_table.num_rows)])
        table = arrow_table.append_column("region", regions)

        result = ops.partition_by_string(table, tmp_path / "out", column="region")

        assert isinstance(result, dict)
        assert len(self._partition_names(tmp_path / "out")) == 2

    def test_partition_by_s2_forwards_to_core(self, arrow_table, tmp_path):
        """`gpio partition s2` cannot run without the 'geography' extension (#737).

        The wiring is still checked: patch core and assert the arguments arrive.
        """
        from geoparquet_io.core.partition import by_s2 as core_by_s2

        with patch.object(core_by_s2, "partition_by_s2") as fake:
            ops.partition_by_s2(
                arrow_table, tmp_path / "out", auto=True, target_rows=5000, max_partitions=50
            )

        kwargs = fake.call_args.kwargs
        assert kwargs["level"] is None
        assert kwargs["auto"] is True
        assert kwargs["target_rows"] == 5000
        assert kwargs["max_partitions"] == 50

    def test_partition_by_s2_fails_like_the_table_method(self, arrow_table, tmp_path):
        """S2's unavailability (#737) must read the same through `ops` and `Table`.

        The forwarding test above patches core away, so it says nothing about
        what a real caller sees. This one runs the whole path unpatched: an
        ``ops`` twin that swallowed the error, or re-raised it as something
        else, would leave a caller unable to tell why S2 failed -- and would do
        it differently from the method it is supposed to mirror.
        """
        skip_if_geography_available()
        from geoparquet_io.core.exceptions import ExtensionUnavailableError

        with pytest.raises(ExtensionUnavailableError) as via_ops:
            ops.partition_by_s2(arrow_table, tmp_path / "ops", level=5)
        with pytest.raises(ExtensionUnavailableError) as via_table:
            Table(arrow_table).partition_by_s2(tmp_path / "table", level=5)

        assert via_ops.value.name == "geography"
        assert "geography" in str(via_ops.value)
        assert str(via_ops.value) == str(via_table.value)

    def test_partition_by_admin_forwards_to_core(self, arrow_table, tmp_path):
        """Admin partitioning downloads a boundaries dataset; check the wiring only."""
        from geoparquet_io.core.partition import admin_hierarchical as core_admin

        with patch.object(core_admin, "partition_by_admin_hierarchical") as fake:
            ops.partition_by_admin(
                arrow_table, tmp_path / "out", dataset="overture", levels=["country"], hive=True
            )

        kwargs = fake.call_args.kwargs
        assert kwargs["dataset_name"] == "overture"
        assert kwargs["levels"] == ["country"]
        assert kwargs["hive"] is True

    def test_partition_by_admin_vecorel_forces_overture_country_region(self, arrow_table, tmp_path):
        """Same vecorel shorthand the Table method applies, not a second spelling."""
        from geoparquet_io.core.partition import admin_hierarchical as core_admin

        with patch.object(core_admin, "partition_by_admin_hierarchical") as fake:
            ops.partition_by_admin(arrow_table, tmp_path / "out", vecorel=True)

        kwargs = fake.call_args.kwargs
        assert kwargs["dataset_name"] == "overture"
        assert kwargs["levels"] == ["country", "region"]
        assert kwargs["vecorel"] is True

    # -- the ops twin and the Table method are the same operation -----------

    def test_ops_and_table_write_the_same_partitions(self, arrow_table, tmp_path):
        ops.partition_by_h3(arrow_table, tmp_path / "ops", resolution=2)
        Table(arrow_table).partition_by_h3(tmp_path / "table", resolution=2)

        assert self._partition_names(tmp_path / "ops") == self._partition_names(tmp_path / "table")

    def test_auto_is_forwarded(self, arrow_table, tmp_path):
        """`auto=True` sizes the resolution from the data, as `--auto` does."""
        stats = ops.partition_by_h3(arrow_table, tmp_path / "out", auto=True)

        assert stats["file_count"] > 0

    def test_hive_produces_key_value_directories(self, arrow_table, tmp_path):
        ops.partition_by_h3(arrow_table, tmp_path / "out", resolution=2, hive=True)

        subdirs = [d for d in (tmp_path / "out").iterdir() if d.is_dir()]
        assert subdirs and all("h3_cell=" in d.name for d in subdirs)

    def test_keep_column_escape_hatch_is_exposed(self, arrow_table, tmp_path):
        ops.partition_by_h3(arrow_table, tmp_path / "out", resolution=2, keep_h3_column=True)

        first = sorted(Path(tmp_path / "out").rglob("*.parquet"))[0]
        assert "h3_cell" in pq.ParquetFile(first).schema_arrow.names

    # -- refuse to guess, exactly as the CLI and the Table methods do -------

    @pytest.mark.parametrize(
        "function",
        ["partition_by_h3", "partition_by_a5", "partition_by_s2", "partition_by_quadkey"],
    )
    def test_refuses_to_guess_a_resolution(self, arrow_table, tmp_path, function):
        from geoparquet_io.core.exceptions import InvalidParameterError

        with pytest.raises(InvalidParameterError, match="auto"):
            getattr(ops, function)(arrow_table, tmp_path / function)

    @pytest.mark.parametrize(
        ("function", "kwargs"),
        [
            ("partition_by_h3", {"resolution": 5}),
            ("partition_by_a5", {"resolution": 5}),
            ("partition_by_s2", {"level": 5}),
            ("partition_by_quadkey", {"resolution": 5, "partition_resolution": 3}),
        ],
    )
    def test_auto_and_an_explicit_resolution_are_mutually_exclusive(
        self, arrow_table, tmp_path, function, kwargs
    ):
        from geoparquet_io.core.exceptions import InvalidParameterError

        with pytest.raises(InvalidParameterError, match="auto"):
            getattr(ops, function)(arrow_table, tmp_path / function, auto=True, **kwargs)
