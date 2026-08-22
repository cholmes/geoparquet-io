"""
Tests for write strategy implementations.

Tests the Strategy Pattern for GeoParquet writes including:
- Factory methods
- Individual strategy implementations
- Security validations
"""

import json
import struct
import tempfile
import uuid
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.write_strategies import (
    WriteStrategy,
    WriteStrategyFactory,
    atomic_write,
    needs_metadata_rewrite,
)


class TestWriteStrategy:
    """Tests for WriteStrategy enum."""

    def test_enum_values(self):
        """All expected strategy values exist."""
        assert WriteStrategy.ARROW_MEMORY.value == "in-memory"
        assert WriteStrategy.ARROW_STREAMING.value == "streaming"
        assert WriteStrategy.DUCKDB_KV.value == "duckdb-kv"
        assert WriteStrategy.DISK_REWRITE.value == "disk-rewrite"

    def test_enum_from_string(self):
        """Enum can be created from string values."""
        assert WriteStrategy("in-memory") == WriteStrategy.ARROW_MEMORY
        assert WriteStrategy("streaming") == WriteStrategy.ARROW_STREAMING
        assert WriteStrategy("duckdb-kv") == WriteStrategy.DUCKDB_KV
        assert WriteStrategy("disk-rewrite") == WriteStrategy.DISK_REWRITE


class TestWriteStrategyFactory:
    """Tests for WriteStrategyFactory."""

    def test_get_strategy_arrow_memory(self):
        """Get in-memory strategy."""
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.ARROW_MEMORY)
        assert strategy.name == "in-memory"
        assert strategy.supports_streaming is False
        assert strategy.supports_remote is True

    def test_get_strategy_streaming(self):
        """Get streaming strategy."""
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.ARROW_STREAMING)
        assert strategy.name == "streaming"
        assert strategy.supports_streaming is True

    def test_get_strategy_duckdb_kv(self):
        """Get DuckDB KV strategy."""
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.DUCKDB_KV)
        assert strategy.name == "duckdb-kv"
        assert strategy.supports_streaming is True

    def test_get_strategy_disk_rewrite(self):
        """Get disk rewrite strategy."""
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.DISK_REWRITE)
        assert strategy.name == "disk-rewrite"
        assert strategy.supports_streaming is False

    def test_list_strategies(self):
        """List all available strategies."""
        strategies = WriteStrategyFactory.list_strategies()
        assert "in-memory" in strategies
        assert "streaming" in strategies
        assert "duckdb-kv" in strategies
        assert "disk-rewrite" in strategies

    def test_cache_clear(self):
        """Cache can be cleared."""
        WriteStrategyFactory.get_strategy(WriteStrategy.ARROW_MEMORY)
        WriteStrategyFactory.clear_cache()


class TestAtomicWrite:
    """Tests for atomic_write context manager."""

    def test_successful_write(self):
        """Successful write atomically renames file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.parquet"

            with atomic_write(str(output_path)) as temp_path:
                Path(temp_path).write_text("test content")

            assert output_path.exists()
            assert output_path.read_text() == "test content"

    def test_failed_write_cleanup(self):
        """Failed write cleans up temp file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = Path(tmpdir) / "output.parquet"

            with pytest.raises(RuntimeError):
                with atomic_write(str(output_path)) as temp_path:
                    Path(temp_path).write_text("partial")
                    raise RuntimeError("Simulated failure")

            assert not output_path.exists()
            # Temp file should be cleaned up
            temp_files = list(Path(tmpdir).glob("*.parquet*"))
            assert len(temp_files) == 0


class TestNeedsMetadataRewrite:
    """Tests for needs_metadata_rewrite function."""

    def test_parquet_geo_only_no_rewrite(self):
        """parquet-geo-only doesn't need rewrite."""
        assert needs_metadata_rewrite("parquet-geo-only", None) is False

    def test_v1_needs_rewrite(self):
        """GeoParquet 1.x needs rewrite."""
        assert needs_metadata_rewrite("1.1", None) is True
        assert needs_metadata_rewrite("1.0", None) is True

    def test_v2_columns_only_no_rewrite(self):
        """GeoParquet 2.0 with columns_only operation skips rewrite."""
        assert needs_metadata_rewrite("2.0", None, "columns_only") is False

    def test_v2_sort_no_rewrite(self):
        """GeoParquet 2.0 with sort operation skips rewrite."""
        assert needs_metadata_rewrite("2.0", None, "sort") is False


@pytest.fixture
def sample_table():
    """Create a sample PyArrow table with geometry."""
    # Simple point geometries as WKB
    wkb_point = b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@"

    return pa.table(
        {
            "id": [1, 2, 3],
            "name": ["a", "b", "c"],
            "geometry": [wkb_point, wkb_point, wkb_point],
        }
    )


@pytest.fixture
def output_file():
    """Create temp output path with cleanup."""
    tmp_path = Path(tempfile.gettempdir()) / f"test_write_{uuid.uuid4()}.parquet"
    yield str(tmp_path)
    if tmp_path.exists():
        tmp_path.unlink()


class TestArrowMemoryStrategy:
    """Tests for ArrowMemoryStrategy."""

    def test_write_from_table(self, sample_table, output_file):
        """Write table produces valid GeoParquet."""
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.ARROW_MEMORY)

        strategy.write_from_table(
            table=sample_table,
            output_path=output_file,
            geometry_column="geometry",
            geoparquet_version="1.1",
            compression="ZSTD",
            compression_level=15,
            row_group_size_mb=None,
            row_group_rows=None,
            verbose=False,
        )

        assert Path(output_file).exists()
        pf = pq.ParquetFile(output_file)
        assert pf.metadata.num_rows == 3

        # Check geo metadata
        metadata = pf.schema_arrow.metadata
        assert b"geo" in metadata


class TestDuckDBKVStrategy:
    """Tests for DuckDBKVStrategy."""

    def test_path_traversal_rejected(self):
        """Path traversal attempts are blocked."""
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.DUCKDB_KV)

        with pytest.raises(ValueError, match="directory traversal"):
            strategy._validate_output_path("../../../etc/passwd")

    def test_null_byte_rejected(self):
        """Null bytes in paths are rejected."""
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.DUCKDB_KV)

        with pytest.raises(ValueError, match="Invalid characters"):
            strategy._validate_output_path("file\x00.parquet")

    def test_semicolon_rejected(self):
        """Semicolons in paths are rejected (SQL injection prevention)."""
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.DUCKDB_KV)

        with pytest.raises(ValueError, match="Invalid characters"):
            strategy._validate_output_path("file;DROP TABLE users;--.parquet")


class TestStripTrailingOrderBy:
    """Sampling for --row-group-size-mb must not re-run the ordered query."""

    def test_strips_top_level_hilbert_order_by(self):
        from geoparquet_io.core.write_strategies.row_group_sizing import _strip_trailing_order_by

        query = "SELECT * FROM t\n        ORDER BY ST_Hilbert(geometry, ST_Extent(x))\n    "
        stripped = _strip_trailing_order_by(query)
        assert "ORDER BY" not in stripped.upper()
        assert stripped.strip() == "SELECT * FROM t"

    def test_leaves_query_without_order_by(self):
        from geoparquet_io.core.write_strategies.row_group_sizing import _strip_trailing_order_by

        query = "SELECT * FROM t WHERE id > 5"
        assert _strip_trailing_order_by(query) == query

    def test_ignores_order_by_inside_subquery(self):
        from geoparquet_io.core.write_strategies.row_group_sizing import _strip_trailing_order_by

        query = "SELECT * FROM (SELECT a FROM t ORDER BY a) sub"
        assert _strip_trailing_order_by(query) == query

    def test_keeps_order_by_when_limit_follows(self):
        from geoparquet_io.core.write_strategies.row_group_sizing import _strip_trailing_order_by

        query = "SELECT * FROM t ORDER BY a LIMIT 10"
        assert _strip_trailing_order_by(query) == query

    def test_stripped_sample_matches_ordered_row_size(self):
        """The estimate must be unaffected by dropping the ordering."""
        from geoparquet_io.core.write_strategies.row_group_sizing import (
            _resolve_row_group_rows,
            _strip_trailing_order_by,
        )

        con = duckdb.connect()
        base = "SELECT i AS id, i * 2 AS v FROM range(1000) t(i)"
        ordered = f"{base}\n            ORDER BY id"

        rows = _resolve_row_group_rows(con, ordered, 0.001, None, verbose=False)
        con.close()
        assert rows is not None and rows > 0
        assert _strip_trailing_order_by(ordered).strip() == base


class TestDiskRewriteStrategy:
    """Tests for DiskRewriteStrategy."""

    def test_write_from_table(self, sample_table, output_file):
        """Write table produces valid GeoParquet."""
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.DISK_REWRITE)

        strategy.write_from_table(
            table=sample_table,
            output_path=output_file,
            geometry_column="geometry",
            geoparquet_version="1.1",
            compression="ZSTD",
            compression_level=15,
            row_group_size_mb=None,
            row_group_rows=None,
            verbose=False,
        )

        assert Path(output_file).exists()
        pf = pq.ParquetFile(output_file)
        assert pf.metadata.num_rows == 3

        # Check geo metadata
        metadata = pf.schema_arrow.metadata
        assert b"geo" in metadata
        geo_meta = json.loads(metadata[b"geo"])
        assert "geometry" in geo_meta["columns"]


@pytest.fixture
def duckdb_connection():
    """Create DuckDB connection with spatial extension."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial")
    yield con
    con.close()


@pytest.fixture
def sample_geoparquet(tmp_path):
    """Create a sample GeoParquet file for testing."""
    # Create sample data with WKB geometry
    wkb_point = b"\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0?\x00\x00\x00\x00\x00\x00\x00@"
    table = pa.table(
        {
            "id": [1, 2, 3],
            "name": ["a", "b", "c"],
            "geometry": [wkb_point, wkb_point, wkb_point],
        }
    )

    output_path = tmp_path / f"sample_{uuid.uuid4()}.parquet"

    # Write with geo metadata
    geo_meta = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {
            "geometry": {
                "encoding": "WKB",
                "geometry_types": ["Point"],
            }
        },
    }

    metadata = {b"geo": json.dumps(geo_meta).encode()}
    schema_with_meta = table.schema.with_metadata(metadata)
    table = table.cast(schema_with_meta)

    pq.write_table(table, output_path)
    return str(output_path)


class TestWriteFromQuery:
    """Tests for writing from DuckDB queries."""

    def test_arrow_memory_write_from_query(self, duckdb_connection, sample_geoparquet, output_file):
        """ArrowMemoryStrategy writes from query correctly."""
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.ARROW_MEMORY)

        query = f"SELECT * FROM read_parquet('{sample_geoparquet}')"

        strategy.write_from_query(
            con=duckdb_connection,
            query=query,
            output_path=output_file,
            geometry_column="geometry",
            original_metadata=None,
            geoparquet_version="1.1",
            compression="ZSTD",
            compression_level=15,
            row_group_size_mb=None,
            row_group_rows=None,
            input_crs=None,
            verbose=False,
        )

        assert Path(output_file).exists()
        pf = pq.ParquetFile(output_file)
        assert pf.metadata.num_rows == 3


class TestWriteStrategiesNoGeometry:
    """
    Tests for write strategies when geometry_column is None.

    Regression tests for issue #440: write strategies should write valid plain
    Parquet (no geo metadata) when the table has no geometry column.
    """

    @pytest.fixture
    def non_geo_table(self):
        """Create a plain Arrow table with no geometry."""
        return pa.table(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "value": [100.5, 200.0, 300.75],
            }
        )

    @pytest.fixture
    def non_geo_query(self, duckdb_connection):
        """Register a non-geo table and return query string."""
        table = pa.table(
            {
                "id": [1, 2, 3],
                "name": ["Alice", "Bob", "Charlie"],
                "value": [100.5, 200.0, 300.75],
            }
        )
        duckdb_connection.register("non_geo_data", table)
        return "SELECT * FROM non_geo_data"

    def _assert_valid_plain_parquet(self, output_path: str):
        """Assert output is valid plain Parquet with no geo metadata."""
        pf = pq.ParquetFile(output_path)
        assert pf.metadata.num_rows == 3

        # Verify no geo metadata key
        schema_metadata = pf.schema_arrow.metadata or {}
        assert b"geo" not in schema_metadata, "Plain Parquet should have no 'geo' metadata"

        # Verify data integrity
        table = pf.read()
        assert table.column_names == ["id", "name", "value"]
        assert table["id"].to_pylist() == [1, 2, 3]

    def test_in_memory_strategy_no_geometry(self, non_geo_table, output_file):
        """in-memory strategy writes valid plain Parquet when geometry_column=None."""
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.ARROW_MEMORY)

        strategy.write_from_table(
            table=non_geo_table,
            output_path=output_file,
            geometry_column=None,
            geoparquet_version="1.1",
            compression="ZSTD",
            compression_level=15,
            row_group_size_mb=None,
            row_group_rows=None,
            verbose=False,
        )

        self._assert_valid_plain_parquet(output_file)

    def test_duckdb_kv_strategy_no_geometry(self, non_geo_table, output_file):
        """duckdb-kv strategy writes valid plain Parquet when geometry_column=None."""
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.DUCKDB_KV)

        strategy.write_from_table(
            table=non_geo_table,
            output_path=output_file,
            geometry_column=None,
            geoparquet_version="1.1",
            compression="ZSTD",
            compression_level=15,
            row_group_size_mb=None,
            row_group_rows=None,
            verbose=False,
        )

        self._assert_valid_plain_parquet(output_file)

    def test_streaming_strategy_no_geometry(self, non_geo_table, output_file):
        """streaming strategy writes valid plain Parquet when geometry_column=None."""
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.ARROW_STREAMING)

        strategy.write_from_table(
            table=non_geo_table,
            output_path=output_file,
            geometry_column=None,
            geoparquet_version="1.1",
            compression="ZSTD",
            compression_level=15,
            row_group_size_mb=None,
            row_group_rows=None,
            verbose=False,
        )

        self._assert_valid_plain_parquet(output_file)

    def test_disk_rewrite_strategy_no_geometry(self, non_geo_table, output_file):
        """disk-rewrite strategy writes valid plain Parquet when geometry_column=None."""
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.DISK_REWRITE)

        strategy.write_from_table(
            table=non_geo_table,
            output_path=output_file,
            geometry_column=None,
            geoparquet_version="1.1",
            compression="ZSTD",
            compression_level=15,
            row_group_size_mb=None,
            row_group_rows=None,
            verbose=False,
        )

        self._assert_valid_plain_parquet(output_file)

    def test_in_memory_query_no_geometry(self, duckdb_connection, non_geo_query, output_file):
        """in-memory strategy write_from_query handles geometry_column=None."""
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.ARROW_MEMORY)

        strategy.write_from_query(
            con=duckdb_connection,
            query=non_geo_query,
            output_path=output_file,
            geometry_column=None,
            original_metadata=None,
            geoparquet_version="1.1",
            compression="ZSTD",
            compression_level=15,
            row_group_size_mb=None,
            row_group_rows=None,
            input_crs=None,
            verbose=False,
        )

        self._assert_valid_plain_parquet(output_file)

    def test_duckdb_kv_query_no_geometry(self, duckdb_connection, non_geo_query, output_file):
        """duckdb-kv strategy write_from_query handles geometry_column=None."""
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.DUCKDB_KV)

        strategy.write_from_query(
            con=duckdb_connection,
            query=non_geo_query,
            output_path=output_file,
            geometry_column=None,
            original_metadata=None,
            geoparquet_version="1.1",
            compression="ZSTD",
            compression_level=15,
            row_group_size_mb=None,
            row_group_rows=None,
            input_crs=None,
            verbose=False,
        )

        self._assert_valid_plain_parquet(output_file)

    def test_streaming_query_no_geometry(self, duckdb_connection, non_geo_query, output_file):
        """streaming strategy write_from_query handles geometry_column=None."""
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.ARROW_STREAMING)

        strategy.write_from_query(
            con=duckdb_connection,
            query=non_geo_query,
            output_path=output_file,
            geometry_column=None,
            original_metadata=None,
            geoparquet_version="1.1",
            compression="ZSTD",
            compression_level=15,
            row_group_size_mb=None,
            row_group_rows=None,
            input_crs=None,
            verbose=False,
        )

        self._assert_valid_plain_parquet(output_file)

    def test_disk_rewrite_query_no_geometry(self, duckdb_connection, non_geo_query, output_file):
        """disk-rewrite strategy write_from_query handles geometry_column=None."""
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.DISK_REWRITE)

        strategy.write_from_query(
            con=duckdb_connection,
            query=non_geo_query,
            output_path=output_file,
            geometry_column=None,
            original_metadata=None,
            geoparquet_version="1.1",
            compression="ZSTD",
            compression_level=15,
            row_group_size_mb=None,
            row_group_rows=None,
            input_crs=None,
            verbose=False,
        )

        self._assert_valid_plain_parquet(output_file)

    def test_compression_options_honored_no_geometry(self, non_geo_table, output_file):
        """Compression options are honored when writing non-geo data."""
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.DUCKDB_KV)

        strategy.write_from_table(
            table=non_geo_table,
            output_path=output_file,
            geometry_column=None,
            geoparquet_version="1.1",
            compression="SNAPPY",
            compression_level=None,
            row_group_size_mb=None,
            row_group_rows=None,
            verbose=False,
        )

        pf = pq.ParquetFile(output_file)
        # Verify compression was applied (SNAPPY)
        row_group = pf.metadata.row_group(0)
        col_meta = row_group.column(0)
        assert col_meta.compression == "SNAPPY"

    @pytest.fixture
    def empty_non_geo_table(self):
        """Create an empty Arrow table with no geometry."""
        return pa.table(
            {
                "id": pa.array([], type=pa.int64()),
                "name": pa.array([], type=pa.string()),
                "value": pa.array([], type=pa.float64()),
            }
        )

    @pytest.fixture
    def empty_non_geo_query(self, duckdb_connection):
        """Register an empty non-geo table and return query string."""
        table = pa.table(
            {
                "id": pa.array([], type=pa.int64()),
                "name": pa.array([], type=pa.string()),
                "value": pa.array([], type=pa.float64()),
            }
        )
        duckdb_connection.register("empty_non_geo_data", table)
        return "SELECT * FROM empty_non_geo_data"

    def _assert_valid_empty_plain_parquet(self, output_path: str):
        """Assert output is valid empty plain Parquet with no geo metadata."""
        pf = pq.ParquetFile(output_path)
        assert pf.metadata.num_rows == 0

        # Verify no geo metadata key
        schema_metadata = pf.schema_arrow.metadata or {}
        assert b"geo" not in schema_metadata, "Plain Parquet should have no 'geo' metadata"

    def test_streaming_empty_table_no_geometry(self, empty_non_geo_table, output_file):
        """streaming strategy handles empty table with geometry_column=None."""
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.ARROW_STREAMING)

        strategy.write_from_table(
            table=empty_non_geo_table,
            output_path=output_file,
            geometry_column=None,
            geoparquet_version="1.1",
            compression="ZSTD",
            compression_level=None,
            row_group_size_mb=None,
            row_group_rows=None,
            verbose=False,
        )

        self._assert_valid_empty_plain_parquet(output_file)

    def test_streaming_empty_query_no_geometry(
        self, duckdb_connection, empty_non_geo_query, output_file
    ):
        """streaming strategy handles empty query result with geometry_column=None."""
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.ARROW_STREAMING)

        strategy.write_from_query(
            con=duckdb_connection,
            query=empty_non_geo_query,
            output_path=output_file,
            geometry_column=None,
            original_metadata=None,
            geoparquet_version="1.1",
            compression="ZSTD",
            compression_level=None,
            row_group_size_mb=None,
            row_group_rows=None,
            input_crs=None,
            verbose=False,
        )

        self._assert_valid_empty_plain_parquet(output_file)


def _write_points_geoparquet(path: Path, num_rows: int) -> str:
    """Write a small WKB-point GeoParquet file used as a multi-row-group source."""
    geo_meta = {
        "version": "1.1.0",
        "primary_column": "geometry",
        "columns": {"geometry": {"encoding": "WKB", "geometry_types": ["Point"]}},
    }
    points = [struct.pack("<BI2d", 1, 1, float(i), float(i)) for i in range(num_rows)]
    table = pa.table({"id": list(range(num_rows)), "geometry": points})
    table = table.replace_schema_metadata({b"geo": json.dumps(geo_meta).encode()})
    pq.write_table(table, path)
    return str(path)


class TestDiskRewriteRowGroupSizing:
    """disk-rewrite must honour row-group sizing requests (issue #689).

    The strategy accepted ``row_group_rows``/``row_group_size_mb`` and used
    neither, so callers silently got DuckDB/PyArrow defaults.
    """

    def _row_group_sizes(self, path: str) -> list[int]:
        pf = pq.ParquetFile(path)
        return [pf.metadata.row_group(i).num_rows for i in range(pf.metadata.num_row_groups)]

    def test_row_group_rows_honored_from_query(self, duckdb_connection, tmp_path, output_file):
        """An explicit rows-per-group request shapes the written row groups."""
        src = _write_points_geoparquet(tmp_path / "src.parquet", 40)
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.DISK_REWRITE)

        strategy.write_from_query(
            con=duckdb_connection,
            query=f"SELECT * FROM read_parquet('{src}')",
            output_path=output_file,
            geometry_column="geometry",
            original_metadata=None,
            geoparquet_version="1.1",
            compression="ZSTD",
            compression_level=None,
            row_group_size_mb=None,
            row_group_rows=10,
            input_crs=None,
            verbose=False,
        )

        sizes = self._row_group_sizes(output_file)
        assert sum(sizes) == 40
        assert len(sizes) == 4, f"expected 4 row groups of 10 rows, got {sizes}"
        assert max(sizes) <= 10

    def test_no_row_group_settings_leaves_default_shape(
        self, duckdb_connection, tmp_path, output_file
    ):
        """Without sizing options the default single-row-group shape is unchanged."""
        src = _write_points_geoparquet(tmp_path / "src.parquet", 40)
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.DISK_REWRITE)

        strategy.write_from_query(
            con=duckdb_connection,
            query=f"SELECT * FROM read_parquet('{src}')",
            output_path=output_file,
            geometry_column="geometry",
            original_metadata=None,
            geoparquet_version="1.1",
            compression="ZSTD",
            compression_level=None,
            row_group_size_mb=None,
            row_group_rows=None,
            input_crs=None,
            verbose=False,
        )

        assert self._row_group_sizes(output_file) == [40]

    def test_row_group_size_mb_splits_row_groups(self, duckdb_connection, tmp_path, output_file):
        """A small MB target produces more, smaller row groups than the default."""
        src = _write_points_geoparquet(tmp_path / "src.parquet", 2000)
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.DISK_REWRITE)

        strategy.write_from_query(
            con=duckdb_connection,
            query=f"SELECT * FROM read_parquet('{src}')",
            output_path=output_file,
            geometry_column="geometry",
            original_metadata=None,
            geoparquet_version="1.1",
            compression="ZSTD",
            compression_level=None,
            row_group_size_mb=0.01,
            row_group_rows=None,
            input_crs=None,
            verbose=False,
        )

        sizes = self._row_group_sizes(output_file)
        assert sum(sizes) == 2000
        assert len(sizes) > 1, f"MB target should split row groups, got {sizes}"
        assert max(sizes) < 2000

    def test_write_from_table_honors_row_group_rows(self, tmp_path, output_file):
        """write_from_table forwards sizing through to the written file."""
        src = _write_points_geoparquet(tmp_path / "src.parquet", 40)
        table = pq.read_table(src)
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.DISK_REWRITE)

        strategy.write_from_table(
            table=table,
            output_path=output_file,
            geometry_column="geometry",
            geoparquet_version="1.1",
            compression="ZSTD",
            compression_level=None,
            row_group_size_mb=None,
            row_group_rows=10,
            verbose=False,
        )

        sizes = self._row_group_sizes(output_file)
        assert sum(sizes) == 40
        assert len(sizes) == 4, f"expected 4 row groups of 10 rows, got {sizes}"

    def test_plain_parquet_query_honors_row_group_rows(self, duckdb_connection, output_file):
        """Non-geo writes honour sizing too (they take a separate COPY path).

        This path is a bare DuckDB COPY, which flushes at 2048-row vector
        granularity, so the request is asserted at that scale (same granularity
        the duckdb-kv strategy gives for non-geo writes).
        """
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.DISK_REWRITE)

        strategy.write_from_query(
            con=duckdb_connection,
            query="SELECT i AS id, i * 2 AS v FROM range(10000) t(i)",
            output_path=output_file,
            geometry_column=None,
            original_metadata=None,
            geoparquet_version="1.1",
            compression="ZSTD",
            compression_level=None,
            row_group_size_mb=None,
            row_group_rows=2048,
            input_crs=None,
            verbose=False,
        )

        sizes = self._row_group_sizes(output_file)
        assert sum(sizes) == 10000
        assert max(sizes) <= 2048, f"expected <=2048 rows per group, got {sizes}"
        assert len(sizes) == 5, f"expected 5 row groups, got {sizes}"

    def test_plain_parquet_table_honors_row_group_size_mb(self, output_file):
        """Non-geo Arrow-table writes convert an MB target to rows per group."""
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.DISK_REWRITE)
        table = pa.table({"id": list(range(5000)), "v": [float(i) for i in range(5000)]})

        strategy.write_from_table(
            table=table,
            output_path=output_file,
            geometry_column=None,
            geoparquet_version="1.1",
            compression="ZSTD",
            compression_level=None,
            row_group_size_mb=0.01,
            row_group_rows=None,
            verbose=False,
        )

        sizes = self._row_group_sizes(output_file)
        assert sum(sizes) == 5000
        assert len(sizes) > 1, f"MB target should split row groups, got {sizes}"

    def test_plain_parquet_table_honors_row_group_rows(self, output_file):
        """Non-geo Arrow-table writes honour sizing too."""
        strategy = WriteStrategyFactory.get_strategy(WriteStrategy.DISK_REWRITE)
        table = pa.table({"id": list(range(40)), "v": [float(i) for i in range(40)]})

        strategy.write_from_table(
            table=table,
            output_path=output_file,
            geometry_column=None,
            geoparquet_version="1.1",
            compression="ZSTD",
            compression_level=None,
            row_group_size_mb=None,
            row_group_rows=10,
            verbose=False,
        )

        sizes = self._row_group_sizes(output_file)
        assert sum(sizes) == 40
        assert len(sizes) == 4, f"expected 4 row groups of 10 rows, got {sizes}"
