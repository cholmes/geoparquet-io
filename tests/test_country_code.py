"""
Tests for find_country_code_column function.
"""

import logging
import os
import tempfile

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.add.country_codes import add_country_codes, find_country_code_column


class TestFindCountryCodeColumn:
    """Test suite for find_country_code_column function."""

    def create_test_parquet(self, columns, data, filename):
        """Helper to create a test parquet file with specified columns."""
        table_dict = {}
        for col, values in zip(columns, data, strict=True):
            table_dict[col] = values

        table = pa.table(table_dict)
        pq.write_table(table, filename)
        return filename

    def test_find_admin_country_code_column(self):
        """Test finding admin:country_code column."""
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            tmp_name = tmp.name

        try:
            # Create test file with admin:country_code column
            self.create_test_parquet(
                ["id", "admin:country_code", "name"],
                [[1, 2], ["US", "CA"], ["Place1", "Place2"]],
                tmp_name,
            )

            con = duckdb.connect()
            try:
                con.execute("INSTALL spatial;")
                con.execute("LOAD spatial;")

                result = find_country_code_column(con, tmp_name, is_subquery=False)
                assert result == "admin:country_code"
            finally:
                con.close()
        finally:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)

    def test_find_country_column(self):
        """Test finding country column."""
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            tmp_name = tmp.name

        try:
            # Create test file with country column
            self.create_test_parquet(
                ["id", "country", "name"],
                [[1, 2], ["US", "CA"], ["Place1", "Place2"]],
                tmp_name,
            )

            con = duckdb.connect()
            try:
                con.execute("INSTALL spatial;")
                con.execute("LOAD spatial;")

                result = find_country_code_column(con, tmp_name, is_subquery=False)
                assert result == "country"
            finally:
                con.close()
        finally:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)

    def test_find_iso_a2_column(self):
        """Test finding ISO_A2 column."""
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            tmp_name = tmp.name

        try:
            # Create test file with ISO_A2 column
            self.create_test_parquet(
                ["id", "ISO_A2", "name"], [[1, 2], ["US", "CA"], ["Place1", "Place2"]], tmp_name
            )

            con = duckdb.connect()
            try:
                con.execute("INSTALL spatial;")
                con.execute("LOAD spatial;")

                result = find_country_code_column(con, tmp_name, is_subquery=False)
                assert result == "ISO_A2"
            finally:
                con.close()
        finally:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)

    def test_priority_order(self):
        """Test that columns are found in priority order."""
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            tmp_name = tmp.name

        try:
            # Create test file with multiple matching columns
            self.create_test_parquet(
                ["id", "ISO_A2", "country", "admin:country_code"],
                [[1, 2], ["US", "CA"], ["USA", "CAN"], ["US", "CA"]],
                tmp_name,
            )

            con = duckdb.connect()
            try:
                con.execute("INSTALL spatial;")
                con.execute("LOAD spatial;")

                result = find_country_code_column(con, tmp_name, is_subquery=False)
                # Should find admin:country_code first due to priority
                assert result == "admin:country_code"
            finally:
                con.close()
        finally:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)

    def test_no_country_column_raises_error(self):
        """Test that error is raised when no country column is found."""
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            tmp_name = tmp.name

        try:
            # Create test file without any country column
            self.create_test_parquet(
                ["id", "name", "value"], [[1, 2], ["Place1", "Place2"], [100, 200]], tmp_name
            )

            con = duckdb.connect()
            try:
                con.execute("INSTALL spatial;")
                con.execute("LOAD spatial;")

                from geoparquet_io.core.exceptions import GeoParquetError

                with pytest.raises(GeoParquetError) as exc_info:
                    find_country_code_column(con, tmp_name, is_subquery=False)

                assert "could not find country code column" in str(exc_info.value).lower()
            finally:
                con.close()
        finally:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)

    def test_with_subquery(self):
        """Test finding column with a subquery source."""
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as tmp:
            tmp_name = tmp.name

        try:
            # Create test file
            self.create_test_parquet(
                ["id", "country", "name"],
                [[1, 2], ["US", "CA"], ["Place1", "Place2"]],
                tmp_name,
            )

            con = duckdb.connect()
            try:
                con.execute("INSTALL spatial;")
                con.execute("LOAD spatial;")

                # Create a subquery
                subquery = f"(SELECT * FROM '{tmp_name}')"

                result = find_country_code_column(con, subquery, is_subquery=True)
                assert result == "country"
            finally:
                con.close()
        finally:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)


class TestCountryCodesDryRun:
    """Dry-run status-message regression tests for add_country_codes.

    The country_codes module is reached via benchmarks/legacy paths rather than a
    CLI command, so the dry-run flow is exercised by calling the core function
    directly with a local countries file (no network).
    """

    def test_dry_run_native_geometry_uses_spatial_join(self, fields_v2_file, caplog):
        """Native-geometry input reports the SPATIAL_JOIN fast path, not a fallback.

        Mirrors ``test_dry_run_with_native_geometry_input`` for admin-divisions
        (#538): a GeoParquet 2.0 / geo-typed input nulls the bbox columns, so the
        ON clause is a bare ST_Intersects that DuckDB's SPATIAL_JOIN operator
        accelerates. The dry-run note must say so rather than the misleading
        "no bbox optimization" fallback message.
        """
        with caplog.at_level(logging.INFO, logger="geoparquet_io"):
            add_country_codes(
                input_parquet=fields_v2_file,
                countries_parquet=fields_v2_file,
                output_parquet="output.parquet",
                add_bbox_flag=False,
                dry_run=True,
                verbose=False,
            )

        output = caplog.text
        # Native geometry is the fast SPATIAL_JOIN path, and the ON clause is a
        # bare ST_Intersects (no bbox pre-filter).
        assert "Using native geometry with DuckDB SPATIAL_JOIN" in output
        assert "no bbox optimization" not in output
        assert "ON ST_Intersects" in output
