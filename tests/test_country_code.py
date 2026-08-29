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


class TestCountryCodesLocalCountriesFile:
    """End-to-end (non-dry-run) runs against a local countries file, no network.

    The dry-run tests above return before the connection is used for real work,
    so nothing covered the write path -- which is also the path the DuckDB
    connection context manager wraps.
    """

    @staticmethod
    def _make_countries_file(source_parquet, dest):
        """Build a countries file from ``source_parquet``'s geometries.

        Reusing the input's own geometries guarantees every feature joins, so
        the assertions below are about plumbing rather than spatial luck.
        """
        con = duckdb.connect()
        try:
            con.execute("INSTALL spatial; LOAD spatial;")
            con.execute(
                f"COPY (SELECT geometry, 'US' AS country_code, "
                f"'US-CA' AS subdivision_code FROM read_parquet('{source_parquet}')) "
                f"TO '{dest}' (FORMAT PARQUET)"
            )
        finally:
            con.close()
        return str(dest)

    def test_local_countries_file_joins_and_writes(self, fields_v2_file, tmp_path):
        """A non-default --countries file produces country/subdivision columns.

        Regression test for the doubled quoting in _determine_code_columns:
        _setup_countries_source hands back an already-quoted URL, and
        find_subdivision_code_column quoted it a second time, so every
        non-default countries file died on `FROM ''/path''`.
        """
        countries = self._make_countries_file(fields_v2_file, tmp_path / "countries.parquet")
        output = tmp_path / "out.parquet"

        add_country_codes(
            input_parquet=fields_v2_file,
            countries_parquet=countries,
            output_parquet=str(output),
            add_bbox_flag=False,
            dry_run=False,
            verbose=True,
        )

        assert output.exists()
        table = pq.read_table(output)
        assert "admin:country_code" in table.column_names
        assert "admin:subdivision_code" in table.column_names
        assert table.num_rows == pq.read_table(fields_v2_file).num_rows
        assert set(table.column("admin:country_code").to_pylist()) == {"US"}
        assert set(table.column("admin:subdivision_code").to_pylist()) == {"US-CA"}

    def test_local_countries_file_reports_totals(self, fields_v2_file, tmp_path, caplog):
        """The run reports its input count and its results summary."""
        countries = self._make_countries_file(fields_v2_file, tmp_path / "countries.parquet")
        output = tmp_path / "out.parquet"

        with caplog.at_level(logging.INFO, logger="geoparquet_io"):
            add_country_codes(
                input_parquet=fields_v2_file,
                countries_parquet=countries,
                output_parquet=str(output),
                add_bbox_flag=False,
                dry_run=False,
                verbose=True,
            )

        assert "input features..." in caplog.text
        assert "Added country codes to" in caplog.text
        assert "Found 1 unique countries" in caplog.text

    @staticmethod
    def _make_countries_file_without_subdivision(source_parquet, dest):
        """Build a countries file with a country column but no subdivision column."""
        con = duckdb.connect()
        try:
            con.execute("INSTALL spatial; LOAD spatial;")
            con.execute(
                f"COPY (SELECT geometry, 'US' AS country_code "
                f"FROM read_parquet('{source_parquet}')) "
                f"TO '{dest}' (FORMAT PARQUET)"
            )
        finally:
            con.close()
        return str(dest)

    def test_countries_file_without_subdivision_column(self, fields_v2_file, tmp_path):
        """A countries file with no subdivision column must not crash the summary.

        Regression test for #672: ``_print_results_summary`` hardcoded
        ``"admin:subdivision_code"`` in its stats query, so a countries file
        without a subdivision column raised a DuckDB BinderException *after*
        the output had already been written -- a successful run reported as a
        failure.
        """
        countries = self._make_countries_file_without_subdivision(
            fields_v2_file, tmp_path / "countries.parquet"
        )
        output = tmp_path / "out.parquet"

        add_country_codes(
            input_parquet=fields_v2_file,
            countries_parquet=countries,
            output_parquet=str(output),
            add_bbox_flag=False,
            dry_run=False,
            verbose=True,
        )

        table = pq.read_table(output)
        assert "admin:country_code" in table.column_names
        assert "admin:subdivision_code" not in table.column_names
        assert set(table.column("admin:country_code").to_pylist()) == {"US"}

    def test_summary_reports_only_country_stats_without_subdivision(
        self, fields_v2_file, tmp_path, caplog
    ):
        """The summary omits subdivision lines when there is no subdivision column."""
        countries = self._make_countries_file_without_subdivision(
            fields_v2_file, tmp_path / "countries.parquet"
        )
        output = tmp_path / "out.parquet"

        with caplog.at_level(logging.INFO, logger="geoparquet_io"):
            add_country_codes(
                input_parquet=fields_v2_file,
                countries_parquet=countries,
                output_parquet=str(output),
                add_bbox_flag=False,
                dry_run=False,
                verbose=True,
            )

        assert "Added country codes to" in caplog.text
        assert "Found 1 unique countries" in caplog.text
        assert "subdivision" not in caplog.text

    def test_output_path_with_apostrophe(self, fields_v2_file, tmp_path):
        """An output directory containing an apostrophe must not break the summary.

        The summary query interpolates the output path as a SQL string literal;
        an unescaped ``'`` broke it the same way #672 did -- after the file was
        already written.
        """
        countries = self._make_countries_file(fields_v2_file, tmp_path / "countries.parquet")
        odd_dir = tmp_path / "o'brien"
        odd_dir.mkdir()
        output = odd_dir / "out.parquet"

        add_country_codes(
            input_parquet=fields_v2_file,
            countries_parquet=countries,
            output_parquet=str(output),
            add_bbox_flag=False,
            dry_run=False,
            verbose=True,
        )

        assert pq.read_table(output).num_rows == pq.read_table(fields_v2_file).num_rows
