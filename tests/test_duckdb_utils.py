"""Tests for core/duckdb_utils.py module."""

from geoparquet_io.core.duckdb_utils import (
    _add_bucket_needing_auth,
    _bucket_needs_auth,
    _clear_s3_cache,
    _escape_sql_string,
    get_duckdb_connection,
    get_duckdb_connection_for_s3,
    quote_identifier,
    s3_config_scope,
)


class TestEscapeSqlString:
    """Tests for SQL string escaping to prevent injection."""

    def test_normal_string_unchanged(self):
        """Normal strings without quotes pass through unchanged."""
        assert _escape_sql_string("normal") == "normal"
        assert _escape_sql_string("path/to/file.parquet") == "path/to/file.parquet"
        assert _escape_sql_string("s3://bucket/data.parquet") == "s3://bucket/data.parquet"

    def test_single_quote_escaped(self):
        """Single quotes are doubled for SQL escaping."""
        assert _escape_sql_string("it's") == "it''s"
        assert _escape_sql_string("O'Brien") == "O''Brien"

    def test_multiple_quotes_escaped(self):
        """Multiple single quotes are all escaped."""
        assert _escape_sql_string("test'';DROP") == "test'''';DROP"
        assert _escape_sql_string("a'b'c") == "a''b''c"

    def test_sql_injection_attempt_neutralized(self):
        """SQL injection attempts are safely escaped."""
        # Classic SQL injection pattern
        malicious = "s3://bucket/test'; DROP TABLE users; --"
        escaped = _escape_sql_string(malicious)
        assert escaped == "s3://bucket/test''; DROP TABLE users; --"
        # The doubled quote prevents the string from terminating early

    def test_empty_string(self):
        """Empty string returns empty string."""
        assert _escape_sql_string("") == ""

    def test_only_quotes(self):
        """String with only quotes gets them all escaped."""
        assert _escape_sql_string("'") == "''"
        assert _escape_sql_string("''") == "''''"
        assert _escape_sql_string("'''") == "''''''"


class TestQuoteIdentifier:
    """Tests for SQL identifier quoting."""

    def test_simple_identifier(self):
        """Simple identifiers are wrapped in double quotes."""
        assert quote_identifier("column") == '"column"'
        assert quote_identifier("table_name") == '"table_name"'

    def test_identifier_with_spaces(self):
        """Identifiers with spaces are safely quoted."""
        assert quote_identifier("my column") == '"my column"'

    def test_identifier_with_double_quotes(self):
        """Embedded double quotes are escaped by doubling."""
        assert quote_identifier('col"name') == '"col""name"'

    def test_reserved_words(self):
        """SQL reserved words are safely quoted."""
        assert quote_identifier("select") == '"select"'
        assert quote_identifier("from") == '"from"'


class TestGetDuckdbConnection:
    """Tests for DuckDB connection creation."""

    def test_basic_connection(self):
        """Basic connection can be created."""
        con = get_duckdb_connection(load_spatial=False, load_httpfs=False)
        assert con is not None
        result = con.execute("SELECT 1").fetchone()
        assert result == (1,)
        con.close()

    def test_thread_limit(self):
        """Connection respects thread limit."""
        con = get_duckdb_connection(load_spatial=False, load_httpfs=False, threads=1)
        assert con is not None
        con.close()


class TestS3CacheThreadSafety:
    """Tests for thread-safe S3 bucket authentication cache."""

    def test_clear_s3_cache_removes_buckets(self):
        """_clear_s3_cache removes all cached buckets."""
        # Add a test bucket
        _add_bucket_needing_auth("test-bucket-1")
        _add_bucket_needing_auth("test-bucket-2")
        assert _bucket_needs_auth("test-bucket-1")
        assert _bucket_needs_auth("test-bucket-2")

        # Clear should remove all
        _clear_s3_cache()
        assert not _bucket_needs_auth("test-bucket-1")
        assert not _bucket_needs_auth("test-bucket-2")

    def test_add_and_check_bucket(self):
        """_add_bucket_needing_auth and _bucket_needs_auth work correctly."""
        _clear_s3_cache()  # Start fresh

        # Initially empty
        assert not _bucket_needs_auth("new-bucket")

        # Add and verify
        _add_bucket_needing_auth("new-bucket")
        assert _bucket_needs_auth("new-bucket")

        # Cleanup
        _clear_s3_cache()

    def test_idempotent_add(self):
        """Adding same bucket multiple times is safe."""
        _clear_s3_cache()

        _add_bucket_needing_auth("idempotent-bucket")
        _add_bucket_needing_auth("idempotent-bucket")
        _add_bucket_needing_auth("idempotent-bucket")

        assert _bucket_needs_auth("idempotent-bucket")
        _clear_s3_cache()


class TestGetDuckdbConnectionS3Config:
    """Tests for S3 endpoint configuration on DuckDB connections."""

    def test_s3_endpoint_sets_duckdb_vars(self):
        """s3_endpoint param emits SET s3_endpoint, s3_url_style, s3_use_ssl."""
        con = get_duckdb_connection(
            load_spatial=False,
            load_httpfs=True,
            s3_endpoint="data.source.coop",
            s3_use_ssl=True,
        )
        result = con.execute("SELECT current_setting('s3_endpoint')").fetchone()
        assert result[0] == "data.source.coop"

        result = con.execute("SELECT current_setting('s3_url_style')").fetchone()
        assert result[0] == "path"

        result = con.execute("SELECT current_setting('s3_use_ssl')").fetchone()
        assert result[0] is True
        con.close()

    def test_s3_region_sets_duckdb_var(self):
        """s3_region param emits SET s3_region."""
        con = get_duckdb_connection(
            load_spatial=False,
            load_httpfs=True,
            s3_region="eu-west-1",
        )
        result = con.execute("SELECT current_setting('s3_region')").fetchone()
        assert result[0] == "eu-west-1"
        con.close()

    def test_s3_no_ssl_sets_duckdb_var(self):
        """s3_use_ssl=False emits SET s3_use_ssl=false."""
        con = get_duckdb_connection(
            load_spatial=False,
            load_httpfs=True,
            s3_endpoint="minio.local:9000",
            s3_use_ssl=False,
        )
        result = con.execute("SELECT current_setting('s3_use_ssl')").fetchone()
        assert result[0] is False
        con.close()

    def test_no_s3_params_no_set_statements(self):
        """Without S3 params, no custom SET statements are emitted."""
        con = get_duckdb_connection(load_spatial=False, load_httpfs=False)
        result = con.execute("SELECT current_setting('s3_endpoint')").fetchone()
        assert not result[0]
        con.close()

    def test_get_duckdb_connection_for_s3_forwards_endpoint(self):
        """get_duckdb_connection_for_s3() forwards S3 endpoint params."""
        con = get_duckdb_connection_for_s3(
            "/local/file.parquet",
            load_spatial=False,
            s3_endpoint="data.source.coop",
        )
        result = con.execute("SELECT current_setting('s3_endpoint')").fetchone()
        assert result[0] == "data.source.coop"
        con.close()


class TestAmbientS3Config:
    """Tests for ambient S3 config via s3_config_scope()."""

    def test_ambient_config_picked_up_by_connection(self):
        """get_duckdb_connection() reads ambient config when no explicit kwargs."""
        with s3_config_scope({"s3_endpoint": "ambient.example.com", "s3_use_ssl": True}):
            con = get_duckdb_connection(load_spatial=False, load_httpfs=True)
            result = con.execute("SELECT current_setting('s3_endpoint')").fetchone()
            assert result[0] == "ambient.example.com"
            con.close()

    def test_explicit_kwargs_override_ambient(self):
        """Explicit s3_endpoint kwarg wins over ambient config."""
        with s3_config_scope({"s3_endpoint": "ambient.example.com"}):
            con = get_duckdb_connection(
                load_spatial=False,
                load_httpfs=True,
                s3_endpoint="explicit.example.com",
            )
            result = con.execute("SELECT current_setting('s3_endpoint')").fetchone()
            assert result[0] == "explicit.example.com"
            con.close()

    def test_ambient_config_cleared_after_scope(self):
        """Ambient config is cleaned up when scope exits."""
        with s3_config_scope({"s3_endpoint": "scoped.example.com"}):
            pass
        con = get_duckdb_connection(load_spatial=False, load_httpfs=False)
        result = con.execute("SELECT current_setting('s3_endpoint')").fetchone()
        assert not result[0]
        con.close()

    def test_ambient_region_picked_up(self):
        """Ambient s3_region is picked up by get_duckdb_connection()."""
        with s3_config_scope({"s3_region": "eu-west-1"}):
            con = get_duckdb_connection(load_spatial=False, load_httpfs=True)
            result = con.execute("SELECT current_setting('s3_region')").fetchone()
            assert result[0] == "eu-west-1"
            con.close()
