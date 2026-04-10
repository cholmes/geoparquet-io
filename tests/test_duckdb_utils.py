"""Tests for core/duckdb_utils.py module."""

from geoparquet_io.core.duckdb_utils import (
    _escape_sql_string,
    get_duckdb_connection,
    quote_identifier,
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
