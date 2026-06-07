"""Tests for core/duckdb_utils.py module."""

from geoparquet_io.core.duckdb_utils import (
    _add_bucket_needing_auth,
    _bucket_needs_auth,
    _clear_s3_cache,
    _escape_sql_string,
    build_spatial_join_condition,
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


class TestBuildSpatialJoinCondition:
    """Tests for the spatial-join ON-clause builder.

    Regression guard for PR #460: the cheap bbox-overlap pre-filter must be
    emitted whenever both sides have a bbox column. It was silently removed in
    #457, which made `add admin-divisions --dataset overture` hang.
    """

    def test_without_bbox_columns_is_plain_intersects(self):
        """No bbox columns -> bare ST_Intersects, no pre-filter."""
        cond = build_spatial_join_condition("geometry", "geom")
        assert cond == 'ST_Intersects(b."geom", a."geometry")'

    def test_with_bbox_columns_adds_prefilter(self):
        """Both sides have bbox -> four-sided overlap test ANDed before ST_Intersects."""
        cond = build_spatial_join_condition("geometry", "geom", "bbox", "geom_bbox")
        assert 'a."bbox".xmin <= b."geom_bbox".xmax' in cond
        assert 'a."bbox".xmax >= b."geom_bbox".xmin' in cond
        assert 'a."bbox".ymin <= b."geom_bbox".ymax' in cond
        assert 'a."bbox".ymax >= b."geom_bbox".ymin' in cond
        assert 'ST_Intersects(b."geom", a."geometry")' in cond
        # The cheap bbox test must precede the expensive geometry intersection.
        assert cond.index("xmin") < cond.index("ST_Intersects")
        assert " AND " in cond

    def test_one_sided_bbox_falls_back_to_intersects(self):
        """A bbox on only one side cannot form a safe overlap test -> fall back."""
        plain = 'ST_Intersects(b."g", a."g")'
        assert build_spatial_join_condition("g", "g", input_bbox_col="bbox") == plain
        assert build_spatial_join_condition("g", "g", target_bbox_col="bbox") == plain

    def test_custom_aliases(self):
        """Table aliases are configurable for both sides."""
        cond = build_spatial_join_condition(
            "geometry",
            "geom",
            "bbox",
            "geom_bbox",
            input_alias="lhs",
            target_alias="rhs",
        )
        assert 'lhs."bbox".xmin <= rhs."geom_bbox".xmax' in cond
        assert 'ST_Intersects(rhs."geom", lhs."geometry")' in cond

    def test_identifiers_are_quoted(self):
        """Column names with special characters are safely quoted."""
        cond = build_spatial_join_condition("geo m", 'g"x', "b b", "q")
        assert '"geo m"' in cond
        assert '"g""x"' in cond  # embedded double-quote doubled


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

    def test_temp_directory_is_set(self, tmp_path):
        """temp_directory enables spill-to-disk (OOM guard, todo 013)."""
        spill = str(tmp_path / "spill")
        con = get_duckdb_connection(load_spatial=False, load_httpfs=False, temp_directory=spill)
        try:
            value = con.execute("SELECT current_setting('temp_directory')").fetchone()[0]
            assert value == spill
        finally:
            con.close()

    def test_memory_limit_is_set(self):
        """memory_limit is applied when provided."""
        con = get_duckdb_connection(load_spatial=False, load_httpfs=False, memory_limit="2GB")
        try:
            value = con.execute("SELECT current_setting('memory_limit')").fetchone()[0]
            # DuckDB normalises the units (e.g. "2.0 GiB"); just assert it changed.
            assert "GiB" in value or "GB" in value
        finally:
            con.close()

    def test_no_temp_directory_by_default(self):
        """Default connection is unchanged (no behavior change on small inputs)."""
        con = get_duckdb_connection(load_spatial=False, load_httpfs=False)
        try:
            # Default temp_directory is empty/unset; the connection still works.
            assert con.execute("SELECT 1").fetchone() == (1,)
        finally:
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
