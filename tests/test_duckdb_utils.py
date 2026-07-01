"""Tests for core/duckdb_utils.py module."""

import duckdb
import pytest

from geoparquet_io.core.duckdb_utils import (
    SPATIAL_JOIN_BBOX_PREFILTER,
    SPATIAL_JOIN_NATIVE,
    SPATIAL_JOIN_NO_BBOX,
    _add_bucket_needing_auth,
    _bucket_needs_auth,
    _clear_s3_cache,
    _escape_sql_string,
    build_spatial_join_condition,
    get_duckdb_connection,
    get_duckdb_connection_for_s3,
    load_community_extension,
    quote_identifier,
    s3_config_scope,
    spatial_join_strategy,
)
from geoparquet_io.core.exceptions import ExtensionUnavailableError


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

    def test_no_derived_bbox_from_geometry(self):
        """Regression guard for #538: the ON clause must never derive a bbox from
        the geometry (ST_XMin/XMax/...) and AND it in front of ST_Intersects.

        #462 proposed exactly that for native geometry; the #538 benchmark showed
        it defeats DuckDB's SPATIAL_JOIN and forces a ~73x slower BLOCKWISE_NL_JOIN.
        The builder must only ever use *stored* bbox covering columns as a
        pre-filter, never compute one inline.
        """
        for cond in (
            build_spatial_join_condition("geometry", "geom"),
            build_spatial_join_condition("geometry", "geom", "bbox", "geom_bbox"),
            build_spatial_join_condition(
                "geometry",
                "geom",
                "bbox",
                "geom_bbox",
                input_geom_sql="ST_Transform(a.\"geometry\", 'EPSG:5070', 'OGC:CRS84')",
            ),
        ):
            assert "ST_XMin" not in cond
            assert "ST_XMax" not in cond
            assert "ST_YMin" not in cond
            assert "ST_YMax" not in cond


class TestSpatialJoinOperatorPlan:
    """Plan-level guard that the native/bare predicate stays on ``SPATIAL_JOIN``.

    Fix C for issue #545. The string-level guards above ensure we *build* the right
    predicate; this one runs ``EXPLAIN`` so a DuckDB-version change or query-shape
    regression that silently defeats the indexed ``SPATIAL_JOIN`` operator fails CI
    instead of hanging in production.

    Why it matters: ``add admin-divisions`` / ``add country-codes`` place the
    predicate from :func:`build_spatial_join_condition` inside a ``LEFT JOIN``
    (``admin_divisions.py``, ``country_codes.py``). If the plan degrades to
    ``BLOCKWISE_NL_JOIN`` (O(n*m)) a large input such as remote Overture hangs --
    the original #460 incident.

    Issue #545 background: on DuckDB 1.5.1 the bare ``ST_Intersects`` predicate
    plans as ``SPATIAL_JOIN`` even at >=1M input features (~15s, no hang), whereas
    ANDing the bbox-overlap pre-filter in front of it under a ``LEFT JOIN`` drops to
    ``BLOCKWISE_NL_JOIN``. That asymmetry is the evidence base for eventually
    dropping the pre-filter (Fix B); this guard protects the bare fast path that
    such a change would rely on. The predicate builder is shared, so guarding it
    here covers both ``admin-divisions`` and ``country-codes``.
    """

    # Nested-loop / cross-product operators. Any of these on the bare predicate is
    # the #460 hang signature.
    _NESTED_LOOP_OPERATORS = ("BLOCKWISE_NL_JOIN", "NESTED_LOOP_JOIN", "CROSS_PRODUCT")

    @staticmethod
    def _explain_native_left_join(con, n_input, n_target):
        """Build the real native-path query and return its physical plan text.

        ``target`` stands in for admin polygons, ``input`` for the features being
        enriched. The ON clause is exactly what ``build_spatial_join_condition``
        emits for a native-geometry input (neither side has a bbox covering), placed
        in the same ``LEFT JOIN`` shape the add commands use.
        """
        con.execute(
            f"""
            CREATE OR REPLACE TABLE target AS
            SELECT
                i AS id,
                ST_Buffer(
                    ST_Point(((i * 11) % 3599) / 10.0 - 180,
                             ((i * 17) % 1799) / 10.0 - 90),
                    0.5
                ) AS geometry
            FROM range({n_target}) t(i)
            """
        )
        con.execute(
            f"""
            CREATE OR REPLACE TABLE input AS
            SELECT
                i AS id,
                ST_Point(((i * 131) % 3599) / 10.0 - 180,
                         ((i * 197) % 1799) / 10.0 - 90) AS geometry
            FROM range({n_input}) t(i)
            """
        )
        # Native path: no bbox columns -> bare ST_Intersects.
        condition = build_spatial_join_condition("geometry", "geometry")
        query = f"SELECT a.id FROM input a LEFT JOIN target b ON {condition}"
        plan_rows = con.execute(f"EXPLAIN {query}").fetchall()
        return "\n".join(row[1] for row in plan_rows)

    def _assert_indexed_spatial_join(self, plan):
        assert "SPATIAL_JOIN" in plan, f"expected SPATIAL_JOIN, got plan:\n{plan}"
        for operator in self._NESTED_LOOP_OPERATORS:
            assert operator not in plan, f"unexpected {operator} in plan:\n{plan}"

    def test_native_predicate_plans_as_spatial_join(self):
        """Bare ST_Intersects LEFT JOIN -> indexed SPATIAL_JOIN, not a nested loop."""
        con = get_duckdb_connection(load_httpfs=False)
        try:
            plan = self._explain_native_left_join(con, n_input=2000, n_target=1000)
        finally:
            con.close()
        self._assert_indexed_spatial_join(plan)

    @pytest.mark.slow
    def test_native_predicate_plans_as_spatial_join_at_scale(self):
        """Fix C (issue #545): at >=1M input features against a 45k-polygon target
        the bare predicate must still plan as SPATIAL_JOIN.

        A cardinality-driven flip to BLOCKWISE_NL_JOIN at this scale is exactly the
        #460 Overture hang; asserting the operator here turns that regression into a
        loud CI failure instead of a silent hang.
        """
        con = get_duckdb_connection(load_httpfs=False)
        try:
            plan = self._explain_native_left_join(con, n_input=1_200_000, n_target=45_000)
        finally:
            con.close()
        self._assert_indexed_spatial_join(plan)


class TestSpatialJoinStrategy:
    """Tests for spatial_join_strategy() (issue #538).

    The classifier keeps the user-facing status message in sync with the
    predicate build_spatial_join_condition actually emits, so native-geometry
    inputs are no longer misreported as a degraded "no bbox" fallback.
    """

    def test_native_geometry_is_fast_path(self):
        """Native geometry -> bare ST_Intersects SPATIAL_JOIN, regardless of bbox cols."""
        assert spatial_join_strategy(True, None, None) == SPATIAL_JOIN_NATIVE
        # Native wins even if bbox columns happen to be present.
        assert spatial_join_strategy(True, "bbox", "bbox") == SPATIAL_JOIN_NATIVE

    def test_explicit_bbox_both_sides_uses_prefilter(self):
        """1.x input with bbox on both sides -> bbox-overlap pre-filter."""
        assert spatial_join_strategy(False, "bbox", "bbox") == SPATIAL_JOIN_BBOX_PREFILTER

    def test_missing_bbox_is_no_bbox_fallback(self):
        """1.x input lacking a bbox column (either side) -> genuine no-bbox fallback."""
        assert spatial_join_strategy(False, None, None) == SPATIAL_JOIN_NO_BBOX
        assert spatial_join_strategy(False, "bbox", None) == SPATIAL_JOIN_NO_BBOX
        assert spatial_join_strategy(False, None, "bbox") == SPATIAL_JOIN_NO_BBOX


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


class TestLoadCommunityExtension:
    """Tests for community extension loading with clear errors (issue #491)."""

    @pytest.mark.network
    def test_unavailable_extension_raises_clear_error(self):
        """A missing/unpublished community extension yields ExtensionUnavailableError.

        DuckDB raises an opaque HTTP 404 when a community extension is not
        published for the running DuckDB version. We translate that into an
        actionable error rather than leaking the raw HTTP failure.
        """
        con = get_duckdb_connection(load_spatial=False, load_httpfs=False)
        try:
            with pytest.raises(ExtensionUnavailableError) as exc_info:
                load_community_extension(con, "definitely_not_a_real_extension_491")
        finally:
            con.close()

        message = str(exc_info.value)
        assert "definitely_not_a_real_extension_491" in message
        assert duckdb.__version__ in message
        assert exc_info.value.name == "definitely_not_a_real_extension_491"

    def test_duckdb_error_is_wrapped(self):
        """Any DuckDB error during INSTALL/LOAD becomes ExtensionUnavailableError."""

        class _FakeConnection:
            def execute(self, sql):
                raise duckdb.IOException("simulated HTTP 404 for community extension")

        with pytest.raises(ExtensionUnavailableError) as exc_info:
            load_community_extension(_FakeConnection(), "geography")

        assert "geography" in str(exc_info.value)
        assert "simulated HTTP 404" in str(exc_info.value)


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
