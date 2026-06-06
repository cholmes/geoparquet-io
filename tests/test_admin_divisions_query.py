"""Tests for admin_divisions query building functions."""

from geoparquet_io.core.add.admin_divisions import (
    _build_spatial_join_query,
    _format_input_ref,
    _WriteConfig,
)


class TestFormatInputRef:
    """Test _format_input_ref SQL reference formatting."""

    def test_file_path_is_quoted(self):
        assert _format_input_ref("/path/to/file.parquet") == "'/path/to/file.parquet'"

    def test_url_is_quoted(self):
        assert _format_input_ref("s3://bucket/file.parquet") == "'s3://bucket/file.parquet'"

    def test_temp_table_is_bare(self):
        assert _format_input_ref("_gpio_admin_step_0") == "_gpio_admin_step_0"

    def test_temp_table_prefix_only(self):
        assert _format_input_ref("_gpio_") == "_gpio_"


class TestBuildSpatialJoinQuery:
    """Test _build_spatial_join_query SQL generation."""

    def test_simple_join(self):
        query = _build_spatial_join_query(
            "/data/input.parquet",
            "(SELECT * FROM admin) b",
            'b."country" as "overture_country"',
            "geometry",
            "geometry",
        )
        assert "ST_Intersects" in query
        assert "LEFT JOIN" in query
        assert "'/data/input.parquet'" in query
        assert "_gpio_row_id" not in query

    def test_dedup_has_cte(self):
        query = _build_spatial_join_query(
            "/data/input.parquet",
            "(SELECT * FROM admin) b",
            'b."country" as "overture_country"',
            "geometry",
            "geometry",
            deduplicate=True,
        )
        assert "_gpio_input" in query
        assert "_gpio_row_id" in query
        assert "QUALIFY ROW_NUMBER()" in query
        assert "PARTITION BY" in query

    def test_dedup_has_precomputed_centroid(self):
        query = _build_spatial_join_query(
            "/data/input.parquet",
            "(SELECT * FROM admin) b",
            'b."country" as "overture_country"',
            "geometry",
            "geometry",
            deduplicate=True,
        )
        assert "_gpio_centroid" in query
        assert "ST_Centroid" in query
        assert "EXCLUDE (_gpio_row_id, _gpio_centroid)" in query

    def test_dedup_has_deterministic_tiebreaker(self):
        query = _build_spatial_join_query(
            "/data/input.parquet",
            "(SELECT * FROM admin) b",
            'b."country" as "overture_country"',
            "geometry",
            "geometry",
            deduplicate=True,
        )
        assert "b.rowid" in query

    def test_temp_table_input_is_bare(self):
        query = _build_spatial_join_query(
            "_gpio_admin_step_0",
            "(SELECT * FROM admin) b",
            'b."region" as "overture_region"',
            "geometry",
            "geometry",
        )
        assert "_gpio_admin_step_0 a" in query
        assert "'" not in query.split("FROM")[1].split("a")[0]

    def test_quotes_geometry_columns(self):
        query = _build_spatial_join_query(
            "/data/input.parquet",
            "(SELECT * FROM admin) b",
            'b."country" as "overture_country"',
            "my geometry",
            "admin geom",
        )
        assert '"my geometry"' in query
        assert '"admin geom"' in query


class TestWriteConfig:
    """Test _WriteConfig dataclass."""

    def test_defaults(self):
        config = _WriteConfig()
        assert config.compression == "ZSTD"
        assert config.compression_level is None
        assert config.row_group_size_mb is None
        assert config.row_group_rows is None
        assert config.profile is None
        assert config.geoparquet_version is None

    def test_custom_values(self):
        config = _WriteConfig(
            compression="GZIP",
            compression_level=6,
            row_group_size_mb=128.0,
        )
        assert config.compression == "GZIP"
        assert config.compression_level == 6
        assert config.row_group_size_mb == 128.0
