"""Tests for admin_divisions query building functions."""

import duckdb
import pytest

from geoparquet_io.core.add.admin_divisions import (
    _build_admin_subquery,
    _build_spatial_join_query,
    _format_input_ref,
    _WriteConfig,
)


class _FakeDataset:
    """Minimal dataset stub for query-building tests."""

    def get_output_column_name(self, level, prefix=None):
        return f"overture_{level}"

    def get_column_transform(self, level):
        return None


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
        assert "b._gpio_admin_rid" in query

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


@pytest.fixture
def spatial_con():
    """In-memory DuckDB connection with spatial loaded and test tables."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute(
        """CREATE TABLE _gpio_input_src AS
        SELECT ST_Point(0.5, 0.5) AS geometry,
               {'xmin': 0.5, 'xmax': 0.5, 'ymin': 0.5, 'ymax': 0.5}::
                   STRUCT(xmin DOUBLE, xmax DOUBLE, ymin DOUBLE, ymax DOUBLE) AS bbox,
               1 AS id"""
    )
    # Two identical overlapping polygons that both contain the point: forces
    # the deduplication tiebreaker to run.
    con.execute(
        """CREATE TABLE admin_src AS
        SELECT ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))') AS geometry,
               {'xmin':0.0,'xmax':1.0,'ymin':0.0,'ymax':1.0}::
                   STRUCT(xmin DOUBLE,xmax DOUBLE,ymin DOUBLE,ymax DOUBLE) AS bbox,
               'AA' AS country
        UNION ALL
        SELECT ST_GeomFromText('POLYGON((0 0,1 0,1 1,0 1,0 0))'),
               {'xmin':0.0,'xmax':1.0,'ymin':0.0,'ymax':1.0}::
                   STRUCT(xmin DOUBLE,xmax DOUBLE,ymin DOUBLE,ymax DOUBLE),
               'BB'"""
    )
    yield con
    con.close()


class TestSpatialJoinQueryExecution:
    """Execute generated SQL to catch binder/runtime errors string checks miss."""

    def _build(self, deduplicate):
        sub = _build_admin_subquery(
            _FakeDataset(), ["country"], ["country"], "admin_src", "geometry", "bbox", []
        )
        return _build_spatial_join_query(
            "_gpio_input_src",
            sub,
            'b."country" as "overture_country"',
            "geometry",
            "geometry",
            input_bbox_col="bbox",
            admin_bbox_col="bbox",
            deduplicate=deduplicate,
        )

    def test_dedup_query_binds_and_collapses(self, spatial_con):
        """Dedup query must bind (b._gpio_admin_rid exists) and yield one row."""
        query = self._build(deduplicate=True)
        count, country = spatial_con.execute(
            f"SELECT COUNT(*), MIN(overture_country) FROM ({query})"
        ).fetchone()
        assert count == 1
        # Deterministic: lowest _gpio_admin_rid wins the tie.
        assert country == "AA"

    def test_non_dedup_query_binds(self, spatial_con):
        """Non-dedup query must bind and run against real tables."""
        query = self._build(deduplicate=False)
        count = spatial_con.execute(f"SELECT COUNT(*) FROM ({query})").fetchone()[0]
        # Point matches both polygons; without dedup both rows are returned.
        assert count == 2
