"""Tests for multi-geometry column support.

GeoParquet files can have multiple geometry columns (e.g., 'geometry' for point
locations and 'boundary' for polygon boundaries). This module tests that gpio
correctly preserves all geometry columns during conversion.

Key behaviors tested:
- Detection of all geometry columns from GeoParquet metadata
- Preservation of secondary geometry columns in output
- Metadata generation for all geometry columns
- Bbox/Hilbert computed from primary column only (documented behavior)
"""

import json
from pathlib import Path

import pyarrow.parquet as pq

from tests.fixtures.multi_geometry import (
    create_multi_geometry_geoparquet,
    create_multi_geometry_geoparquet_different_crs,
)


class TestMultiGeometryDetection:
    """Tests for detecting multiple geometry columns from input files."""

    def test_detect_all_geometry_columns_from_geoparquet(self, tmp_path):
        """Should detect all geometry columns from GeoParquet metadata."""
        from geoparquet_io.core.convert import detect_all_geometry_columns

        input_file = tmp_path / "multi_geom.parquet"
        create_multi_geometry_geoparquet(str(input_file))

        columns = detect_all_geometry_columns(str(input_file))

        assert columns["primary"] == "geometry"
        assert "boundary" in columns["secondary"]
        assert len(columns["secondary"]) == 1

    def test_detect_preserves_column_metadata(self, tmp_path):
        """Should preserve per-column metadata (encoding, crs, geometry_types)."""
        from geoparquet_io.core.convert import detect_all_geometry_columns

        input_file = tmp_path / "multi_geom.parquet"
        create_multi_geometry_geoparquet(str(input_file))

        columns = detect_all_geometry_columns(str(input_file))

        # Primary column metadata
        assert columns["metadata"]["geometry"]["encoding"] == "WKB"
        assert "Point" in columns["metadata"]["geometry"]["geometry_types"]

        # Secondary column metadata
        assert columns["metadata"]["boundary"]["encoding"] == "WKB"
        assert "Polygon" in columns["metadata"]["boundary"]["geometry_types"]

    def test_detect_single_geometry_file_returns_empty_secondary(self, tmp_path):
        """Single-geometry files should return empty secondary list."""
        import duckdb

        from geoparquet_io.core.convert import detect_all_geometry_columns

        input_file = tmp_path / "single_geom.parquet"
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        con.execute(f"""
            COPY (
                SELECT 1 as id, ST_Point(0, 0) as geometry
            ) TO '{input_file}' (FORMAT PARQUET)
        """)
        con.close()

        columns = detect_all_geometry_columns(str(input_file))

        assert columns["primary"] is not None
        assert columns["secondary"] == []


class TestMultiGeometryConversion:
    """Tests for converting files with multiple geometry columns."""

    def test_convert_preserves_all_geometry_columns(self, tmp_path):
        """Converting should preserve all geometry columns in output."""
        from geoparquet_io.core.convert import convert_to_geoparquet

        input_file = tmp_path / "input.parquet"
        output_file = tmp_path / "output.parquet"
        create_multi_geometry_geoparquet(str(input_file))

        convert_to_geoparquet(str(input_file), str(output_file), skip_hilbert=True)

        # Read output and verify both columns exist
        table = pq.read_table(str(output_file))
        column_names = [f.name for f in table.schema]

        assert "geometry" in column_names
        assert "boundary" in column_names

    def test_convert_preserves_geometry_metadata_for_all_columns(self, tmp_path):
        """Output metadata should include all geometry columns."""
        from geoparquet_io.core.convert import convert_to_geoparquet

        input_file = tmp_path / "input.parquet"
        output_file = tmp_path / "output.parquet"
        create_multi_geometry_geoparquet(str(input_file))

        convert_to_geoparquet(str(input_file), str(output_file), skip_hilbert=True)

        # Read geo metadata
        meta = pq.read_metadata(str(output_file))
        geo_meta = json.loads(meta.metadata[b"geo"].decode("utf-8"))

        # Both columns should be in the columns dict
        assert "geometry" in geo_meta["columns"]
        assert "boundary" in geo_meta["columns"]

        # Both should have encoding
        assert geo_meta["columns"]["geometry"]["encoding"] == "WKB"
        assert geo_meta["columns"]["boundary"]["encoding"] == "WKB"

    def test_convert_preserves_primary_column_designation(self, tmp_path):
        """Output should preserve the primary_column from input."""
        from geoparquet_io.core.convert import convert_to_geoparquet

        input_file = tmp_path / "input.parquet"
        output_file = tmp_path / "output.parquet"
        create_multi_geometry_geoparquet(str(input_file))

        convert_to_geoparquet(str(input_file), str(output_file), skip_hilbert=True)

        meta = pq.read_metadata(str(output_file))
        geo_meta = json.loads(meta.metadata[b"geo"].decode("utf-8"))

        assert geo_meta["primary_column"] == "geometry"

    def test_convert_preserves_crs_for_all_columns(self, tmp_path):
        """CRS should be preserved for all geometry columns.

        Note: EPSG:4326 is the default CRS and may be omitted per GeoParquet spec.
        When CRS is missing, it implies EPSG:4326.
        """
        from geoparquet_io.core.convert import convert_to_geoparquet

        input_file = tmp_path / "input.parquet"
        output_file = tmp_path / "output.parquet"
        create_multi_geometry_geoparquet(str(input_file))

        convert_to_geoparquet(str(input_file), str(output_file), skip_hilbert=True)

        meta = pq.read_metadata(str(output_file))
        geo_meta = json.loads(meta.metadata[b"geo"].decode("utf-8"))

        # Both columns should have CRS preserved or be omitted (implying 4326)
        geom_crs = geo_meta["columns"]["geometry"].get("crs")
        boundary_crs = geo_meta["columns"]["boundary"].get("crs")

        # Per GeoParquet spec: missing CRS implies EPSG:4326
        # Either CRS is present with 4326, or it's omitted (None)
        if geom_crs is not None:
            assert geom_crs.get("id", {}).get("code") == 4326
        # Missing CRS is valid for 4326

        if boundary_crs is not None:
            assert boundary_crs.get("id", {}).get("code") == 4326
        # Missing CRS is valid for 4326

    def test_convert_preserves_different_crs_per_column(self, tmp_path):
        """Each geometry column's CRS should be preserved independently.

        Note: EPSG:4326 (primary) may be omitted per GeoParquet spec.
        EPSG:3857 (secondary) should be explicitly preserved.
        """
        from geoparquet_io.core.convert import convert_to_geoparquet

        input_file = tmp_path / "input.parquet"
        output_file = tmp_path / "output.parquet"
        create_multi_geometry_geoparquet_different_crs(str(input_file))

        convert_to_geoparquet(str(input_file), str(output_file), skip_hilbert=True)

        meta = pq.read_metadata(str(output_file))
        geo_meta = json.loads(meta.metadata[b"geo"].decode("utf-8"))

        # Primary (geometry) is 4326 - may be omitted
        geom_crs = geo_meta["columns"]["geometry"].get("crs")
        if geom_crs is not None:
            assert geom_crs.get("id", {}).get("code") == 4326

        # Secondary (boundary) is 3857 - must be explicitly preserved
        boundary_crs = geo_meta["columns"]["boundary"].get("crs", {})
        assert boundary_crs.get("id", {}).get("code") == 3857

    def test_convert_preserves_geometry_types(self, tmp_path):
        """geometry_types should be preserved for all columns."""
        from geoparquet_io.core.convert import convert_to_geoparquet

        input_file = tmp_path / "input.parquet"
        output_file = tmp_path / "output.parquet"
        create_multi_geometry_geoparquet(str(input_file))

        convert_to_geoparquet(str(input_file), str(output_file), skip_hilbert=True)

        meta = pq.read_metadata(str(output_file))
        geo_meta = json.loads(meta.metadata[b"geo"].decode("utf-8"))

        # Check geometry_types preserved
        assert "Point" in geo_meta["columns"]["geometry"].get("geometry_types", [])
        assert "Polygon" in geo_meta["columns"]["boundary"].get("geometry_types", [])


class TestMultiGeometryBboxAndHilbert:
    """Tests for bbox and Hilbert ordering with multiple geometry columns.

    DOCUMENTED BEHAVIOR: Bbox column and Hilbert ordering are computed based
    on the PRIMARY geometry column only. Secondary geometry columns are
    preserved but do not influence spatial indexing.
    """

    def test_bbox_added_for_primary_column_only(self, tmp_path):
        """Bbox covering metadata should reference primary column only."""
        from geoparquet_io.core.convert import convert_to_geoparquet

        input_file = tmp_path / "input.parquet"
        output_file = tmp_path / "output.parquet"
        create_multi_geometry_geoparquet(str(input_file))

        # Convert with bbox (GeoParquet 1.1)
        convert_to_geoparquet(
            str(input_file), str(output_file), skip_hilbert=True, geoparquet_version="1.1"
        )

        meta = pq.read_metadata(str(output_file))
        geo_meta = json.loads(meta.metadata[b"geo"].decode("utf-8"))

        secondary_meta = geo_meta["columns"]["boundary"]

        # Primary may have covering metadata for bbox
        # Secondary should NOT have covering (bbox is for primary geometry)
        assert "covering" not in secondary_meta or secondary_meta.get("covering") is None

    def test_hilbert_ordering_succeeds_with_multiple_columns(self, tmp_path):
        """Hilbert ordering should work (using primary column)."""
        from geoparquet_io.core.convert import convert_to_geoparquet

        input_file = tmp_path / "input.parquet"
        output_file = tmp_path / "output.parquet"
        create_multi_geometry_geoparquet(str(input_file))

        # Convert with Hilbert ordering
        convert_to_geoparquet(
            str(input_file), str(output_file), skip_hilbert=False, geoparquet_version="1.1"
        )

        # Verify file was created and both columns present
        assert Path(output_file).exists()

        table = pq.read_table(str(output_file))
        column_names = [f.name for f in table.schema]
        assert "geometry" in column_names
        assert "boundary" in column_names


class TestMultiGeometryRoundTrip:
    """Integration tests for round-trip conversion with multiple geometry columns."""

    def test_geoparquet_to_geoparquet_roundtrip(self, tmp_path):
        """GeoParquet -> GeoParquet should preserve all geometry columns."""
        from geoparquet_io.core.convert import convert_to_geoparquet

        input_file = tmp_path / "input.parquet"
        output_file = tmp_path / "output.parquet"
        roundtrip_file = tmp_path / "roundtrip.parquet"

        create_multi_geometry_geoparquet(str(input_file))

        # First conversion
        convert_to_geoparquet(str(input_file), str(output_file), skip_hilbert=True)

        # Second conversion (round-trip)
        convert_to_geoparquet(str(output_file), str(roundtrip_file), skip_hilbert=True)

        # Verify both geometry columns preserved through both conversions
        meta = pq.read_metadata(str(roundtrip_file))
        geo_meta = json.loads(meta.metadata[b"geo"].decode("utf-8"))

        assert "geometry" in geo_meta["columns"]
        assert "boundary" in geo_meta["columns"]
        assert geo_meta["primary_column"] == "geometry"

    def test_row_count_preserved(self, tmp_path):
        """Row count should be preserved through conversion."""
        from geoparquet_io.core.convert import convert_to_geoparquet

        input_file = tmp_path / "input.parquet"
        output_file = tmp_path / "output.parquet"

        create_multi_geometry_geoparquet(str(input_file))

        input_table = pq.read_table(str(input_file))
        convert_to_geoparquet(str(input_file), str(output_file), skip_hilbert=True)
        output_table = pq.read_table(str(output_file))

        assert len(input_table) == len(output_table)

    def test_data_integrity_preserved(self, tmp_path):
        """Actual geometry data should be preserved through conversion."""
        from geoparquet_io.core.convert import convert_to_geoparquet

        input_file = tmp_path / "input.parquet"
        output_file = tmp_path / "output.parquet"

        create_multi_geometry_geoparquet(str(input_file))

        input_table = pq.read_table(str(input_file))
        convert_to_geoparquet(str(input_file), str(output_file), skip_hilbert=True)
        output_table = pq.read_table(str(output_file))

        # Compare boundary column data (secondary geometry)
        input_boundary = input_table.column("boundary").to_pylist()
        output_boundary = output_table.column("boundary").to_pylist()

        assert input_boundary == output_boundary


class TestMultiGeometryVersions:
    """Tests for multi-geometry support across GeoParquet versions."""

    def test_geoparquet_1_0_preserves_multiple_columns(self, tmp_path):
        """GeoParquet 1.0 output should preserve all geometry columns."""
        from geoparquet_io.core.convert import convert_to_geoparquet

        input_file = tmp_path / "input.parquet"
        output_file = tmp_path / "output.parquet"
        create_multi_geometry_geoparquet(str(input_file))

        convert_to_geoparquet(
            str(input_file), str(output_file), skip_hilbert=True, geoparquet_version="1.0"
        )

        meta = pq.read_metadata(str(output_file))
        geo_meta = json.loads(meta.metadata[b"geo"].decode("utf-8"))

        assert "geometry" in geo_meta["columns"]
        assert "boundary" in geo_meta["columns"]
        assert geo_meta["version"] == "1.0.0"

    def test_geoparquet_1_1_preserves_multiple_columns(self, tmp_path):
        """GeoParquet 1.1 output should preserve all geometry columns."""
        from geoparquet_io.core.convert import convert_to_geoparquet

        input_file = tmp_path / "input.parquet"
        output_file = tmp_path / "output.parquet"
        create_multi_geometry_geoparquet(str(input_file))

        convert_to_geoparquet(
            str(input_file), str(output_file), skip_hilbert=True, geoparquet_version="1.1"
        )

        meta = pq.read_metadata(str(output_file))
        geo_meta = json.loads(meta.metadata[b"geo"].decode("utf-8"))

        assert "geometry" in geo_meta["columns"]
        assert "boundary" in geo_meta["columns"]
        assert geo_meta["version"] == "1.1.0"

    def test_geoparquet_2_0_preserves_multiple_columns(self, tmp_path):
        """GeoParquet 2.0 output should preserve all geometry columns."""
        from geoparquet_io.core.convert import convert_to_geoparquet

        input_file = tmp_path / "input.parquet"
        output_file = tmp_path / "output.parquet"
        create_multi_geometry_geoparquet(str(input_file))

        convert_to_geoparquet(
            str(input_file), str(output_file), skip_hilbert=True, geoparquet_version="2.0"
        )

        meta = pq.read_metadata(str(output_file))
        geo_meta = json.loads(meta.metadata[b"geo"].decode("utf-8"))

        assert "geometry" in geo_meta["columns"]
        assert "boundary" in geo_meta["columns"]
        assert geo_meta["version"] == "2.0.0"
