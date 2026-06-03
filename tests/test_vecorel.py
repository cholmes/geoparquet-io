"""Tests for Vecorel integration (admin column naming and metadata)."""

import json

from geoparquet_io.core.admin_datasets import (
    CurrentAdminDataset,
    GAULAdminDataset,
    OvertureAdminDataset,
)
from geoparquet_io.core.constants import (
    FIBOA_CORE_SCHEMA,
    VECOREL_ADMIN_SCHEMA,
    VECOREL_CORE_SCHEMA,
    VECOREL_METRICS_SCHEMA,
)


class TestVecorelColumnNaming:
    """Test Vecorel-compliant column name generation."""

    def test_overture_vecorel_country(self):
        dataset = OvertureAdminDataset()
        name = dataset.get_output_column_name("country", prefix="vecorel")
        assert name == "admin:country_code"

    def test_overture_vecorel_region(self):
        dataset = OvertureAdminDataset()
        name = dataset.get_output_column_name("region", prefix="vecorel")
        assert name == "admin:subdivision_code"

    def test_overture_vecorel_unknown_level(self):
        dataset = OvertureAdminDataset()
        name = dataset.get_output_column_name("locality", prefix="vecorel")
        assert name == "admin:locality"

    def test_gaul_vecorel_falls_back_to_base(self):
        dataset = GAULAdminDataset()
        name = dataset.get_output_column_name("country", prefix="vecorel")
        assert name == "admin:country"

    def test_current_vecorel_falls_back_to_base(self):
        dataset = CurrentAdminDataset()
        name = dataset.get_output_column_name("country", prefix="vecorel")
        assert name == "admin:country"


class TestGetOutputColumnNamePrefixes:
    """Test all prefix modes still work correctly."""

    def test_none_prefix_uses_default(self):
        dataset = OvertureAdminDataset()
        name = dataset.get_output_column_name("country", prefix=None)
        assert name == "overture_country"

    def test_admin_prefix_uses_colon(self):
        dataset = OvertureAdminDataset()
        name = dataset.get_output_column_name("country", prefix="admin")
        assert name == "admin:country"

    def test_custom_prefix_uses_underscore(self):
        dataset = OvertureAdminDataset()
        name = dataset.get_output_column_name("country", prefix="source1")
        assert name == "source1_country"

    def test_vecorel_prefix_uses_mapping(self):
        dataset = OvertureAdminDataset()
        name = dataset.get_output_column_name("country", prefix="vecorel")
        assert name == "admin:country_code"


class TestVecorelMetadataConstants:
    """Test that Vecorel constants are correct URLs."""

    def test_core_schema_url(self):
        assert "vecorel.org/specification" in VECOREL_CORE_SCHEMA
        assert VECOREL_CORE_SCHEMA.endswith(".yaml")

    def test_admin_schema_url(self):
        assert "administrative-division-extension" in VECOREL_ADMIN_SCHEMA
        assert VECOREL_ADMIN_SCHEMA.endswith(".yaml")

    def test_metrics_schema_url(self):
        assert "geometry-metrics-extension" in VECOREL_METRICS_SCHEMA
        assert VECOREL_METRICS_SCHEMA.endswith(".yaml")

    def test_fiboa_schema_url(self):
        assert "fiboa.org/specification" in FIBOA_CORE_SCHEMA
        assert FIBOA_CORE_SCHEMA.endswith(".yaml")


class TestVecorelMetadataPreservation:
    """Test that Vecorel metadata survives gpio operations."""

    def test_bbox_add_preserves_vecorel(self, buildings_test_file, temp_output_file):
        """Adding bbox to a file with vecorel metadata should preserve it."""
        import os
        import tempfile

        import pyarrow.parquet as pq
        from click.testing import CliRunner

        from geoparquet_io.cli.main import add
        from geoparquet_io.core.add.geometry_metrics import add_geometry_metrics

        # Step 1: add geometry metrics (writes vecorel metadata)
        fd, intermediate = tempfile.mkstemp(suffix=".parquet")
        os.close(fd)
        os.unlink(intermediate)
        try:
            add_geometry_metrics(buildings_test_file, intermediate, vecorel=True)

            # Step 2: add bbox (should preserve vecorel metadata)
            runner = CliRunner()
            result = runner.invoke(add, ["bbox", intermediate, temp_output_file])
            assert result.exit_code == 0

            pf = pq.ParquetFile(temp_output_file)
            meta = pf.schema_arrow.metadata
            assert b"collection" in meta
            vecorel = json.loads(meta[b"collection"])
            assert VECOREL_METRICS_SCHEMA in vecorel["schemas"]["default"]
        finally:
            if os.path.exists(intermediate):
                os.unlink(intermediate)


class TestExtraKvMetadata:
    """Test extra_kv_metadata plumbing through write pipeline."""

    def test_build_copy_options_with_extra_kv(self):
        from geoparquet_io.core.write_strategies.duckdb_kv import _build_copy_options

        options = _build_copy_options(
            compression="ZSTD",
            row_group_rows=None,
            geo_meta_escaped='{"version": "1.1"}',
            extra_kv_metadata={"vecorel": '{"schemas": {}}'},
        )
        kv_option = [o for o in options if "KV_METADATA" in o]
        assert len(kv_option) == 1
        assert "'geo':" in kv_option[0]
        assert "'vecorel':" in kv_option[0]

    def test_build_copy_options_only_extra_kv(self):
        from geoparquet_io.core.write_strategies.duckdb_kv import _build_copy_options

        options = _build_copy_options(
            compression="ZSTD",
            row_group_rows=None,
            geo_meta_escaped=None,
            extra_kv_metadata={"vecorel": '{"schemas": {}}'},
        )
        kv_option = [o for o in options if "KV_METADATA" in o]
        assert len(kv_option) == 1
        assert "'vecorel':" in kv_option[0]
        assert "'geo':" not in kv_option[0]

    def test_build_copy_options_no_kv(self):
        from geoparquet_io.core.write_strategies.duckdb_kv import _build_copy_options

        options = _build_copy_options(
            compression="ZSTD",
            row_group_rows=None,
        )
        kv_option = [o for o in options if "KV_METADATA" in o]
        assert len(kv_option) == 0
