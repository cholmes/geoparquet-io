"""
Tests for admin dataset abstraction layer.
"""

import os
import tempfile
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from geoparquet_io.core.admin_datasets import (
    AdminDatasetFactory,
    CurrentAdminDataset,
    GAULAdminDataset,
    OvertureAdminDataset,
    check_cache_age,
    clear_cache,
    get_cache_dir,
    get_cached_path,
    get_or_cache_dataset,
)


class TestCurrentAdminDataset:
    """Test CurrentAdminDataset implementation."""

    def test_get_dataset_name(self):
        dataset = CurrentAdminDataset()
        assert dataset.get_dataset_name() == "Current (source.coop countries)"

    def test_get_default_source(self):
        dataset = CurrentAdminDataset()
        source = dataset.get_default_source()
        assert source.startswith("https://data.source.coop/")
        assert "countries.parquet" in source

    def test_get_available_levels(self):
        dataset = CurrentAdminDataset()
        levels = dataset.get_available_levels()
        assert levels == ["country"]

    def test_get_level_column_mapping(self):
        dataset = CurrentAdminDataset()
        mapping = dataset.get_level_column_mapping()
        assert mapping == {"country": "country"}

    def test_get_geometry_column(self):
        dataset = CurrentAdminDataset()
        assert dataset.get_geometry_column() == "geometry"

    def test_get_bbox_column(self):
        dataset = CurrentAdminDataset()
        assert dataset.get_bbox_column() == "bbox"

    def test_is_remote_with_default(self):
        dataset = CurrentAdminDataset()
        assert dataset.is_remote() is True

    def test_is_remote_with_local_path(self):
        dataset = CurrentAdminDataset(source_path="/local/path/data.parquet")
        assert dataset.is_remote() is False

    def test_get_partition_columns(self):
        dataset = CurrentAdminDataset()
        columns = dataset.get_partition_columns(["country"])
        assert columns == ["country"]

    def test_validate_levels_valid(self):
        dataset = CurrentAdminDataset()
        # Should not raise
        dataset.validate_levels(["country"])

    def test_validate_levels_invalid(self):
        from geoparquet_io.core.exceptions import InvalidParameterError

        dataset = CurrentAdminDataset()
        with pytest.raises(InvalidParameterError) as exc_info:
            dataset.validate_levels(["continent"])
        assert "invalid" in str(exc_info.value).lower()
        assert "continent" in str(exc_info.value)


class TestGAULAdminDataset:
    """Test GAULAdminDataset implementation."""

    def test_get_dataset_name(self):
        dataset = GAULAdminDataset()
        assert dataset.get_dataset_name() == "GAUL L2 Admin Boundaries"

    def test_get_default_source(self):
        dataset = GAULAdminDataset()
        source = dataset.get_default_source()
        assert "gaul-l2-admin" in source.lower()
        assert source.endswith("*.parquet")

    def test_get_available_levels(self):
        dataset = GAULAdminDataset()
        levels = dataset.get_available_levels()
        assert levels == ["continent", "country", "department"]

    def test_get_level_column_mapping(self):
        dataset = GAULAdminDataset()
        mapping = dataset.get_level_column_mapping()
        assert mapping["continent"] == "continent"
        assert mapping["country"] == "gaul0_name"
        assert mapping["department"] == "gaul2_name"

    def test_get_geometry_column(self):
        dataset = GAULAdminDataset()
        assert dataset.get_geometry_column() == "geometry"

    def test_get_bbox_column(self):
        dataset = GAULAdminDataset()
        assert dataset.get_bbox_column() == "geometry_bbox"

    def test_get_partition_columns_single_level(self):
        dataset = GAULAdminDataset()
        columns = dataset.get_partition_columns(["continent"])
        assert columns == ["continent"]

    def test_get_partition_columns_multi_level(self):
        dataset = GAULAdminDataset()
        columns = dataset.get_partition_columns(["continent", "country", "department"])
        assert columns == ["continent", "gaul0_name", "gaul2_name"]

    def test_validate_levels_all_valid(self):
        dataset = GAULAdminDataset()
        # Should not raise
        dataset.validate_levels(["continent", "country", "department"])

    def test_validate_levels_partial_valid(self):
        dataset = GAULAdminDataset()
        # Should not raise
        dataset.validate_levels(["continent", "country"])

    def test_validate_levels_invalid(self):
        from geoparquet_io.core.exceptions import InvalidParameterError

        dataset = GAULAdminDataset()
        with pytest.raises(InvalidParameterError) as exc_info:
            dataset.validate_levels(["region"])
        assert "invalid" in str(exc_info.value).lower()


class TestOvertureAdminDataset:
    """Test OvertureAdminDataset implementation."""

    def test_get_dataset_name(self):
        dataset = OvertureAdminDataset()
        assert dataset.get_dataset_name() == "Overture Maps Divisions"

    def test_get_default_source(self):
        dataset = OvertureAdminDataset()
        source = dataset.get_default_source()
        assert source.startswith("s3://")
        assert "overturemaps" in source.lower()
        assert "divisions" in source.lower()

    def test_get_available_levels(self):
        dataset = OvertureAdminDataset()
        levels = dataset.get_available_levels()
        # At minimum should have some levels defined
        assert len(levels) > 0
        assert isinstance(levels, list)

    def test_get_geometry_column(self):
        dataset = OvertureAdminDataset()
        assert dataset.get_geometry_column() == "geometry"

    def test_get_bbox_column(self):
        dataset = OvertureAdminDataset()
        assert dataset.get_bbox_column() == "bbox"

    def test_get_read_parquet_options(self):
        dataset = OvertureAdminDataset()
        options = dataset.get_read_parquet_options()
        assert "hive_partitioning" in options
        assert options["hive_partitioning"] == 1

    def test_get_subtype_filter(self):
        dataset = OvertureAdminDataset()
        # Test with country level
        filter_country = dataset.get_subtype_filter(["country"])
        assert filter_country == "subtype IN ('country')"

        # Test with both levels
        filter_both = dataset.get_subtype_filter(["country", "region"])
        assert "country" in filter_both
        assert "region" in filter_both
        assert "subtype IN" in filter_both

    def test_get_column_transform_region(self):
        """Test that region level returns SQL transformation for Vecorel compliance."""
        dataset = OvertureAdminDataset()
        transform = dataset.get_column_transform("region")

        # Should return transformation SQL to strip country prefix
        assert transform is not None
        assert "CASE WHEN region LIKE '%-%'" in transform
        assert "split_part(region, '-', 2)" in transform
        assert "ELSE region END" in transform

    def test_get_column_transform_country(self):
        """Test that country level returns None (no transformation needed)."""
        dataset = OvertureAdminDataset()
        transform = dataset.get_column_transform("country")

        # Country doesn't need transformation
        assert transform is None

    def test_get_column_transform_unknown_level(self):
        """Test that unknown levels return None."""
        dataset = OvertureAdminDataset()
        transform = dataset.get_column_transform("unknown_level")

        assert transform is None

    def test_get_output_column_name_country(self):
        """Test default country column name (dataset prefix)."""
        dataset = OvertureAdminDataset()
        col_name = dataset.get_output_column_name("country")

        # Default prefix should be "overture"
        assert col_name == "overture_country"

    def test_get_output_column_name_region(self):
        """Test default region column name (dataset prefix)."""
        dataset = OvertureAdminDataset()
        col_name = dataset.get_output_column_name("region")

        # Default prefix should be "overture"
        assert col_name == "overture_region"

    def test_get_output_column_name_fallback(self):
        """Test fallback to default pattern for unknown levels."""
        dataset = OvertureAdminDataset()
        col_name = dataset.get_output_column_name("unknown_level")

        # Should use dataset prefix
        assert col_name == "overture_unknown_level"


class TestBaseAdminDatasetDefaults:
    """Test base AdminDataset default implementations for backwards compatibility."""

    def test_get_column_transform_default(self):
        """Test that base class returns None by default (no transformation)."""
        # Use CurrentAdminDataset as a concrete implementation
        dataset = CurrentAdminDataset()
        transform = dataset.get_column_transform("country")

        # Default implementation should return None
        assert transform is None

    def test_get_output_column_name_default(self):
        """Test that base class uses dataset prefix by default."""
        # Use CurrentAdminDataset as a concrete implementation
        dataset = CurrentAdminDataset()
        col_name = dataset.get_output_column_name("country")

        # Default implementation should use dataset prefix
        assert col_name == "current_country"

    def test_gaul_uses_default_column_names(self):
        """Test that GAUL dataset uses dataset prefix by default."""
        dataset = GAULAdminDataset()

        # GAUL should use dataset prefix
        assert dataset.get_output_column_name("continent") == "gaul_continent"
        assert dataset.get_output_column_name("country") == "gaul_country"
        assert dataset.get_output_column_name("department") == "gaul_department"

    def test_gaul_no_column_transforms(self):
        """Test that GAUL dataset has no column transformations."""
        dataset = GAULAdminDataset()

        # GAUL should not transform columns
        assert dataset.get_column_transform("continent") is None
        assert dataset.get_column_transform("country") is None
        assert dataset.get_column_transform("department") is None


class TestAdminDatasetFactory:
    """Test AdminDatasetFactory."""

    def test_get_available_datasets(self):
        datasets = AdminDatasetFactory.get_available_datasets()
        assert "current" in datasets
        assert "gaul" in datasets
        assert "overture" in datasets

    def test_create_current_dataset(self):
        dataset = AdminDatasetFactory.create("current")
        assert isinstance(dataset, CurrentAdminDataset)

    def test_create_gaul_dataset(self):
        dataset = AdminDatasetFactory.create("gaul")
        assert isinstance(dataset, GAULAdminDataset)

    def test_create_overture_dataset(self):
        dataset = AdminDatasetFactory.create("overture")
        assert isinstance(dataset, OvertureAdminDataset)

    def test_create_with_custom_source(self):
        dataset = AdminDatasetFactory.create("current", source_path="/custom/path.parquet")
        assert dataset.source_path == "/custom/path.parquet"
        assert dataset.get_source() == "/custom/path.parquet"

    def test_create_with_verbose(self):
        dataset = AdminDatasetFactory.create("current", verbose=True)
        assert dataset.verbose is True

    def test_create_invalid_dataset(self):
        from geoparquet_io.core.exceptions import InvalidParameterError

        with pytest.raises(InvalidParameterError) as exc_info:
            AdminDatasetFactory.create("invalid_dataset")
        assert "unknown" in str(exc_info.value).lower()
        assert "invalid_dataset" in str(exc_info.value)


class TestAdminDatasetIntegration:
    """Integration tests for admin datasets."""

    def test_current_dataset_full_workflow(self):
        """Test typical workflow with current dataset."""
        dataset = AdminDatasetFactory.create("current")

        # Validate levels
        dataset.validate_levels(["country"])

        # Get partition columns
        columns = dataset.get_partition_columns(["country"])
        assert columns == ["country"]

        # Check remote status
        assert dataset.is_remote() is True

    def test_gaul_dataset_hierarchical_workflow(self):
        """Test hierarchical workflow with GAUL dataset."""
        dataset = AdminDatasetFactory.create("gaul")

        # Test single level
        dataset.validate_levels(["continent"])
        columns = dataset.get_partition_columns(["continent"])
        assert columns == ["continent"]

        # Test two levels
        dataset.validate_levels(["continent", "country"])
        columns = dataset.get_partition_columns(["continent", "country"])
        assert columns == ["continent", "gaul0_name"]

        # Test all three levels
        dataset.validate_levels(["continent", "country", "department"])
        columns = dataset.get_partition_columns(["continent", "country", "department"])
        assert columns == ["continent", "gaul0_name", "gaul2_name"]

    def test_custom_source_override(self):
        """Test using custom source instead of default."""
        custom_path = "/my/custom/gaul.parquet"
        dataset = AdminDatasetFactory.create("gaul", source_path=custom_path)

        assert dataset.get_source() == custom_path
        assert dataset.is_remote() is False

    def test_remote_url_detection(self):
        """Test detection of remote vs local sources."""
        # HTTP URL
        dataset = AdminDatasetFactory.create("gaul", source_path="http://example.com/data.parquet")
        assert dataset.is_remote() is True

        # HTTPS URL
        dataset = AdminDatasetFactory.create("gaul", source_path="https://example.com/data.parquet")
        assert dataset.is_remote() is True

        # S3 URL
        dataset = AdminDatasetFactory.create("gaul", source_path="s3://bucket/data.parquet")
        assert dataset.is_remote() is True

        # Local path
        dataset = AdminDatasetFactory.create("gaul", source_path="/local/path/data.parquet")
        assert dataset.is_remote() is False


class TestAdminDatasetPrefixFunctionality:
    """Test prefix functionality for column naming."""

    def test_get_default_prefix_gaul(self):
        """Test that GAUL dataset returns 'gaul' as default prefix."""
        dataset = GAULAdminDataset()
        prefix = dataset.get_default_prefix()
        assert prefix == "gaul"

    def test_get_default_prefix_overture(self):
        """Test that Overture dataset returns 'overture' as default prefix."""
        dataset = OvertureAdminDataset()
        prefix = dataset.get_default_prefix()
        assert prefix == "overture"

    def test_get_default_prefix_current(self):
        """Test that Current dataset returns 'current' as default prefix."""
        dataset = CurrentAdminDataset()
        prefix = dataset.get_default_prefix()
        assert prefix == "current"

    def test_get_output_column_name_with_default_prefix_gaul(self):
        """Test GAUL column naming with default prefix (None)."""
        dataset = GAULAdminDataset()

        # With no prefix specified, should use default prefix (gaul)
        assert dataset.get_output_column_name("continent", prefix=None) == "gaul_continent"
        assert dataset.get_output_column_name("country", prefix=None) == "gaul_country"
        assert dataset.get_output_column_name("department", prefix=None) == "gaul_department"

    def test_get_output_column_name_with_default_prefix_overture(self):
        """Test Overture column naming with default prefix (None)."""
        dataset = OvertureAdminDataset()

        # With no prefix specified, should use default prefix (overture)
        assert dataset.get_output_column_name("country", prefix=None) == "overture_country"
        assert dataset.get_output_column_name("region", prefix=None) == "overture_region"

    def test_get_output_column_name_with_admin_prefix(self):
        """Test column naming with admin prefix (colon format)."""
        dataset = GAULAdminDataset()

        # With admin prefix, should use colon format
        assert dataset.get_output_column_name("continent", prefix="admin") == "admin:continent"
        assert dataset.get_output_column_name("country", prefix="admin") == "admin:country"
        assert dataset.get_output_column_name("department", prefix="admin") == "admin:department"

    def test_get_output_column_name_with_custom_prefix(self):
        """Test column naming with custom prefix."""
        dataset = GAULAdminDataset()

        # With custom prefix, should use underscore format
        assert (
            dataset.get_output_column_name("continent", prefix="mycustom") == "mycustom_continent"
        )
        assert dataset.get_output_column_name("country", prefix="mycustom") == "mycustom_country"
        assert (
            dataset.get_output_column_name("department", prefix="mycustom") == "mycustom_department"
        )

    def test_prefix_prevents_duplicate_columns(self):
        """Test that different prefixes create unique column names."""
        gaul_dataset = GAULAdminDataset()
        overture_dataset = OvertureAdminDataset()

        # Same level, different datasets, default prefixes
        gaul_country = gaul_dataset.get_output_column_name("country", prefix=None)
        overture_country = overture_dataset.get_output_column_name("country", prefix=None)

        # Should be different
        assert gaul_country == "gaul_country"
        assert overture_country == "overture_country"
        assert gaul_country != overture_country

    def test_multiple_custom_prefixes(self):
        """Test using multiple custom prefixes on same dataset."""
        dataset = GAULAdminDataset()

        # Different prefixes should create different column names
        prefix1_col = dataset.get_output_column_name("country", prefix="source1")
        prefix2_col = dataset.get_output_column_name("country", prefix="source2")

        assert prefix1_col == "source1_country"
        assert prefix2_col == "source2_country"
        assert prefix1_col != prefix2_col


# =============================================================================
# CACHING TESTS - Issue #43
# =============================================================================


class TestAdminDatasetVersion:
    """Test VERSION attribute and get_version() method on datasets."""

    def test_gaul_dataset_has_version(self):
        """Test that GAUL dataset has a VERSION class attribute."""
        assert hasattr(GAULAdminDataset, "VERSION")
        assert isinstance(GAULAdminDataset.VERSION, str)
        # Version should be in date format like "2024-12-19"
        assert len(GAULAdminDataset.VERSION.split("-")) >= 2

    def test_overture_dataset_has_version(self):
        """Test that Overture dataset has a VERSION class attribute."""
        assert hasattr(OvertureAdminDataset, "VERSION")
        assert isinstance(OvertureAdminDataset.VERSION, str)
        # VERSION is the fallback release; should match date format like "2026-05-20.0"
        assert "." in OvertureAdminDataset.VERSION and "-" in OvertureAdminDataset.VERSION

    def test_gaul_get_version(self):
        """Test get_version() method on GAUL dataset."""
        dataset = GAULAdminDataset()
        version = dataset.get_version()
        assert version == GAULAdminDataset.VERSION

    def test_overture_get_version(self):
        """Test get_version() method on Overture dataset resolves dynamically."""
        from unittest.mock import patch

        import geoparquet_io.core.overture as overture_mod

        original_cached = overture_mod._cached_release
        try:
            dataset = OvertureAdminDataset()
            with patch("geoparquet_io.core.overture._fetch_latest_release") as mock:
                mock.return_value = "2099-01-01.0"
                overture_mod._cached_release = None
                version = dataset.get_version()
                assert version == "2099-01-01.0"
        finally:
            overture_mod._cached_release = original_cached

    def test_overture_get_version_rejects_malformed(self):
        """Test get_version() falls back when release format is malformed."""
        from unittest.mock import patch

        import geoparquet_io.core.overture as overture_mod

        original_cached = overture_mod._cached_release
        try:
            dataset = OvertureAdminDataset()
            with patch("geoparquet_io.core.overture._fetch_latest_release") as mock:
                mock.return_value = "malicious-input; DROP TABLE"
                overture_mod._cached_release = None
                version = dataset.get_version()
                assert version == OvertureAdminDataset.VERSION
        finally:
            overture_mod._cached_release = original_cached

    def test_current_dataset_has_version(self):
        """Test that Current dataset has a VERSION attribute."""
        assert hasattr(CurrentAdminDataset, "VERSION")
        dataset = CurrentAdminDataset()
        version = dataset.get_version()
        assert isinstance(version, str)


class TestGetCacheDir:
    """Test get_cache_dir() function."""

    def test_returns_path_object(self):
        """Test that get_cache_dir returns a Path object."""
        cache_dir = get_cache_dir()
        assert isinstance(cache_dir, Path)

    def test_cache_dir_in_user_home(self):
        """Test that cache directory is in user's home directory."""
        cache_dir = get_cache_dir()
        home = Path.home()
        assert str(cache_dir).startswith(str(home))

    def test_cache_dir_path_structure(self):
        """Test expected cache directory path structure."""
        cache_dir = get_cache_dir()
        # Should be ~/.geoparquet-io/cache/admin/
        assert cache_dir.parts[-1] == "admin"
        assert cache_dir.parts[-2] == "cache"
        assert cache_dir.parts[-3] == ".geoparquet-io"

    def test_cache_dir_respects_xdg_cache_home(self):
        """Test that XDG_CACHE_HOME is respected if set."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.dict(os.environ, {"XDG_CACHE_HOME": tmpdir}):
                # Note: This tests if we decide to support XDG - for now we use ~/.geoparquet-io
                cache_dir = get_cache_dir()
                # Default implementation uses ~/.geoparquet-io regardless of XDG
                # This test documents the current behavior
                assert ".geoparquet-io" in str(cache_dir)


class TestGetCachedPath:
    """Test get_cached_path() function for generating cache file paths."""

    def test_gaul_cached_path(self):
        """Test cached path for GAUL dataset."""
        dataset = GAULAdminDataset()
        cached_path = get_cached_path(dataset)

        assert isinstance(cached_path, Path)
        assert cached_path.suffix == ".parquet"
        assert "gaul" in cached_path.name.lower()
        assert dataset.get_version() in cached_path.name

    def test_overture_cached_path(self):
        """Test cached path for Overture dataset."""
        dataset = OvertureAdminDataset()
        cached_path = get_cached_path(dataset)

        assert isinstance(cached_path, Path)
        assert cached_path.suffix == ".parquet"
        assert "overture" in cached_path.name.lower()
        assert dataset.get_version() in cached_path.name

    def test_cached_path_format(self):
        """Test that cached path follows the expected format: {dataset}-{version}.parquet"""
        dataset = GAULAdminDataset()
        cached_path = get_cached_path(dataset)

        # Path should be in format: gaul-{version}.parquet
        expected_name = f"gaul-{dataset.get_version()}.parquet"
        assert cached_path.name == expected_name

    def test_cached_path_is_in_cache_dir(self):
        """Test that cached path is inside the cache directory."""
        dataset = GAULAdminDataset()
        cached_path = get_cached_path(dataset)
        cache_dir = get_cache_dir()

        assert cached_path.parent == cache_dir


class TestCheckCacheAge:
    """Test check_cache_age() function for age warnings."""

    def test_new_cache_no_warning(self):
        """Test that new cache files don't trigger warning."""
        # Create temp file and close it immediately (Windows compatibility)
        f = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        cache_file = Path(f.name)
        f.close()
        try:
            # File was just created, should be recent
            warning = check_cache_age(cache_file)
            assert warning is None
        finally:
            cache_file.unlink()

    def test_old_cache_triggers_warning(self):
        """Test that cache files older than 6 months trigger warning."""
        # Create temp file and close it immediately (Windows compatibility)
        f = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        cache_file = Path(f.name)
        f.close()
        try:
            # Set file modification time to 7 months ago
            seven_months_ago = time.time() - (7 * 30 * 24 * 60 * 60)
            os.utime(cache_file, (seven_months_ago, seven_months_ago))

            warning = check_cache_age(cache_file)
            assert warning is not None
            assert "6 months" in warning or "old" in warning.lower()
        finally:
            cache_file.unlink()

    def test_six_month_boundary(self):
        """Test behavior at exactly 6 months."""
        # Create temp file and close it immediately (Windows compatibility)
        f = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        cache_file = Path(f.name)
        f.close()
        try:
            # Set file modification time to exactly 6 months ago (should trigger)
            six_months_ago = time.time() - (6 * 30 * 24 * 60 * 60)
            os.utime(cache_file, (six_months_ago, six_months_ago))

            warning = check_cache_age(cache_file)
            assert warning is not None
        finally:
            cache_file.unlink()

    def test_nonexistent_file_returns_none(self):
        """Test that non-existent file returns None (no warning)."""
        fake_path = Path("/nonexistent/path/to/cache.parquet")
        warning = check_cache_age(fake_path)
        assert warning is None


class TestClearCache:
    """Test clear_cache() function."""

    def test_clear_cache_empty_directory(self):
        """Test clearing an empty cache directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch("geoparquet_io.core.admin_datasets.get_cache_dir") as mock_get_cache:
                mock_get_cache.return_value = Path(tmpdir)
                result = clear_cache(confirm=True)

                # Should return info about clearing
                assert result is not None
                assert result["files_deleted"] == 0
                assert result["bytes_freed"] == 0

    def test_clear_cache_with_files(self):
        """Test clearing cache with actual files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            # Create some fake cache files
            (cache_dir / "gaul-2024-12-19.parquet").write_bytes(b"fake data 1")
            (cache_dir / "overture-2025-10-22.0.parquet").write_bytes(b"fake data 2" * 100)

            with patch("geoparquet_io.core.admin_datasets.get_cache_dir") as mock_get_cache:
                mock_get_cache.return_value = cache_dir
                result = clear_cache(confirm=True)

                assert result["files_deleted"] == 2
                assert result["bytes_freed"] > 0
                # Check files are actually deleted
                assert not (cache_dir / "gaul-2024-12-19.parquet").exists()
                assert not (cache_dir / "overture-2025-10-22.0.parquet").exists()

    def test_clear_cache_without_confirm_does_nothing(self):
        """Test that clear_cache without confirm does nothing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            (cache_dir / "gaul-2024-12-19.parquet").write_bytes(b"fake data")

            with patch("geoparquet_io.core.admin_datasets.get_cache_dir") as mock_get_cache:
                mock_get_cache.return_value = cache_dir
                result = clear_cache(confirm=False)

                # Should return without deleting
                assert result is None or result.get("cancelled", False)
                # File should still exist
                assert (cache_dir / "gaul-2024-12-19.parquet").exists()

    def test_clear_cache_reports_size(self):
        """Test that clear_cache reports the total size freed."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            # Create file with known size
            data = b"x" * 1024  # 1KB
            (cache_dir / "test.parquet").write_bytes(data)

            with patch("geoparquet_io.core.admin_datasets.get_cache_dir") as mock_get_cache:
                mock_get_cache.return_value = cache_dir
                result = clear_cache(confirm=True)

                assert result["bytes_freed"] == 1024

    def test_clear_cache_only_deletes_parquet_files(self):
        """Test that clear_cache only deletes .parquet files."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            (cache_dir / "gaul-2024.parquet").write_bytes(b"parquet")
            (cache_dir / "readme.txt").write_text("do not delete")
            (cache_dir / ".gitkeep").write_text("")

            with patch("geoparquet_io.core.admin_datasets.get_cache_dir") as mock_get_cache:
                mock_get_cache.return_value = cache_dir
                result = clear_cache(confirm=True)

                assert result["files_deleted"] == 1
                # Non-parquet files should remain
                assert (cache_dir / "readme.txt").exists()
                assert (cache_dir / ".gitkeep").exists()


class TestGetOrCacheDataset:
    """Test get_or_cache_dataset() function - the main caching logic."""

    def test_returns_cached_path_when_exists(self):
        """Test that existing cache is used without re-download."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            dataset = GAULAdminDataset()
            expected_path = cache_dir / f"gaul-{dataset.get_version()}.parquet"
            expected_path.write_bytes(b"cached data")

            with patch("geoparquet_io.core.admin_datasets.get_cache_dir") as mock_get_cache:
                mock_get_cache.return_value = cache_dir
                result = get_or_cache_dataset(dataset)

                # Result is a string path, expected_path is Path
                assert result == str(expected_path)
                # Content should be unchanged (not re-downloaded)
                assert expected_path.read_bytes() == b"cached data"

    def test_cache_miss_triggers_download(self):
        """Test that cache miss triggers download and caching."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            dataset = GAULAdminDataset()

            with patch("geoparquet_io.core.admin_datasets.get_cache_dir") as mock_get_cache:
                mock_get_cache.return_value = cache_dir
                with patch.object(dataset, "_download_to_cache") as mock_download:
                    expected_path = cache_dir / f"gaul-{dataset.get_version()}.parquet"
                    mock_download.return_value = expected_path

                    get_or_cache_dataset(dataset)

                    mock_download.assert_called_once()

    def test_creates_cache_directory_if_missing(self):
        """Test that cache directory is created if it doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir) / "cache" / "admin"  # Doesn't exist yet
            dataset = GAULAdminDataset()

            with patch("geoparquet_io.core.admin_datasets.get_cache_dir") as mock_get_cache:
                mock_get_cache.return_value = cache_dir
                with patch.object(dataset, "_download_to_cache") as mock_download:
                    expected_path = cache_dir / f"gaul-{dataset.get_version()}.parquet"

                    # Simulate download creating the file
                    def create_file():
                        cache_dir.mkdir(parents=True, exist_ok=True)
                        expected_path.write_bytes(b"data")
                        return expected_path

                    mock_download.side_effect = create_file

                    get_or_cache_dataset(dataset)

                    # Directory should be created
                    assert cache_dir.exists()

    def test_no_cache_flag_skips_cache(self):
        """Test that no_cache=True skips caching and returns remote URL."""
        dataset = GAULAdminDataset()

        result = get_or_cache_dataset(dataset, no_cache=True)

        # Should return the remote source URL directly
        assert result == dataset.get_default_source()

    def test_custom_source_not_cached(self):
        """Test that custom source files are not cached."""
        custom_path = "/my/custom/data.parquet"
        dataset = GAULAdminDataset(source_path=custom_path)

        result = get_or_cache_dataset(dataset)

        # Should return custom path as-is
        assert result == custom_path

    def test_local_source_not_cached(self):
        """Test that local files are not cached."""
        # Create temp file and close it immediately (Windows compatibility)
        f = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        local_path = f.name
        f.close()
        try:
            dataset = GAULAdminDataset(source_path=local_path)
            result = get_or_cache_dataset(dataset)

            # Should return local path as-is
            assert result == local_path
        finally:
            os.unlink(local_path)

    def test_age_warning_on_old_cache(self):
        """Test that old cache triggers age warning."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            dataset = GAULAdminDataset()
            cached_path = cache_dir / f"gaul-{dataset.get_version()}.parquet"
            cached_path.write_bytes(b"old cached data")

            # Set file to 7 months old
            seven_months_ago = time.time() - (7 * 30 * 24 * 60 * 60)
            os.utime(cached_path, (seven_months_ago, seven_months_ago))

            with patch("geoparquet_io.core.admin_datasets.get_cache_dir") as mock_get_cache:
                mock_get_cache.return_value = cache_dir
                with patch("geoparquet_io.core.admin_datasets.warn") as mock_warn:
                    get_or_cache_dataset(dataset)

                    # Warning should have been issued
                    mock_warn.assert_called()
                    warning_msg = str(mock_warn.call_args)
                    assert "old" in warning_msg.lower() or "month" in warning_msg.lower()


class TestCacheMessaging:
    """Test user messaging during cache operations."""

    def test_first_run_message_on_cache_miss(self):
        """Test that first-run notification is shown when caching."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            dataset = GAULAdminDataset()

            with patch("geoparquet_io.core.admin_datasets.get_cache_dir") as mock_get_cache:
                mock_get_cache.return_value = cache_dir
                with patch.object(dataset, "_download_to_cache") as mock_download:
                    expected_path = cache_dir / f"gaul-{dataset.get_version()}.parquet"
                    mock_download.return_value = expected_path

                    with patch("geoparquet_io.core.admin_datasets.info") as mock_info:
                        get_or_cache_dataset(dataset)

                        # Should show message about caching
                        mock_info.assert_called()
                        info_msg = " ".join(str(c) for c in mock_info.call_args_list)
                        assert "cache" in info_msg.lower() or "download" in info_msg.lower()

    def test_cache_hit_message(self):
        """Test that cache hit shows appropriate message."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            dataset = GAULAdminDataset()
            cached_path = cache_dir / f"gaul-{dataset.get_version()}.parquet"
            cached_path.write_bytes(b"cached data")

            with patch("geoparquet_io.core.admin_datasets.get_cache_dir") as mock_get_cache:
                mock_get_cache.return_value = cache_dir
                with patch("geoparquet_io.core.admin_datasets.debug") as mock_debug:
                    get_or_cache_dataset(dataset, verbose=True)

                    # Should show cache hit message
                    mock_debug.assert_called()


class TestCacheCLIIntegration:
    """Test CLI flag integration for caching."""

    def test_no_cache_flag_exists(self):
        """Test that --no-cache flag will be available on add admin-divisions."""
        # This test documents the expected CLI interface
        # Implementation will add the flag to main.py
        pass  # Placeholder - actual CLI test in test_cli.py

    def test_clear_cache_flag_exists(self):
        """Test that --clear-cache flag will be available on add admin-divisions."""
        # This test documents the expected CLI interface
        pass  # Placeholder - actual CLI test in test_cli.py


class TestCacheEdgeCases:
    """Test edge cases and error handling in caching."""

    def test_handles_permission_error_on_cache_dir(self):
        """Test graceful handling when cache directory cannot be created."""
        dataset = GAULAdminDataset()

        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            # Don't create the cached file - simulate cache miss

            with patch("geoparquet_io.core.admin_datasets.get_cache_dir") as mock_get_cache:
                mock_get_cache.return_value = cache_dir

                # Make mkdir raise PermissionError
                def mock_mkdir_error(self, *args, **kwargs):
                    raise PermissionError("Cannot create directory")

                with patch.object(Path, "mkdir", mock_mkdir_error):
                    # Should fall back to remote source
                    result = get_or_cache_dataset(dataset)
                    assert result == dataset.get_default_source()

    def test_handles_download_failure(self):
        """Test graceful handling when download fails."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            dataset = GAULAdminDataset()

            with patch("geoparquet_io.core.admin_datasets.get_cache_dir") as mock_get_cache:
                mock_get_cache.return_value = cache_dir
                with patch.object(dataset, "_download_to_cache") as mock_download:
                    mock_download.side_effect = Exception("Network error")

                    # Should fall back to remote source
                    result = get_or_cache_dataset(dataset)
                    assert result == dataset.get_default_source()

    def test_handles_corrupted_cache_file(self):
        """Test handling of corrupted cache file (0 bytes)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_dir = Path(tmpdir)
            dataset = GAULAdminDataset()
            cached_path = cache_dir / f"gaul-{dataset.get_version()}.parquet"
            cached_path.write_bytes(b"")  # Empty/corrupted file

            with patch("geoparquet_io.core.admin_datasets.get_cache_dir") as mock_get_cache:
                mock_get_cache.return_value = cache_dir
                with patch.object(dataset, "_download_to_cache") as mock_download:
                    fresh_path = cache_dir / f"gaul-{dataset.get_version()}.parquet"

                    def redownload():
                        fresh_path.write_bytes(b"fresh data")
                        return fresh_path

                    mock_download.side_effect = redownload

                    get_or_cache_dataset(dataset)

                    # Should re-download when cache is corrupted
                    mock_download.assert_called_once()

    def test_concurrent_cache_access(self):
        """Test that concurrent access to cache is handled safely."""
        # This is a placeholder for thread-safety testing
        # The implementation should use atomic file operations
        pass
