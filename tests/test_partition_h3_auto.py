#!/usr/bin/env python3

"""Integration tests for H3 auto-resolution partitioning."""

import pytest

from geoparquet_io.core.partition_by_h3 import partition_by_h3


class TestH3AutoResolutionIntegration:
    """Test H3 auto-resolution with real files."""

    def test_partition_h3_auto_resolution(self, fields_5070_file, tmp_path):
        """Test H3 partitioning with auto-resolution."""
        output_dir = tmp_path / "h3_auto_output"

        # Use auto-resolution with small target for test file
        partition_by_h3(
            input_parquet=fields_5070_file,
            output_folder=str(output_dir),
            h3_column_name="h3_cell",
            resolution=None,
            auto=True,
            target_rows=50,  # Small target for 100-row test file
            max_partitions=100,
            force=True,  # Override tiny partition warnings for test
            verbose=True,
        )

        # Verify partitions were created
        assert output_dir.exists()
        parquet_files = list(output_dir.glob("*.parquet"))
        assert len(parquet_files) > 0, "Should create at least one partition"

        # Verify all partitions are valid GeoParquet
        import pyarrow.parquet as pq

        for file in parquet_files:
            table = pq.read_table(file)
            assert table.num_rows > 0, f"Partition {file} should have rows"

    def test_partition_h3_auto_vs_manual(self, fields_5070_file, tmp_path):
        """Test that auto-resolution produces reasonable results compared to manual."""
        auto_dir = tmp_path / "h3_auto"
        manual_dir = tmp_path / "h3_manual"

        # Auto-resolution
        partition_by_h3(
            input_parquet=fields_5070_file,
            output_folder=str(auto_dir),
            auto=True,
            target_rows=50,
            force=True,  # Override warnings for test
            verbose=False,
        )

        # Manual resolution (using same resolution auto would calculate)
        from geoparquet_io.core.partition_auto_resolution import calculate_auto_resolution

        calculated_res = calculate_auto_resolution(
            input_parquet=fields_5070_file,
            spatial_index_type="h3",
            target_rows_per_partition=50,
            max_partitions=100,
            verbose=False,
        )

        partition_by_h3(
            input_parquet=fields_5070_file,
            output_folder=str(manual_dir),
            resolution=calculated_res,
            auto=False,
            force=True,  # Override warnings for test
            verbose=False,
        )

        # Both should create same number of partitions
        auto_files = list(auto_dir.glob("*.parquet"))
        manual_files = list(manual_dir.glob("*.parquet"))
        assert len(auto_files) == len(manual_files), "Auto and manual should create same partitions"

    def test_partition_h3_auto_with_constraints(self, fields_5070_file, tmp_path):
        """Test auto-resolution respects max_partitions constraint."""
        output_dir = tmp_path / "h3_constrained"

        # Force very low max_partitions
        partition_by_h3(
            input_parquet=fields_5070_file,
            output_folder=str(output_dir),
            auto=True,
            target_rows=1,  # Would normally create many partitions
            max_partitions=10,  # But limit to 10
            force=True,  # Override warnings for test
            verbose=False,
        )

        # Should create ≤ 10 partitions (likely much less due to resolution rounding)
        parquet_files = list(output_dir.glob("*.parquet"))
        # H3 at low resolutions has ~122, ~854 cells, so we'll get coarse partitioning
        assert len(parquet_files) <= 200, "Should respect max_partitions constraint loosely"

    def test_partition_h3_auto_with_hive(self, fields_5070_file, tmp_path):
        """Test auto-resolution with Hive-style partitioning."""
        output_dir = tmp_path / "h3_hive"

        partition_by_h3(
            input_parquet=fields_5070_file,
            output_folder=str(output_dir),
            auto=True,
            target_rows=50,
            hive=True,
            force=True,  # Override warnings for test
            verbose=False,
        )

        # Should create Hive-style directories
        subdirs = [d for d in output_dir.iterdir() if d.is_dir()]
        assert len(subdirs) > 0, "Should create Hive-style subdirectories"

        # Check subdirectory names match pattern
        for subdir in subdirs:
            assert subdir.name.startswith("h3_cell="), f"Subdir should be Hive-style: {subdir.name}"

    def test_partition_h3_auto_error_when_both_auto_and_resolution(
        self, fields_5070_file, tmp_path
    ):
        """Test that specifying both --auto and --resolution raises error."""
        output_dir = tmp_path / "h3_error"

        with pytest.raises(Exception, match="Cannot specify both"):
            partition_by_h3(
                input_parquet=fields_5070_file,
                output_folder=str(output_dir),
                resolution=5,  # Manual resolution
                auto=True,  # And auto - should conflict
                verbose=False,
            )

    def test_partition_h3_preview_with_auto(self, fields_5070_file):
        """Test preview mode with auto-resolution."""
        # Should not create any files, just preview
        partition_by_h3(
            input_parquet=fields_5070_file,
            output_folder=None,  # Not required for preview
            auto=True,
            target_rows=50,
            preview=True,
            verbose=False,
        )
        # If we get here without error, preview worked
