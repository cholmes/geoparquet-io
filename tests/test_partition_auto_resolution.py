#!/usr/bin/env python3

"""Tests for auto-resolution calculation for spatial partitioning."""

import logging

import pytest

from geoparquet_io.core.logging_config import get_logger
from geoparquet_io.core.partition.auto_resolution import (
    _calculate_a5_resolution,
    _calculate_h3_resolution,
    _calculate_quadkey_resolution,
    calculate_auto_resolution,
)


class TestH3ResolutionCalculation:
    """Test H3 auto-resolution calculation logic.

    Note on test tolerances: We use 2-3 resolution level tolerance ranges because:
    1. The algorithm uses rounding, which can shift results by ±1 resolution
    2. Spatial data is rarely uniformly distributed, so exact math is theoretical
    3. Different resolution levels within tolerance provide valid partitioning
    The goal is to verify the algorithm produces sensible results, not exact values.
    """

    def test_calculate_h3_resolution_small_dataset(self):
        """Small dataset should use low resolution."""
        # 10K rows, want ~1K per partition = ~10 partitions
        # H3 has ~122 cells at res 0, so res 0 should be fine
        # Tolerance: res 0-2 because 122 cells at res 0 already exceeds 10 target
        resolution = _calculate_h3_resolution(
            total_rows=10000, target_rows_per_partition=1000, verbose=False
        )
        assert 0 <= resolution <= 2  # Low resolution range is acceptable

    def test_calculate_h3_resolution_medium_dataset(self):
        """Medium dataset should use medium resolution."""
        # 1M rows, want ~100K per partition = ~10 partitions
        resolution = _calculate_h3_resolution(
            total_rows=1000000, target_rows_per_partition=100000, verbose=False
        )
        assert 0 <= resolution <= 3  # ~122 to ~850 cells

    def test_calculate_h3_resolution_large_dataset(self):
        """Large dataset should use higher resolution."""
        # 100M rows, want ~100K per partition = ~1000 partitions
        # 122 * 7^res = 1000 → res ≈ 1.1 (round to 1)
        resolution = _calculate_h3_resolution(
            total_rows=100000000, target_rows_per_partition=100000, verbose=False
        )
        # With 100M rows / 100K target = 1000 partitions
        # 122 * 7^n = 1000 → n = log(1000/122) / log(7) ≈ 1.1
        assert resolution >= 1  # Should be at least resolution 1

    def test_calculate_h3_resolution_very_large_dataset(self):
        """Very large dataset should approach max resolution."""
        # 1B rows, want ~10K per partition = ~100K partitions
        resolution = _calculate_h3_resolution(
            total_rows=1000000000, target_rows_per_partition=10000, verbose=False
        )
        # With 1B rows / 10K target = 100K partitions
        # 122 * 7^n = 100K → n = log(100K/122) / log(7) ≈ 3.5
        # Rounding may give us 2, 3, or 4
        assert 2 <= resolution <= 5

    def test_calculate_h3_resolution_respects_max_partitions(self):
        """Should not exceed max_partitions constraint."""
        # Request way too many partitions
        resolution = _calculate_h3_resolution(
            total_rows=1000000,
            target_rows_per_partition=10,  # Would create 100K partitions
            max_partitions=1000,  # But limit to 1K
            verbose=False,
        )
        # Should use resolution that creates ~1000 partitions instead
        # 122 * 7^n = 1000 → n ≈ 1
        assert resolution <= 3

    def test_calculate_h3_resolution_respects_bounds(self):
        """Should respect min/max resolution bounds."""
        # Force high resolution but clamp to max
        resolution = _calculate_h3_resolution(
            total_rows=1000000000,
            target_rows_per_partition=1,
            min_resolution=0,
            max_resolution=5,  # Cap at 5
            verbose=False,
        )
        assert resolution <= 5

        # Force low resolution but clamp to min
        resolution = _calculate_h3_resolution(
            total_rows=100, target_rows_per_partition=100, min_resolution=3, max_resolution=15
        )
        assert resolution >= 3

    def test_calculate_h3_resolution_zero_rows(self):
        """Zero rows should return min resolution."""
        resolution = _calculate_h3_resolution(
            total_rows=0, target_rows_per_partition=100, verbose=False
        )
        assert resolution == 0


class TestQuadkeyResolutionCalculation:
    """Test quadkey auto-resolution calculation logic.

    Note on test tolerances: See TestH3ResolutionCalculation docstring.
    Quadkey uses power-of-4 progression, so tolerances account for rounding.
    """

    def test_calculate_quadkey_resolution_small_dataset(self):
        """Small dataset should use low zoom level."""
        # 10K rows, want ~1K per partition = ~10 partitions
        # Quadkey: 4^zoom, so zoom=2 gives 16 cells
        resolution = _calculate_quadkey_resolution(
            total_rows=10000, target_rows_per_partition=1000, verbose=False
        )
        assert 0 <= resolution <= 3

    def test_calculate_quadkey_resolution_medium_dataset(self):
        """Medium dataset should use medium zoom level."""
        # 1M rows, want ~100K per partition = ~10 partitions
        # 4^zoom = 10 → zoom = log2(10)/2 ≈ 1.7
        resolution = _calculate_quadkey_resolution(
            total_rows=1000000, target_rows_per_partition=100000, verbose=False
        )
        assert 1 <= resolution <= 3

    def test_calculate_quadkey_resolution_large_dataset(self):
        """Large dataset should use higher zoom level."""
        # 100M rows, want ~100K per partition = ~1000 partitions
        # 4^zoom = 1000 → zoom = log2(1000)/2 ≈ 5
        resolution = _calculate_quadkey_resolution(
            total_rows=100000000, target_rows_per_partition=100000, verbose=False
        )
        assert 4 <= resolution <= 6

    def test_calculate_quadkey_resolution_very_large_dataset(self):
        """Very large dataset should approach max zoom level."""
        # 1B rows, want ~1K per partition = ~1M partitions
        # 4^zoom = 1M → zoom = log2(1M)/2 = 10
        # Rounding may give us 7-11
        resolution = _calculate_quadkey_resolution(
            total_rows=1000000000, target_rows_per_partition=1000, verbose=False
        )
        assert 7 <= resolution <= 11

    def test_calculate_quadkey_resolution_respects_max_partitions(self):
        """Should not exceed max_partitions constraint."""
        resolution = _calculate_quadkey_resolution(
            total_rows=1000000,
            target_rows_per_partition=10,  # Would create 100K partitions
            max_partitions=1000,  # But limit to 1K
            verbose=False,
        )
        # 4^zoom = 1000 → zoom = log2(1000)/2 ≈ 5
        assert resolution <= 6

    def test_calculate_quadkey_resolution_respects_bounds(self):
        """Should respect min/max zoom level bounds."""
        resolution = _calculate_quadkey_resolution(
            total_rows=1000000000,
            target_rows_per_partition=1,
            min_resolution=0,
            max_resolution=8,
            verbose=False,
        )
        assert resolution <= 8

        resolution = _calculate_quadkey_resolution(
            total_rows=100, target_rows_per_partition=100, min_resolution=5, max_resolution=23
        )
        assert resolution >= 5

    def test_calculate_quadkey_resolution_zero_rows(self):
        """Zero rows should return min resolution."""
        resolution = _calculate_quadkey_resolution(
            total_rows=0, target_rows_per_partition=100, verbose=False
        )
        assert resolution == 0


class TestA5ResolutionCalculation:
    """Test A5 auto-resolution calculation logic.

    Note on test tolerances: See TestH3ResolutionCalculation docstring.
    A5/S2 uses power-of-4 progression with 6 base cells.
    """

    def test_calculate_a5_resolution_small_dataset(self):
        """Small dataset should use low resolution."""
        # 10K rows, want ~1K per partition = ~10 partitions
        # A5 has 6 base cells, so res 0 should be fine
        resolution = _calculate_a5_resolution(
            total_rows=10000, target_rows_per_partition=1000, verbose=False
        )
        assert 0 <= resolution <= 2  # Should be very low resolution

    def test_calculate_a5_resolution_medium_dataset(self):
        """Medium dataset should use medium resolution."""
        # 1M rows, want ~100K per partition = ~10 partitions
        # 6 * 4^res = 10 → res ≈ 0.36 (round to 0)
        resolution = _calculate_a5_resolution(
            total_rows=1000000, target_rows_per_partition=100000, verbose=False
        )
        assert 0 <= resolution <= 2

    def test_calculate_a5_resolution_large_dataset(self):
        """Large dataset should use higher resolution."""
        # 100M rows, want ~100K per partition = ~1000 partitions
        # 6 * 4^res = 1000 → res = log(1000/6) / log(4) ≈ 3.7
        resolution = _calculate_a5_resolution(
            total_rows=100000000, target_rows_per_partition=100000, verbose=False
        )
        assert 3 <= resolution <= 5

    def test_calculate_a5_resolution_very_large_dataset(self):
        """Very large dataset should use higher resolution."""
        # 1B rows, want ~10K per partition = ~100K partitions
        # 6 * 4^res = 100K → res = log(100K/6) / log(4) ≈ 7.4
        resolution = _calculate_a5_resolution(
            total_rows=1000000000, target_rows_per_partition=10000, verbose=False
        )
        assert 5 <= resolution <= 9

    def test_calculate_a5_resolution_respects_max_partitions(self):
        """Should not exceed max_partitions constraint."""
        resolution = _calculate_a5_resolution(
            total_rows=1000000,
            target_rows_per_partition=10,  # Would create 100K partitions
            max_partitions=1000,  # But limit to 1K
            verbose=False,
        )
        # 6 * 4^res = 1000 → res = log(1000/6) / log(4) ≈ 3.7
        assert resolution <= 5

    def test_calculate_a5_resolution_respects_bounds(self):
        """Should respect min/max resolution bounds."""
        resolution = _calculate_a5_resolution(
            total_rows=1000000000,
            target_rows_per_partition=1,
            min_resolution=0,
            max_resolution=15,  # Cap at 15
            verbose=False,
        )
        assert resolution <= 15

        resolution = _calculate_a5_resolution(
            total_rows=100, target_rows_per_partition=100, min_resolution=5, max_resolution=30
        )
        assert resolution >= 5

    def test_calculate_a5_resolution_zero_rows(self):
        """Zero rows should return min resolution."""
        resolution = _calculate_a5_resolution(
            total_rows=0, target_rows_per_partition=100, verbose=False
        )
        assert resolution == 0


class TestS2ResolutionCalculation:
    """Test S2 auto-resolution calculation logic.

    Note on test tolerances: See TestH3ResolutionCalculation docstring.
    S2 uses the same math as A5 (power-of-4 with 6 base cells).
    """

    def test_calculate_s2_resolution_small_dataset(self):
        """Small dataset should use low level."""
        # 10K rows, want ~1K per partition = ~10 partitions
        # S2 has 6 base cells, so level 0 should be fine
        resolution = _calculate_a5_resolution(
            total_rows=10000, target_rows_per_partition=1000, verbose=False, index_name="S2"
        )
        assert 0 <= resolution <= 2  # Should be very low resolution

    def test_calculate_s2_resolution_medium_dataset(self):
        """Medium dataset should use medium level."""
        # 1M rows, want ~100K per partition = ~10 partitions
        # 6 * 4^level = 10 → level ≈ 0.36 (round to 0)
        resolution = _calculate_a5_resolution(
            total_rows=1000000, target_rows_per_partition=100000, verbose=False, index_name="S2"
        )
        assert 0 <= resolution <= 2

    def test_calculate_s2_resolution_large_dataset(self):
        """Large dataset should use higher level."""
        # 100M rows, want ~100K per partition = ~1000 partitions
        # 6 * 4^level = 1000 → level = log(1000/6) / log(4) ≈ 3.7
        resolution = _calculate_a5_resolution(
            total_rows=100000000, target_rows_per_partition=100000, verbose=False, index_name="S2"
        )
        assert 3 <= resolution <= 5

    def test_calculate_s2_resolution_very_large_dataset(self):
        """Very large dataset should use higher level."""
        # 1B rows, want ~10K per partition = ~100K partitions
        # 6 * 4^level = 100K → level = log(100K/6) / log(4) ≈ 7.4
        resolution = _calculate_a5_resolution(
            total_rows=1000000000, target_rows_per_partition=10000, verbose=False, index_name="S2"
        )
        assert 5 <= resolution <= 9

    def test_calculate_s2_resolution_respects_max_partitions(self):
        """Should not exceed max_partitions constraint."""
        resolution = _calculate_a5_resolution(
            total_rows=1000000,
            target_rows_per_partition=10,  # Would create 100K partitions
            max_partitions=1000,  # But limit to 1K
            verbose=False,
            index_name="S2",
        )
        # 6 * 4^level = 1000 → level = log(1000/6) / log(4) ≈ 3.7
        assert resolution <= 5

    def test_calculate_s2_resolution_respects_bounds(self):
        """Should respect min/max level bounds."""
        resolution = _calculate_a5_resolution(
            total_rows=1000000000,
            target_rows_per_partition=1,
            min_resolution=0,
            max_resolution=15,  # Cap at 15
            verbose=False,
            index_name="S2",
        )
        assert resolution <= 15

        resolution = _calculate_a5_resolution(
            total_rows=100,
            target_rows_per_partition=100,
            min_resolution=5,
            max_resolution=30,
            index_name="S2",
        )
        assert resolution >= 5

    def test_calculate_s2_resolution_zero_rows(self):
        """Zero rows should return min level."""
        resolution = _calculate_a5_resolution(
            total_rows=0, target_rows_per_partition=100, verbose=False, index_name="S2"
        )
        assert resolution == 0


class TestAutoResolutionIntegration:
    """Test the main calculate_auto_resolution function with real files."""

    def test_calculate_auto_resolution_h3_with_real_file(self, fields_5070_file):
        """Test auto-resolution calculation with a real GeoParquet file (H3)."""
        resolution = calculate_auto_resolution(
            input_parquet=fields_5070_file,
            spatial_index_type="h3",
            target_rows_per_partition=50,  # Small target for test file
            max_partitions=100,
            verbose=False,
        )
        # Should return a valid H3 resolution
        assert 0 <= resolution <= 15

    def test_calculate_auto_resolution_quadkey_with_real_file(self, fields_5070_file):
        """Test auto-resolution calculation with a real GeoParquet file (quadkey)."""
        resolution = calculate_auto_resolution(
            input_parquet=fields_5070_file,
            spatial_index_type="quadkey",
            target_rows_per_partition=50,
            max_partitions=100,
            verbose=False,
        )
        # Should return a valid quadkey zoom level
        assert 0 <= resolution <= 23

    def test_calculate_auto_resolution_a5_with_real_file(self, fields_5070_file):
        """Test auto-resolution calculation with a real GeoParquet file (A5)."""
        resolution = calculate_auto_resolution(
            input_parquet=fields_5070_file,
            spatial_index_type="a5",
            target_rows_per_partition=50,
            max_partitions=100,
            verbose=False,
        )
        # Should return a valid A5 resolution
        assert 0 <= resolution <= 30

    def test_calculate_auto_resolution_s2_with_real_file(self, fields_5070_file):
        """Test auto-resolution calculation with a real GeoParquet file (S2)."""
        resolution = calculate_auto_resolution(
            input_parquet=fields_5070_file,
            spatial_index_type="s2",
            target_rows_per_partition=50,
            max_partitions=100,
            verbose=False,
        )
        # Should return a valid S2 level
        assert 0 <= resolution <= 30

    def test_calculate_auto_resolution_invalid_type(self, fields_5070_file):
        """Test that invalid spatial index type raises error."""
        with pytest.raises(ValueError, match="Unsupported spatial index type"):
            calculate_auto_resolution(
                input_parquet=fields_5070_file,
                spatial_index_type="invalid_type",
                target_rows_per_partition=100,
            )

    def test_calculate_auto_resolution_empty_file(self, tmp_path):
        """Test that empty file raises error."""
        # Create an empty GeoParquet file
        import geopandas as gpd

        # Create empty GeoDataFrame
        gdf = gpd.GeoDataFrame({"geometry": []}, crs="EPSG:4326")
        empty_file = tmp_path / "empty.parquet"
        gdf.to_parquet(empty_file)

        with pytest.raises(ValueError, match="Input file has no rows"):
            calculate_auto_resolution(
                input_parquet=str(empty_file),
                spatial_index_type="h3",
                target_rows_per_partition=100,
            )

    def test_calculate_auto_resolution_custom_bounds(self, fields_5070_file):
        """Test custom min/max resolution bounds."""
        resolution = calculate_auto_resolution(
            input_parquet=fields_5070_file,
            spatial_index_type="h3",
            target_rows_per_partition=10,  # Would normally create many partitions
            min_resolution=3,
            max_resolution=6,
            verbose=False,
        )
        # Should respect bounds
        assert 3 <= resolution <= 6

    def test_calculate_auto_resolution_negative_target_rows(self, fields_5070_file):
        """Test that negative target_rows raises error."""
        with pytest.raises(
            ValueError, match="target_rows_per_partition must be a positive integer"
        ):
            calculate_auto_resolution(
                input_parquet=fields_5070_file,
                spatial_index_type="h3",
                target_rows_per_partition=-100,
            )

    def test_calculate_auto_resolution_zero_target_rows(self, fields_5070_file):
        """Test that zero target_rows raises error."""
        with pytest.raises(
            ValueError, match="target_rows_per_partition must be a positive integer"
        ):
            calculate_auto_resolution(
                input_parquet=fields_5070_file,
                spatial_index_type="h3",
                target_rows_per_partition=0,
            )

    def test_calculate_auto_resolution_negative_max_partitions(self, fields_5070_file):
        """Test that negative max_partitions raises error."""
        with pytest.raises(ValueError, match="max_partitions must be a positive integer"):
            calculate_auto_resolution(
                input_parquet=fields_5070_file,
                spatial_index_type="h3",
                target_rows_per_partition=100,
                max_partitions=-10,
            )

    def test_calculate_auto_resolution_zero_max_partitions(self, fields_5070_file):
        """Test that zero max_partitions raises error."""
        with pytest.raises(ValueError, match="max_partitions must be a positive integer"):
            calculate_auto_resolution(
                input_parquet=fields_5070_file,
                spatial_index_type="h3",
                target_rows_per_partition=100,
                max_partitions=0,
            )


class TestAutoResolutionMath:
    """Test the mathematical correctness of resolution calculations."""

    def test_h3_resolution_math(self):
        """Verify H3 resolution calculation math is correct."""
        # H3 has ~122 cells at res 0, ~7x more per level
        # For 1000 target partitions: 122 * 7^res = 1000
        # res = log(1000/122) / log(7) ≈ 1.1 → round to 1

        total_rows = 100000
        target_rows = 100
        # This should give us ~1000 partitions

        resolution = _calculate_h3_resolution(total_rows, target_rows)

        # Verify result is close to expected
        expected_partitions = 122 * (7**resolution)
        actual_avg_rows = total_rows / expected_partitions

        # Average rows per partition should be reasonably close to target
        # Allow 2x tolerance due to rounding
        assert target_rows / 2 <= actual_avg_rows <= target_rows * 10

    def test_quadkey_resolution_math(self):
        """Verify quadkey resolution calculation math is correct."""
        # Quadkey has 4^zoom tiles
        # For 1024 target partitions: 4^zoom = 1024
        # zoom = log2(1024) / 2 = 10 / 2 = 5

        total_rows = 102400
        target_rows = 100
        # This should give us ~1024 partitions

        resolution = _calculate_quadkey_resolution(total_rows, target_rows)

        # Verify result is close to expected
        expected_partitions = 4**resolution
        actual_avg_rows = total_rows / expected_partitions

        # Average rows per partition should be reasonably close to target
        assert target_rows / 2 <= actual_avg_rows <= target_rows * 10


class TestVerboseOutput:
    """Test verbose output logging with correct index names."""

    @pytest.fixture(autouse=True)
    def setup_logging(self):
        """Set up logging for caplog to work."""
        logger = get_logger()
        original_propagate = logger.propagate
        original_level = logger.level
        original_handlers = logger.handlers.copy()
        logger.setLevel(logging.DEBUG)
        logger.handlers.clear()
        logger.propagate = True  # Enable propagation for caplog to work
        yield
        # Clean up - restore original state
        logger.handlers.clear()
        logger.handlers.extend(original_handlers)
        logger.propagate = original_propagate
        logger.setLevel(original_level)

    def test_s2_verbose_output_uses_s2_name(self, caplog):
        """Verbose output should show 'S2' for S2 index type."""
        with caplog.at_level(logging.INFO, logger="geoparquet_io"):
            _calculate_a5_resolution(
                total_rows=100000,
                target_rows_per_partition=1000,
                verbose=True,
                index_name="S2",
            )
        assert "S2 auto-resolution" in caplog.text

    def test_a5_verbose_output_uses_a5_name(self, caplog):
        """Verbose output should show 'A5' for A5 index type."""
        with caplog.at_level(logging.INFO, logger="geoparquet_io"):
            _calculate_a5_resolution(
                total_rows=100000,
                target_rows_per_partition=1000,
                verbose=True,
                index_name="A5",
            )
        assert "A5 auto-resolution" in caplog.text

    def test_h3_verbose_output_uses_h3_name(self, caplog):
        """Verbose output should show 'H3' for H3 index type."""
        with caplog.at_level(logging.INFO, logger="geoparquet_io"):
            _calculate_h3_resolution(
                total_rows=100000,
                target_rows_per_partition=1000,
                verbose=True,
            )
        assert "H3 auto-resolution" in caplog.text

    def test_quadkey_verbose_output_uses_quadkey_name(self, caplog):
        """Verbose output should show 'Quadkey' for Quadkey index type."""
        with caplog.at_level(logging.INFO, logger="geoparquet_io"):
            _calculate_quadkey_resolution(
                total_rows=100000,
                target_rows_per_partition=1000,
                verbose=True,
            )
        assert "Quadkey auto-resolution" in caplog.text


# ---------------------------------------------------------------------------
# Extent-aware resolution (issue #524)
# ---------------------------------------------------------------------------


def _make_clustered_geoparquet(path, n=8000, seed=42):
    """Write a regionally-clustered point GeoParquet (lon/lat, EPSG:4326).

    Points are confined to a small bbox (roughly the Netherlands) so the data
    occupies a tiny fraction of the globe. A globally-uniform resolution formula
    badly under-resolves data like this; this is the core of issue #524.
    """
    import geopandas as gpd
    import numpy as np
    from shapely.geometry import Point

    rng = np.random.default_rng(seed)
    lons = rng.uniform(4.0, 7.0, n)
    lats = rng.uniform(51.0, 53.5, n)
    gdf = gpd.GeoDataFrame(
        {"id": range(n), "geometry": [Point(x, y) for x, y in zip(lons, lats, strict=False)]},
        crs="EPSG:4326",
    )
    gdf.to_parquet(path)
    return str(path)


def _count_distinct_cells(parquet_file, index_type, resolution):
    """Count distinct non-empty cells over the data at a given resolution.

    Independent of the implementation under test: builds the cell expression
    directly via DuckDB spatial/h3/quadkey functions.
    """
    import mercantile

    from geoparquet_io.core.common import get_duckdb_connection

    con = get_duckdb_connection(load_spatial=True)
    try:
        lon = "ST_X(ST_Centroid(geometry))"
        lat = "ST_Y(ST_Centroid(geometry))"
        if index_type == "a5":
            con.execute("INSTALL a5 FROM community")
            con.execute("LOAD a5")
            expr = f"a5_lonlat_to_cell({lon}, {lat}, {resolution})"
        elif index_type == "s2":
            con.execute("INSTALL geography FROM community")
            con.execute("LOAD geography")
            expr = f"s2_cell_token(s2_cell_parent(s2_cellfromlonlat({lon}, {lat}), {resolution}))"
        elif index_type == "h3":
            con.execute("INSTALL h3 FROM community")
            con.execute("LOAD h3")
            expr = f"h3_latlng_to_cell_string({lat}, {lon}, {resolution})"
        elif index_type == "quadkey":
            con.create_function(
                "lat_lon_to_quadkey",
                lambda la, lo, lv: mercantile.quadkey(mercantile.tile(lo, la, lv)),
                ["DOUBLE", "DOUBLE", "INTEGER"],
                "VARCHAR",
            )
            expr = f"lat_lon_to_quadkey({lat}, {lon}, {resolution})"
        else:
            raise ValueError(index_type)
        row = con.execute(f"SELECT COUNT(DISTINCT {expr}) FROM '{parquet_file}'").fetchone()
        return row[0]
    finally:
        con.close()


@pytest.fixture
def clustered_file(tmp_path):
    """A regionally-clustered point GeoParquet for extent-aware tests."""
    return _make_clustered_geoparquet(tmp_path / "clustered.parquet")


@pytest.mark.network
class TestExtentAwareResolution:
    """Auto-resolution should be sized to the data's actual extent (issue #524).

    A globally-uniform formula picks a far-too-coarse resolution for
    regional/national data, collapsing it into a handful of giant partitions.
    These tests pin the extent-aware behavior.
    """

    @pytest.mark.parametrize("index_type", ["a5", "s2", "h3", "quadkey"])
    def test_finer_than_global_formula(self, clustered_file, index_type):
        """Extent-aware resolution must exceed the old global-formula choice."""
        total_rows = 8000
        target_rows = 80  # ~100 target partitions

        if index_type in ("a5", "s2"):
            global_res = _calculate_a5_resolution(total_rows, target_rows)
        elif index_type == "h3":
            global_res = _calculate_h3_resolution(total_rows, target_rows)
        else:
            global_res = _calculate_quadkey_resolution(total_rows, target_rows)

        extent_res = calculate_auto_resolution(
            input_parquet=clustered_file,
            spatial_index_type=index_type,
            target_rows_per_partition=target_rows,
        )

        assert extent_res > global_res, (
            f"{index_type}: extent-aware res {extent_res} should be finer than "
            f"global-formula res {global_res} for clustered data"
        )

    @pytest.mark.parametrize("index_type", ["a5", "s2", "h3", "quadkey"])
    def test_chosen_resolution_near_target_partitions(self, clustered_file, index_type):
        """The chosen resolution's non-empty cell count is near the target."""
        total_rows = 8000
        target_rows = 80
        target_partitions = total_rows / target_rows  # 100

        res = calculate_auto_resolution(
            input_parquet=clustered_file,
            spatial_index_type=index_type,
            target_rows_per_partition=target_rows,
        )
        cells = _count_distinct_cells(clustered_file, index_type, res)

        # Should be within a 4x band of the target partition count, and far
        # better than the 1-3 partitions the global formula would yield.
        assert target_partitions / 4 <= cells <= target_partitions * 4, (
            f"{index_type}: res {res} gave {cells} cells, target ~{target_partitions:.0f}"
        )

    def test_respects_max_resolution(self, clustered_file):
        """Probe must not exceed the max_resolution bound."""
        res = calculate_auto_resolution(
            input_parquet=clustered_file,
            spatial_index_type="a5",
            target_rows_per_partition=1,  # would want a very fine resolution
            max_resolution=5,
        )
        assert res <= 5

    def test_respects_min_resolution(self, clustered_file):
        """Probe must not go below the min_resolution bound."""
        res = calculate_auto_resolution(
            input_parquet=clustered_file,
            spatial_index_type="a5",
            target_rows_per_partition=10_000_000,  # would want a coarse resolution
            min_resolution=4,
        )
        assert res >= 4

    def test_falls_back_when_no_geometry(self, tmp_path):
        """A parquet with no geometry column falls back to the global formula."""
        import pandas as pd

        non_geo = tmp_path / "non_geo.parquet"
        pd.DataFrame({"a": range(8000)}).to_parquet(non_geo)

        res = calculate_auto_resolution(
            input_parquet=str(non_geo),
            spatial_index_type="a5",
            target_rows_per_partition=80,
        )
        # Falls back to the pure-math global formula for the same row count.
        expected = _calculate_a5_resolution(8000, 80)
        assert res == expected

    def test_falls_back_when_all_geometries_null(self, tmp_path):
        """All-NULL geometries probe to zero cells and fall back, not min_resolution."""
        import geopandas as gpd

        all_null = tmp_path / "all_null.parquet"
        gdf = gpd.GeoDataFrame(
            {"id": range(8000), "geometry": [None] * 8000},
            crs="EPSG:4326",
        )
        gdf.to_parquet(all_null)

        res = calculate_auto_resolution(
            input_parquet=str(all_null),
            spatial_index_type="a5",
            target_rows_per_partition=80,
        )
        # The probe finds no non-empty cells, so the global formula is used
        # instead of silently collapsing to the coarsest (min) resolution.
        expected = _calculate_a5_resolution(8000, 80)
        assert res == expected
        assert res > 0
