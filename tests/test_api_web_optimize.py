"""Tests for the Python API's optimize_for="web" surface (Task 7).

Mirrors `gpio convert geoparquet --optimize-for web` in the Python API via:
  7a. Module-level `convert_geoparquet(input_file, output_file, optimize_for="web")`
      that delegates to the tested `convert_to_geoparquet` file->file pipeline.
  7b. `Table.write(path, optimize_for="web")` for the fluent, in-memory Table path.

Both must produce a GeoParquet 2.0 file with native Parquet GeospatialStatistics
and a covering bbox column whose leaves carry per-row-group page index entries.
"""

import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.metadata_utils import has_parquet_native_geo_stats


@pytest.mark.slow
def test_convert_geoparquet_optimize_for_web(buildings_test_file, tmp_path):
    from geoparquet_io.api.table import convert_geoparquet

    out = tmp_path / "web_api.parquet"
    result = convert_geoparquet(buildings_test_file, str(out), optimize_for="web")

    assert result == out
    assert has_parquet_native_geo_stats(str(out))["has_stats"] is True


@pytest.mark.slow
def test_convert_geoparquet_importable_from_top_level():
    from geoparquet_io import convert_geoparquet as top_level_convert_geoparquet
    from geoparquet_io.api.table import convert_geoparquet

    assert top_level_convert_geoparquet is convert_geoparquet


@pytest.mark.slow
def test_table_write_optimize_for_web(gpkg_buildings, tmp_path):
    import geoparquet_io as gpio

    out = tmp_path / "web_table.parquet"
    gpio.convert(gpkg_buildings).sort_hilbert().write(str(out), optimize_for="web")

    assert has_parquet_native_geo_stats(str(out))["has_stats"] is True

    pf = pq.ParquetFile(str(out))
    names = pf.schema_arrow.names
    assert "bbox" in names

    rg0 = pf.metadata.row_group(0)
    leaf_names = [rg0.column(i).path_in_schema for i in range(rg0.num_columns)]
    bbox_leaf = leaf_names.index("bbox.xmin")
    assert rg0.column(bbox_leaf).has_column_index is True
    assert rg0.column(bbox_leaf).has_offset_index is True
