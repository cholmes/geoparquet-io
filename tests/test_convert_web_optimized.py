"""End-to-end tests for the --optimize-for web convert profile (Tasks 4 + 5).

Verifies that convert_to_geoparquet(..., optimize_for="web") forces GeoParquet
2.0 with native GeospatialStatistics, keeps a covering bbox column for viewport
pruning, and routes the write through the streaming strategy so per-row-group
bboxes and the page index are actually produced (not silently skipped by the
plain-COPY fast path).
"""

import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.convert import convert_to_geoparquet
from geoparquet_io.core.metadata_utils import has_parquet_native_geo_stats


@pytest.mark.slow
def test_web_convert_has_native_stats_and_covering(buildings_test_file, tmp_path):
    out = tmp_path / "web.parquet"
    convert_to_geoparquet(
        buildings_test_file, str(out), skip_hilbert=False, optimize_for="web", verbose=False
    )

    # 1. Native GeospatialStatistics present (GeoParquet 2.0).
    assert has_parquet_native_geo_stats(str(out))["has_stats"] is True

    pf = pq.ParquetFile(str(out))
    names = pf.schema_arrow.names
    # 2. Covering bbox column present despite version 2.0.
    assert "bbox" in names
    # 3. Covering bbox leaves carry per-row-group stats + page index.
    rg0 = pf.metadata.row_group(0)
    leaf_names = [rg0.column(i).path_in_schema for i in range(rg0.num_columns)]
    bbox_leaf = leaf_names.index("bbox.xmin")
    assert rg0.column(bbox_leaf).has_column_index is True
    assert rg0.column(bbox_leaf).has_offset_index is True


@pytest.mark.slow
def test_web_convert_geojson_input_has_native_stats_and_covering(geojson_input, tmp_path):
    out = tmp_path / "web_geojson.parquet"
    convert_to_geoparquet(
        geojson_input, str(out), skip_hilbert=False, optimize_for="web", verbose=False
    )

    # 1. Native GeospatialStatistics present (GeoParquet 2.0) for a non-parquet
    #    (GDAL/ST_Read) input, covering the _convert_spatial_path branch.
    assert has_parquet_native_geo_stats(str(out))["has_stats"] is True

    pf = pq.ParquetFile(str(out))
    names = pf.schema_arrow.names
    # 2. Covering bbox column forced even at v2.0, proving force_include_bbox
    #    threaded through the non-parquet path too.
    assert "bbox" in names


@pytest.mark.slow
def test_web_convert_row_group_bboxes_usable_after_hilbert(buildings_test_file, tmp_path):
    from geoparquet_io.core.duckdb_metadata import get_per_row_group_bbox_stats

    out = tmp_path / "web2.parquet"
    convert_to_geoparquet(
        buildings_test_file, str(out), skip_hilbert=False, optimize_for="web", verbose=False
    )
    stats = get_per_row_group_bbox_stats(str(out), bbox_column="bbox")
    assert len(stats) >= 1
    for s in stats:  # each row group has a finite bbox usable by a viewer
        assert s["xmin"] <= s["xmax"] and s["ymin"] <= s["ymax"]


@pytest.mark.slow
def test_cli_optimize_for_web(buildings_test_file, tmp_path):
    from click.testing import CliRunner

    from geoparquet_io.cli.main import convert

    out = tmp_path / "cli_web.parquet"
    result = CliRunner().invoke(
        convert, ["geoparquet", buildings_test_file, str(out), "--optimize-for", "web"]
    )
    assert result.exit_code == 0, result.output
    assert has_parquet_native_geo_stats(str(out))["has_stats"] is True
