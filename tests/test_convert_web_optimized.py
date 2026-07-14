"""End-to-end tests for the --optimize-for web convert profile (Task 4).

Verifies that convert_to_geoparquet(..., optimize_for="web") forces GeoParquet
2.0 with native GeospatialStatistics and keeps a covering bbox column for
viewport pruning, with per-row-group stats and a page index on the bbox leaf.
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
