"""Spatial-index covering metadata survives alongside a bbox covering.

Regression tests for #694: `gpio add quadkey/h3/s2/a5` record their index in the
GeoParquet `covering` metadata, and the bbox covering used to *replace* that dict
rather than merge into it. On an input with a bbox column the index entry was
silently discarded; on an input without one the clobbering branch was skipped,
which is why every pre-existing covering assertion ran against
`buildings_test.parquet` (no bbox column) and passed.

These tests run against a bbox-bearing 1.1 input and assert *both* entries.
"""

import json

import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.add.a5 import add_a5_column
from geoparquet_io.core.add.h3 import add_h3_column
from geoparquet_io.core.add.quadkey import add_quadkey_column
from geoparquet_io.core.add.s2 import add_s2_column

# (index key, writer, kwargs, expected covering column)
INDEX_COMMANDS = [
    ("h3", add_h3_column, {"h3_resolution": 8}, "h3_cell"),
    ("s2", add_s2_column, {"s2_level": 10}, "s2_cell"),
    ("a5", add_a5_column, {"a5_resolution": 8}, "a5_cell"),
    ("quadkey", add_quadkey_column, {}, "quadkey"),
]


def _covering(path):
    geo = json.loads(pq.ParquetFile(str(path)).metadata.metadata[b"geo"].decode("utf-8"))
    return geo["columns"][geo["primary_column"]].get("covering", {})


@pytest.mark.parametrize(
    ("index_key", "writer", "kwargs", "column"),
    INDEX_COMMANDS,
    ids=[c[0] for c in INDEX_COMMANDS],
)
class TestSpatialIndexCoveringWithBbox:
    def test_index_covering_survives_bbox_covering(
        self, places_v11_file, tmp_path, index_key, writer, kwargs, column
    ):
        """Both the bbox covering and the spatial-index covering reach the file."""
        output = tmp_path / f"{index_key}.parquet"

        writer(places_v11_file, str(output), **kwargs)

        covering = _covering(output)
        assert index_key in covering, (
            f"{index_key} covering was dropped; got keys {sorted(covering)}"
        )
        assert "bbox" in covering, f"bbox covering was dropped; got keys {sorted(covering)}"
        assert covering[index_key]["column"] == column
        assert covering["bbox"]["xmin"] == ["bbox", "xmin"]

    def test_index_covering_present_without_bbox_column(
        self, buildings_test_file, tmp_path, index_key, writer, kwargs, column
    ):
        """The bbox-free input keeps working — this is the branch that always passed."""
        output = tmp_path / f"{index_key}.parquet"

        writer(buildings_test_file, str(output), **kwargs)

        covering = _covering(output)
        assert index_key in covering
        assert covering[index_key]["column"] == column
