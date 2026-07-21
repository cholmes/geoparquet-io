"""Rewrites must not silently drop non-planar edges semantics (gpio #588).

DuckDB has no GEOGRAPHY type, so a native-GEOGRAPHY input inevitably comes
back as GEOMETRY — but the `edges` declaration (spherical/ellipsoidal edge
interpretation) must survive in the geo metadata, or consumers will read
great-circle edges as planar.
"""

import json

import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.common import get_duckdb_connection
from geoparquet_io.core.convert import convert_to_geoparquet
from tests.conftest import get_geo_metadata


@pytest.fixture
def spherical_input(tmp_path):
    """A 2.0 file whose geo metadata declares edges=spherical.

    (Native GEOGRAPHY logical types can't be written locally — only sedonadb
    does that — so this simulates the metadata half of a geography input; the
    corpus suite covers the logical-type half.)
    """
    path = tmp_path / "src.parquet"
    con = get_duckdb_connection(load_spatial=True)
    con.execute(f"""
        COPY (
          SELECT * FROM (VALUES
            (1, ST_GeomFromText('LINESTRING (0 0, 10 10)')),
            (2, ST_GeomFromText('LINESTRING (-179 0, 179 5)'))
          ) t(id, geometry)
        ) TO '{path.as_posix()}' (FORMAT PARQUET, GEOPARQUET_VERSION 'V2')
    """)
    con.close()

    import geoarrow.pyarrow  # noqa: F401  (keep native types through rewrite)

    table = pq.read_table(str(path))
    meta = dict(table.schema.metadata)
    geo = json.loads(meta[b"geo"])
    geo["columns"]["geometry"]["edges"] = "spherical"
    meta[b"geo"] = json.dumps(geo).encode()
    pq.write_table(table.replace_schema_metadata(meta), str(path))
    return path


def test_v2_rewrite_preserves_spherical_edges(spherical_input, tmp_path):
    out = tmp_path / "out.parquet"
    convert_to_geoparquet(
        str(spherical_input), str(out), skip_hilbert=True, geoparquet_version="2.0"
    )
    geo = get_geo_metadata(str(out))
    assert geo["columns"]["geometry"].get("edges") == "spherical", geo["columns"]["geometry"]


def test_v1_1_rewrite_preserves_spherical_edges(spherical_input, tmp_path):
    out = tmp_path / "out.parquet"
    convert_to_geoparquet(
        str(spherical_input), str(out), skip_hilbert=True, geoparquet_version="1.1"
    )
    geo = get_geo_metadata(str(out))
    assert geo["columns"]["geometry"].get("edges") == "spherical", geo["columns"]["geometry"]


def test_auto_rewrite_preserves_spherical_edges(spherical_input, tmp_path):
    out = tmp_path / "out.parquet"
    convert_to_geoparquet(
        str(spherical_input), str(out), skip_hilbert=True, geoparquet_version=None
    )
    geo = get_geo_metadata(str(out))
    assert geo["columns"]["geometry"].get("edges") == "spherical", geo["columns"]["geometry"]
