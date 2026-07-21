"""GeoParquet 2.0 writes of M/ZM data must not lose the geo metadata (gpio #589).

DuckDB 1.5.4's V2 writer omits the `geo` key-value metadata when geometries
have an M dimension (XY and XYZ are fine). gpio must guarantee the metadata
regardless, or the output silently degrades to parquet-geo-only.
"""

import json

import pyarrow.parquet as pq
import pytest

from geoparquet_io.core.common import get_duckdb_connection
from geoparquet_io.core.convert import convert_to_geoparquet
from tests.conftest import get_geo_metadata, get_geoparquet_version, has_native_geo_types


@pytest.fixture
def zm_source(tmp_path):
    path = tmp_path / "src.parquet"
    con = get_duckdb_connection(load_spatial=True)
    con.execute(f"""
        COPY (
          SELECT * FROM (VALUES
            (1, ST_GeomFromText('LINESTRING ZM (0 0 100 0, 1 1 110 10)')),
            (2, ST_GeomFromText('LINESTRING ZM (5 5 50 0, 6 5 60 5)'))
          ) t(id, geometry)
        ) TO '{path.as_posix()}' (FORMAT PARQUET, GEOPARQUET_VERSION 'V2')
    """)
    con.close()
    return path


@pytest.fixture
def xym_source(tmp_path):
    path = tmp_path / "src.parquet"
    con = get_duckdb_connection(load_spatial=True)
    con.execute(f"""
        COPY (
          SELECT * FROM (VALUES
            (1, ST_GeomFromText('LINESTRING M (0 0 0, 1 1 10)'))
          ) t(id, geometry)
        ) TO '{path.as_posix()}' (FORMAT PARQUET, GEOPARQUET_VERSION 'V2')
    """)
    con.close()
    return path


@pytest.mark.parametrize("source", ["zm_source", "xym_source"])
def test_v2_write_of_m_data_keeps_geo_metadata(source, tmp_path, request):
    src = request.getfixturevalue(source)
    out = tmp_path / "out.parquet"
    convert_to_geoparquet(str(src), str(out), skip_hilbert=True, geoparquet_version="2.0")

    assert get_geoparquet_version(str(out)) == "2.0.0", "geo metadata missing from 2.0 output"
    assert has_native_geo_types(str(out)), "native logical type lost"

    geo = get_geo_metadata(str(out))
    col = geo["columns"][geo["primary_column"]]
    assert col["encoding"] == "WKB"
    suffix = "ZM" if source == "zm_source" else "M"
    assert col["geometry_types"] == [f"LineString {suffix}"], col["geometry_types"]


def test_repaired_metadata_matches_duckdb_shape(zm_source, tmp_path):
    """The repaired geo block mirrors what DuckDB writes for XY/Z data."""
    out = tmp_path / "out.parquet"
    convert_to_geoparquet(str(zm_source), str(out), skip_hilbert=True, geoparquet_version="2.0")
    kv = pq.ParquetFile(str(out)).metadata.metadata
    geo = json.loads(kv[b"geo"])
    assert geo["version"] == "2.0.0"
    assert geo["primary_column"] == "geometry"
    assert set(geo["columns"]) == {"geometry"}
