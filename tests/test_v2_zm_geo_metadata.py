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


def test_repair_preserves_lz4_codec_and_row_groups(zm_source, tmp_path):
    """The metadata repair rewrite must not change codec or row-group layout (todo 041)."""
    out = tmp_path / "out.parquet"
    convert_to_geoparquet(
        str(zm_source),
        str(out),
        skip_hilbert=True,
        geoparquet_version="2.0",
        compression="LZ4",
        row_group_rows=1,
    )
    pf = pq.ParquetFile(str(out))
    assert b"geo" in pf.metadata.metadata, "geo metadata missing after repair"
    assert pf.metadata.num_row_groups == 2, "explicit row-group size not preserved"
    codecs = {
        pf.metadata.row_group(i).column(0).compression for i in range(pf.metadata.num_row_groups)
    }
    assert codecs <= {"LZ4", "LZ4_RAW"}, f"LZ4 silently rewritten: {codecs}"


def test_rewrite_keeps_row_group_structure_when_unspecified(tmp_path):
    """With row_group_rows=None the rewrite must mirror the file's own layout."""
    import pyarrow as pa

    from geoparquet_io.core.common import _rewrite_file_with_geo_metadata

    path = tmp_path / "plain.parquet"
    pq.write_table(pa.table({"a": list(range(30))}), str(path), row_group_size=10)
    assert pq.ParquetFile(str(path)).metadata.num_row_groups == 3

    _rewrite_file_with_geo_metadata(str(path), {"version": "2.0.0"}, "ZSTD", None, None)
    assert pq.ParquetFile(str(path)).metadata.num_row_groups == 3


def test_rewrite_preserves_lz4_codec_unit(tmp_path):
    """codec_map must cover the normalized 'LZ4' name callers actually pass."""
    import pyarrow as pa

    from geoparquet_io.core.common import _rewrite_file_with_geo_metadata

    path = tmp_path / "plain.parquet"
    pq.write_table(pa.table({"a": [1, 2, 3]}), str(path), compression="lz4")

    _rewrite_file_with_geo_metadata(str(path), {"version": "2.0.0"}, "LZ4", None, None)
    codec = pq.ParquetFile(str(path)).metadata.row_group(0).column(0).compression
    assert codec in ("LZ4", "LZ4_RAW"), codec


# --- Primary-column choice for multi-geometry repairs (todo 047-C6) ---


def test_repair_uses_real_primary_column_not_alphabetical(tmp_path):
    """With two geometry columns, the repair must honor the caller's primary
    column instead of picking the alphabetically-first one."""
    from geoparquet_io.core.common import _ensure_v2_geo_metadata

    path = tmp_path / "two_geoms.parquet"
    con = get_duckdb_connection(load_spatial=True)
    con.execute(f"""
        COPY (
          SELECT * FROM (VALUES
            (1, ST_GeomFromText('LINESTRING M (0 0 0, 1 1 10)'),
                ST_GeomFromText('POINT M (5 5 1)'))
          ) t(id, a_geom, the_geom)
        ) TO '{path.as_posix()}' (FORMAT PARQUET, GEOPARQUET_VERSION 'V2')
    """)
    con.close()
    # Precondition: the DuckDB M-dimension bug leaves the file without geo KV.
    assert (pq.ParquetFile(str(path)).metadata.metadata or {}).get(b"geo") is None

    _ensure_v2_geo_metadata(str(path), primary_column="the_geom")

    geo = json.loads(pq.ParquetFile(str(path)).metadata.metadata[b"geo"])
    assert set(geo["columns"]) == {"a_geom", "the_geom"}
    assert geo["primary_column"] == "the_geom"
