"""Writers must emit dimension-suffixed geometry_types for Z/M data (review todo 035).

The GeoParquet spec treats "LineString" and "LineString ZM" as distinct
geometry_types entries. The write-side SQL scan previously collapsed the
dimension, so gpio's own converted Z/M output failed gpio's own spec check.
"""

import pytest

from geoparquet_io.core.common import get_duckdb_connection, split_zm_suffix
from geoparquet_io.core.convert import convert_to_geoparquet
from geoparquet_io.core.validate import CheckStatus, validate_geoparquet
from tests.conftest import get_geo_metadata

WKT_BY_DIM = {
    "Z": ("LINESTRING Z (0 0 1, 1 1 2)", "LineString Z"),
    "M": ("LINESTRING M (0 0 5, 1 1 6)", "LineString M"),
    "ZM": ("LINESTRING ZM (0 0 1 5, 1 1 2 6)", "LineString ZM"),
}


def _make_source(tmp_path, wkt):
    path = tmp_path / "src.parquet"
    con = get_duckdb_connection(load_spatial=True)
    con.execute(f"""
        COPY (
          SELECT * FROM (VALUES
            (1, ST_GeomFromText('{wkt}'))
          ) t(id, geometry)
        ) TO '{path.as_posix()}' (FORMAT PARQUET, GEOPARQUET_VERSION 'V2')
    """)
    con.close()
    return path


def test_split_zm_suffix():
    assert split_zm_suffix("Point") == ("Point", "")
    assert split_zm_suffix("Point Z") == ("Point", " Z")
    assert split_zm_suffix("MultiPolygon M") == ("MultiPolygon", " M")
    assert split_zm_suffix("LineString ZM") == ("LineString", " ZM")


def test_compute_geometry_types_via_sql_is_dimension_aware():
    """Both copies (common + geo_metadata) must return spec-suffixed names."""
    from geoparquet_io.core import common, geo_metadata

    con = get_duckdb_connection(load_spatial=True)
    try:
        query = (
            "SELECT ST_GeomFromText('LINESTRING ZM (0 0 1 5, 1 1 2 6)') AS geometry "
            "UNION ALL SELECT ST_GeomFromText('LINESTRING (2 2, 3 3)')"
        )
        expected = ["LineString", "LineString ZM"]
        assert common.compute_geometry_types_via_sql(con, query, "geometry") == expected
        assert geo_metadata.compute_geometry_types_via_sql(con, query, "geometry") == expected
    finally:
        con.close()


@pytest.mark.parametrize("dim", ["Z", "M", "ZM"])
@pytest.mark.parametrize("version", ["1.1", "2.0"])
def test_convert_emits_suffixed_geometry_types(tmp_path, dim, version):
    """Round-trip: converted Z/M output declares suffixed types and self-validates."""
    wkt, expected = WKT_BY_DIM[dim]
    src = _make_source(tmp_path, wkt)
    out = tmp_path / f"out_{dim}_{version.replace('.', '_')}.parquet"
    convert_to_geoparquet(str(src), str(out), skip_hilbert=True, geoparquet_version=version)

    geo = get_geo_metadata(str(out))
    col = geo["columns"][geo["primary_column"]]
    assert col["geometry_types"] == [expected], col["geometry_types"]

    result = validate_geoparquet(str(out), validate_data=True, sample_size=0)
    failures = [
        c for c in result.checks if "geometry_types" in c.name and c.status == CheckStatus.FAILED
    ]
    assert not failures, [f"{c.name}: {c.message}" for c in failures]
