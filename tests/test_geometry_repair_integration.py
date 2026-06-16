"""End-to-end geometry-repair behavior across the pipeline (issue #506).

The shared helpers are unit-tested in ``test_geometry_repair.py``. These tests
exercise the wiring: that ``convert``, ``convert geojson`` and ``pmtiles create``
repair invalid geometry by default, honour ``--no-repair-geometry``, and that the
reported regression (invalid polygons crashing tippecanoe) no longer occurs.
"""

import json
import logging
import shutil

import duckdb
import pytest

from geoparquet_io.core.convert import convert_to_geoparquet

# Two self-intersecting "bowtie" polygons (invalid) and one valid square.
_INVALID_GEOJSON = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "properties": {"id": 1},
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[0, 0], [1, 1], [1, 0], [0, 1], [0, 0]]],
            },
        },
        {
            "type": "Feature",
            "properties": {"id": 2},
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[0, 0], [2, 2], [2, 0], [0, 2], [0, 0]]],
            },
        },
        {
            "type": "Feature",
            "properties": {"id": 3},
            "geometry": {
                "type": "Polygon",
                "coordinates": [[[0, 0], [0, 1], [1, 1], [1, 0], [0, 0]]],
            },
        },
    ],
}


def _write_invalid_geojson(tmp_path):
    path = tmp_path / "invalid.geojson"
    path.write_text(json.dumps(_INVALID_GEOJSON))
    return str(path)


def _invalid_count(parquet_path, geom_col="geom"):
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    try:
        return con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{parquet_path}') WHERE NOT ST_IsValid({geom_col})"
        ).fetchone()[0]
    finally:
        con.close()


def test_convert_repairs_invalid_geometry_by_default(tmp_path):
    src = _write_invalid_geojson(tmp_path)
    out = str(tmp_path / "out.parquet")
    convert_to_geoparquet(src, out)
    assert _invalid_count(out) == 0


def test_convert_opt_out_preserves_invalid_and_warns(tmp_path, caplog):
    src = _write_invalid_geojson(tmp_path)
    out = str(tmp_path / "out.parquet")
    with caplog.at_level(logging.WARNING, logger="geoparquet_io"):
        convert_to_geoparquet(src, out, repair_geometry=False)
    assert _invalid_count(out) == 2
    assert "Left unrepaired 2 invalid geometries" in caplog.text


def test_convert_repair_preserves_row_count(tmp_path):
    src = _write_invalid_geojson(tmp_path)
    out = str(tmp_path / "out.parquet")
    convert_to_geoparquet(src, out)
    con = duckdb.connect()
    try:
        assert con.execute(f"SELECT COUNT(*) FROM read_parquet('{out}')").fetchone()[0] == 3
    finally:
        con.close()


@pytest.mark.integration
@pytest.mark.skipif(shutil.which("tippecanoe") is None, reason="tippecanoe not installed")
def test_pmtiles_from_invalid_geometry_does_not_crash(tmp_path):
    """Regression: invalid polygons used to crash tippecanoe (exit 110)."""
    from geoparquet_io.core.pmtiles import create_pmtiles_from_geoparquet

    src = _write_invalid_geojson(tmp_path)
    # Build a parquet that still contains the invalid geometry (opt out of repair
    # at convert time) so the PMTiles step is the one that must cope.
    raw = str(tmp_path / "raw.parquet")
    convert_to_geoparquet(src, raw, repair_geometry=False)
    assert _invalid_count(raw) == 2

    out = str(tmp_path / "out.pmtiles")
    create_pmtiles_from_geoparquet(raw, out, force=True)
    assert (tmp_path / "out.pmtiles").exists()
