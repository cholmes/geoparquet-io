"""Cell boundaries must not tear at the antimeridian.

`h3_cell_to_boundary_wkt` wraps every vertex into [-180, 180], so a cell that
straddles the antimeridian comes back with vertices at both +179.x and -179.x.
Read as a ring that is a cell spanning ~359 degrees of longitude, and a renderer
draws it as a band across the whole map.

A5 does not have the problem: `a5_cell_to_boundary` returns continuous
coordinates that run past 180 (for example -180.2), so its ring stays contiguous.
These tests hold both schemes to that behaviour.
"""

import duckdb
import pytest

from geoparquet_io.core.process.aggregate.by_a5 import aggregate_by_a5
from geoparquet_io.core.process.aggregate.by_h3 import aggregate_by_h3

# Fiji sits on the antimeridian, so cells here straddle it at low resolutions.
ANTIMERIDIAN_POINTS = [
    (179.95, -16.2),
    (179.80, -16.4),
    (-179.90, -16.1),
    (-179.75, -16.3),
    (179.60, -16.0),
    (-179.60, -16.5),
]


@pytest.fixture
def antimeridian_parquet(tmp_path):
    path = tmp_path / "antimeridian.parquet"
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy=true")
    values = ", ".join(f"(ST_Point({x}, {y}), 1)" for x, y in ANTIMERIDIAN_POINTS)
    con.execute(
        f"COPY (SELECT * FROM (VALUES {values}) AS t(geometry, n)) TO '{path}' (FORMAT PARQUET)"
    )
    con.close()
    return str(path)


def _max_cell_width_deg(parquet_file):
    """Widest cell in the output, in degrees of longitude."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; SET geometry_always_xy=true")
    width = con.execute(
        f"SELECT max(ST_XMax(geometry) - ST_XMin(geometry)) "
        f"FROM read_parquet('{parquet_file}') WHERE geometry IS NOT NULL"
    ).fetchone()[0]
    con.close()
    return width


@pytest.mark.slow
@pytest.mark.network
@pytest.mark.parametrize("resolution", [1, 3])
def test_h3_cells_do_not_tear_at_the_antimeridian(antimeridian_parquet, tmp_path, resolution):
    out = tmp_path / f"h3_{resolution}.parquet"
    aggregate_by_h3(antimeridian_parquet, str(out), resolution=resolution)
    # A real cell is small. Anything approaching 360 is a torn ring.
    assert _max_cell_width_deg(str(out)) < 180, (
        "an H3 cell spans nearly the whole globe, so its ring tore at the antimeridian"
    )


@pytest.mark.slow
@pytest.mark.network
@pytest.mark.parametrize("resolution", [1, 3])
def test_a5_cells_do_not_tear_at_the_antimeridian(antimeridian_parquet, tmp_path, resolution):
    out = tmp_path / f"a5_{resolution}.parquet"
    aggregate_by_a5(antimeridian_parquet, str(out), resolution=resolution)
    assert _max_cell_width_deg(str(out)) < 180
