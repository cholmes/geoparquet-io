#!/usr/bin/env python3
"""Zoom-band and overview-level selection for `gpio process overview`.

Shared with `gpio pmtiles pyramid`: both need to know which aggregate level
should serve which WebMercator zoom range so that no tile blows past the
tile-size budget. The selection is driven by a *worst tile* probe -- for each
candidate level, the number of cells landing in the fullest tile per zoom --
multiplied by an estimated compressed bytes-per-cell.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from geoparquet_io.core.exceptions import InvalidParameterError

# Estimated compressed bytes a single cell contributes to an MVT tile,
# by output geometry mode. Deliberately conservative (budget heuristic,
# not a codec model); calibrated loosely against GlobalBuildingAtlas
# measurements (a5 res-5 z0 tile ~297 KB, res-6 z0 ~791 KB).
GEOMETRY_BYTES: dict[str, float] = {
    "polygon": 35.0,
    "centroid": 12.0,
    "both": 45.0,
    "none": 0.0,
}
BYTES_PER_ATTRIBUTE = 8.0
CELL_ID_BYTES = 8.0

DEFAULT_MAX_TILE_KB = 500  # tippecanoe's default per-tile cap
MAX_PROBE_ZOOM = 20


@dataclass(frozen=True)
class Band:
    """One zoom band of the pyramid: ``level`` serves zooms minzoom..maxzoom.

    ``maxzoom is None`` marks the final (open-ended) band.
    """

    level: int | str
    minzoom: int
    maxzoom: int | None


def estimate_bytes_per_cell(num_attributes: int, out_geometry: str) -> float:
    """Estimate compressed bytes one cell contributes to a tile.

    ``num_attributes`` counts the numeric attribute columns carried per cell
    (``count`` plus every rollup/breakdown column).
    """
    return CELL_ID_BYTES + GEOMETRY_BYTES[out_geometry] + BYTES_PER_ATTRIBUTE * num_attributes


def probe_worst_tile_counts(con, cells_sql: str, max_zoom: int = MAX_PROBE_ZOOM) -> dict[int, int]:
    """Worst (max) cells-per-tile for each zoom 0..max_zoom.

    ``cells_sql`` must select one row per cell with ``lon``/``lat`` columns.
    The connection must have the ``lat_lon_to_quadkey`` UDF registered (see
    :func:`geoparquet_io.core.partition.auto_resolution._register_quadkey_udf`).
    A quadkey prefix of length ``z`` identifies the WebMercator tile at zoom
    ``z``, so one quadkey per cell at ``max_zoom`` covers every zoom via
    ``substr``. Latitudes are clamped to the WebMercator domain.
    """
    con.execute(
        "CREATE OR REPLACE TEMP TABLE __overview_qk AS "
        f"SELECT lat_lon_to_quadkey(LEAST(GREATEST(lat, -85.05), 85.05), lon, {max_zoom}) AS qk "
        f"FROM ({cells_sql}) WHERE lon IS NOT NULL AND lat IS NOT NULL"
    )
    counts: dict[int, int] = {}
    for z in range(max_zoom + 1):
        row = con.execute(
            "SELECT COALESCE(MAX(cnt), 0) FROM "
            f"(SELECT COUNT(*) AS cnt FROM __overview_qk GROUP BY substr(qk, 1, {z}))"
        ).fetchone()
        counts[z] = int(row[0]) if row else 0
    return counts


def select_bands(
    worst_counts: Mapping[int | str, Mapping[int, int]],
    candidates: Sequence[int | str],
    bytes_per_cell: float,
    max_tile_kb: int = DEFAULT_MAX_TILE_KB,
) -> list[Band]:
    """Assign candidate levels to contiguous zoom bands within the tile budget.

    Walks zooms from 0 upward, at each zoom picking the finest candidate whose
    worst tile fits ``max_tile_kb`` (never coarser than the previous pick), and
    stops once the base level -- the last candidate -- fits. When nothing fits
    at a zoom the coarsest candidate is used, so the first band always starts
    at z0 (a single oversized z0 tile is acceptable). If the base never fits
    within the probed zooms it still gets the final open-ended band.
    """
    if not candidates:
        raise InvalidParameterError("levels", "no candidate levels to select from")
    budget = max_tile_kb * 1024.0
    base = candidates[-1]
    max_zoom = min(max(zooms) for zooms in (worst_counts[lvl] for lvl in candidates))

    picks: list[int | str] = []
    prev_idx = 0
    for z in range(max_zoom + 1):
        fitting = [
            i for i, lvl in enumerate(candidates) if worst_counts[lvl][z] * bytes_per_cell <= budget
        ]
        idx = max(max(fitting) if fitting else 0, prev_idx)
        picks.append(candidates[idx])
        prev_idx = idx
        if candidates[idx] == base:
            break
    else:
        # Probe range exhausted without the base fitting; hand the remaining
        # zooms to the base anyway -- it is the real data.
        picks.append(base)

    bands: list[Band] = []
    start = 0
    for i in range(1, len(picks) + 1):
        if i == len(picks) or picks[i] != picks[i - 1]:
            bands.append(Band(picks[i - 1], start, i - 1))
            start = i
    last = bands[-1]
    bands[-1] = Band(last.level, last.minzoom, None)
    return bands
