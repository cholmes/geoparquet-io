#!/usr/bin/env python3
"""A5-cell aggregation for `gpio process aggregate a5`.

Thin scheme definition over the shared engine in ``grid_common``.
"""

from __future__ import annotations

from geoparquet_io.core.constants import DEFAULT_A5_COLUMN_NAME
from geoparquet_io.core.process.aggregate.grid_common import (
    GridScheme,
    aggregate_grid_file,
    aggregate_grid_table,
)

A5_SCHEME = GridScheme(
    name="a5",
    extension="a5",
    min_resolution=0,
    max_resolution=30,
    default_column=DEFAULT_A5_COLUMN_NAME,
    key_template="a5_lonlat_to_cell(ST_X(ST_Centroid({geom})), ST_Y(ST_Centroid({geom})), {res})",
    boundary_template="a5_cell_to_boundary({cell})",
    latlng_template="a5_cell_to_lonlat({cell})",
    # a5_cell_to_boundary returns DOUBLE[2][]; close the ring and build a polygon.
    poly_wkb_template=(
        "ST_AsWKB(ST_MakePolygon(ST_MakeLine("
        "list_transform(list_append({bnd}, {bnd}[1]), p -> ST_Point(p[1], p[2])))))"
    ),
    # a5_cell_to_lonlat returns [lon, lat].
    centroid_wkb_template="ST_AsWKB(ST_Point({ll}[1], {ll}[2]))",
)


def aggregate_by_a5(
    input_parquet: str,
    output_parquet: str,
    resolution: int | None = None,
    auto: bool = False,
    target_per_cell: int = 10000,
    max_cells: int = 500000,
    metric: str | None = None,
    breakdown: str | None = None,
    breakdown_limit: int = 20,
    out_geometry: str = "polygon",
    a5_column_name: str = DEFAULT_A5_COLUMN_NAME,
    compression: str = "ZSTD",
    compression_level: int | None = None,
    geoparquet_version: str | None = None,
    verbose: bool = False,
    show_sql: bool = False,
    where: str | None = None,
) -> None:
    """Aggregate a GeoParquet file into A5 cells. Writes the output file."""
    aggregate_grid_file(
        A5_SCHEME,
        input_parquet,
        output_parquet,
        resolution=resolution,
        auto=auto,
        target_per_cell=target_per_cell,
        max_cells=max_cells,
        metric=metric,
        breakdown=breakdown,
        breakdown_limit=breakdown_limit,
        out_geometry=out_geometry,
        cell_column=a5_column_name,
        compression=compression,
        compression_level=compression_level,
        geoparquet_version=geoparquet_version,
        verbose=verbose,
        show_sql=show_sql,
        where=where,
    )


def aggregate_a5_table(
    table,
    resolution: int,
    metric: str | None = None,
    breakdown: str | None = None,
    breakdown_limit: int = 20,
    out_geometry: str = "polygon",
    a5_column_name: str = DEFAULT_A5_COLUMN_NAME,
    geometry_column: str | None = None,
    where: str | None = None,
):
    """Aggregate an in-memory Arrow table by a5 cell. Returns a new Arrow table."""
    return aggregate_grid_table(
        A5_SCHEME,
        table,
        resolution=resolution,
        metric=metric,
        breakdown=breakdown,
        breakdown_limit=breakdown_limit,
        out_geometry=out_geometry,
        cell_column=a5_column_name,
        geometry_column=geometry_column,
        where=where,
    )
