"""Shared spatial join query construction for admin divisions and country codes."""

from geoparquet_io.core.logging_config import debug, info, progress

# Internal column name uses dunder prefix to avoid collision with user data
_DEDUP_ROW_COL = "__gpio_dedup_rownum__"


def log_bbox_status(
    input_bbox_col, other_bbox_col, input_has_native_geo, *, dry_run=False, verbose=False
):
    """Log bbox optimization status for spatial join."""
    if dry_run:
        if input_bbox_col and other_bbox_col:
            info("-- Using bbox columns for optimized spatial join")
        elif input_has_native_geo and other_bbox_col:
            info("-- Using native geometry bounds for optimized spatial join")
        else:
            info("-- Using full geometry intersection (no bbox optimization)")
    else:
        if input_bbox_col and other_bbox_col and verbose:
            debug("Using bbox columns for initial filtering...")
        elif input_has_native_geo and other_bbox_col:
            progress("Using native geometry bounds for bbox pre-filtering...")
        elif not (input_bbox_col and other_bbox_col):
            progress("No bbox columns available, using full geometry intersection...")


def build_bbox_condition(
    input_geom_col: str,
    other_bbox_col: str,
    input_bbox_col: str | None = None,
    input_has_native_geo: bool = False,
) -> str | None:
    """Build a bbox intersection condition for spatial join pre-filtering.

    Supports three modes:
    - Explicit bbox column (GeoParquet 1.x with bbox struct)
    - Native geometry bounds (GeoParquet 2.0 / parquet-geo-only)
    - No pre-filter (returns None)

    Args:
        input_geom_col: Name of the input geometry column
        other_bbox_col: Name of the other table's bbox column
        input_bbox_col: Name of the input bbox column (if available)
        input_has_native_geo: Whether input uses native Parquet geometry types
    """
    if input_bbox_col and other_bbox_col:
        return (
            f"(a.{input_bbox_col}.xmin <= b.{other_bbox_col}.xmax AND\n"
            f"        a.{input_bbox_col}.xmax >= b.{other_bbox_col}.xmin AND\n"
            f"        a.{input_bbox_col}.ymin <= b.{other_bbox_col}.ymax AND\n"
            f"        a.{input_bbox_col}.ymax >= b.{other_bbox_col}.ymin)"
        )

    if input_has_native_geo and other_bbox_col:
        return (
            f'(ST_XMin(a."{input_geom_col}") <= b.{other_bbox_col}.xmax AND\n'
            f'        ST_XMax(a."{input_geom_col}") >= b.{other_bbox_col}.xmin AND\n'
            f'        ST_YMin(a."{input_geom_col}") <= b.{other_bbox_col}.ymax AND\n'
            f'        ST_YMax(a."{input_geom_col}") >= b.{other_bbox_col}.ymin)'
        )

    return None


def build_join_clause(
    bbox_condition: str | None,
    other_geom_col: str,
    input_geom_col: str,
) -> str:
    """Build the JOIN ON clause combining bbox pre-filter and ST_Intersects."""
    intersects = f'ST_Intersects(b."{other_geom_col}", a."{input_geom_col}")'
    if bbox_condition:
        return f"ON {bbox_condition}\n        AND {intersects}"
    return f"ON {intersects}"


def build_spatial_join_query(
    input_url: str,
    other_subquery: str,
    select_clause: str,
    input_geom_col: str,
    other_geom_col: str,
    input_bbox_col: str | None = None,
    other_bbox_col: str | None = None,
    input_has_native_geo: bool = False,
    deduplicate: bool = True,
) -> str:
    """Build a spatial join query with optional bbox optimization and deduplication.

    Args:
        input_url: URL/path to input parquet file
        other_subquery: SQL subquery or table reference for the join target
        select_clause: SQL SELECT clause for columns from the join target
        input_geom_col: Name of the input geometry column
        other_geom_col: Name of the other table's geometry column
        input_bbox_col: Name of the input bbox column (if available)
        other_bbox_col: Name of the other table's bbox column (if available)
        input_has_native_geo: Whether input uses native Parquet geometry types
        deduplicate: Keep only the match with largest overlap area (default True)
    """
    bbox_condition = build_bbox_condition(
        input_geom_col=input_geom_col,
        other_bbox_col=other_bbox_col,
        input_bbox_col=input_bbox_col,
        input_has_native_geo=input_has_native_geo,
    )

    join_clause = build_join_clause(bbox_condition, other_geom_col, input_geom_col)

    if deduplicate:
        return f"""
    WITH _gpio_input AS (
        SELECT *, ROW_NUMBER() OVER () AS {_DEDUP_ROW_COL}
        FROM '{input_url}'
    )
    SELECT * EXCLUDE ({_DEDUP_ROW_COL}) FROM (
        SELECT
            a.*,
            {select_clause}
        FROM _gpio_input a
        LEFT JOIN {other_subquery} b
        {join_clause}
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY a.{_DEDUP_ROW_COL}
            ORDER BY ST_Area(ST_Intersection(b."{other_geom_col}", a."{input_geom_col}")) DESC NULLS LAST,
                HASH(b."{other_geom_col}") -- deterministic tiebreaker for zero-area intersections
        ) = 1
    )
"""

    return f"""
    SELECT
        a.*,
        {select_clause}
    FROM '{input_url}' a
    LEFT JOIN {other_subquery} b
    {join_clause}
"""
