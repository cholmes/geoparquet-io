"""Shared spatial join query construction for admin divisions and country codes."""


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
