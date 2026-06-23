"""WKB-to-native GeoArrow conversion helpers for GeoParquet 1.1-geoarrow output.

Pure pyarrow/geoarrow utilities (no DuckDB, no Click). Used by the
arrow-streaming write strategy to emit nested-coordinate GeoArrow encoding.
"""

from __future__ import annotations

from geoparquet_io.core.crs_utils import is_default_crs
from geoparquet_io.core.logging_config import debug

# GeoParquet geometry_types base name -> geoarrow.pyarrow factory attribute.
# Maps GeoParquet base names (including Multi* types) to geoarrow factory attribute names.
# geometry_type_common handles promotion and unification.
_BASE_NAME_TO_FACTORY = {
    "point": "point",
    "linestring": "linestring",
    "polygon": "polygon",
    "multipoint": "multipoint",
    "multilinestring": "multilinestring",
    "multipolygon": "multipolygon",
}


def _normalize_base_name(geometry_type: str) -> str:
    """Strip Z/M dimension suffixes and lowercase: 'MultiPolygon Z' -> 'multipolygon'."""
    return geometry_type.split(" ")[0].strip().lower()


def determine_geoarrow_target_type(geometry_types: list[str], input_crs: dict | None = None):
    """Determine the single GeoArrow target type for a dataset.

    Args:
        geometry_types: GeoParquet geometry_types strings, e.g. ["Polygon", "MultiPolygon"].
        input_crs: PROJJSON dict to attach to the type (skipped when default CRS).

    Returns:
        (geoarrow_type, encoding_name). geoarrow_type is None and encoding_name
        is "WKB" when the set is empty or cannot be unified into one native type.
    """
    import geoarrow.pyarrow as ga

    base_names = {_normalize_base_name(g) for g in geometry_types if g}
    if not base_names or not base_names.issubset(_BASE_NAME_TO_FACTORY):
        return None, "WKB"

    types = []
    for name in base_names:
        factory = getattr(ga, _BASE_NAME_TO_FACTORY[name])
        types.append(factory())

    try:
        common = types[0] if len(types) == 1 else ga.geometry_type_common(types)
    except (ValueError, TypeError) as exc:  # incompatible mix (e.g. point + polygon)
        debug(f"GeoArrow types not unifiable ({base_names}); falling back to WKB: {exc}")
        return None, "WKB"

    if common.extension_name == "geoarrow.wkb":
        return None, "WKB"

    if input_crs and not is_default_crs(input_crs):
        common = common.with_crs(input_crs)

    encoding = common.extension_name.replace("geoarrow.", "")
    return common, encoding


def wkb_array_to_geoarrow(arr, target_type):
    """Convert a WKB/binary Arrow array to the given native GeoArrow target type."""
    import geoarrow.pyarrow as ga

    return ga.as_geoarrow(ga.as_wkb(arr), type=target_type)
