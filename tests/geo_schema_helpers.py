"""Test-support parser for Parquet native geo type annotations.

Lives under tests/ deliberately: production code reads native geo types
through DuckDB (core/duckdb_metadata.py), while the test suite and the
fixture generator verify written files by parsing the Parquet schema
string directly. Rehomed from core/metadata_utils.py when its last
production caller was deleted (#829, #833) so coverage and vulture
account for it as the test infrastructure it is.
"""

import json
import re
from typing import Any


def parse_geometry_type_from_schema(
    field_name: str, parquet_schema_str: str
) -> dict[str, Any] | None:
    """
    Parse geometry type details from Parquet schema string.

    According to the Parquet geospatial spec, the format is:
    field_name (Geometry(geom_type, coord_dimension, crs=..., ...))
    or
    field_name (Geography(geom_type, coord_dimension, crs=..., algorithm=...))

    Args:
        field_name: Name of the field to parse
        parquet_schema_str: Parquet schema string

    Returns:
        dict with 'geometry_type', 'coordinate_dimension', and 'crs', or None if not present
    """

    # Escape special regex characters in field name
    escaped_name = re.escape(field_name)

    # Pattern to match the full Geometry/Geography annotation
    # We need to capture everything inside Geometry(...) including nested structures
    pattern = rf"{escaped_name}\s+[^(]*\((Geometry|Geography)\((.*)\)\)"
    match = re.search(pattern, parquet_schema_str)

    if not match:
        return None

    params_str = match.group(2)  # Get the full parameters string

    result = {}

    # Parse CRS if present - look for crs={...} or crs="..."
    # CRS can be a complex JSON object, so we need to find the matching braces
    crs_match = re.search(r'crs=(\{.*?\}(?=\s*[,)])|"[^"]*"|\S+)', params_str)
    if crs_match:
        crs_value = crs_match.group(1)
        # Skip if CRS is empty (just a comma or closing paren after =)
        if crs_value and crs_value != "," and crs_value != ")":
            # Try to parse as JSON if it starts with {
            if crs_value.startswith("{"):
                try:
                    # Find the complete CRS object by counting braces
                    start_pos = params_str.find("crs={") + 4  # Position after "crs="
                    brace_count = 0
                    end_pos = start_pos
                    for i, char in enumerate(params_str[start_pos:], start=start_pos):
                        if char == "{":
                            brace_count += 1
                        elif char == "}":
                            brace_count -= 1
                            if brace_count == 0:
                                end_pos = i + 1
                                break

                    if end_pos > start_pos:
                        crs_json_str = params_str[start_pos:end_pos]
                        try:
                            result["crs"] = json.loads(crs_json_str)
                        except Exception:
                            result["crs"] = crs_json_str
                except Exception:
                    pass
            elif crs_value.startswith('"') and crs_value.endswith('"'):
                result["crs"] = crs_value.strip('"')
            else:
                result["crs"] = crs_value

    # Parse algorithm parameter (for Geography type) - planar or spherical
    algorithm_match = re.search(r"algorithm=(planar|spherical)", params_str)
    if algorithm_match:
        result["algorithm"] = algorithm_match.group(1)

    # Split by comma, but be careful about commas inside JSON objects
    # For simplicity, we'll look for positional parameters at the start
    # before any = signs
    parts = []
    depth = 0
    current_part = []

    for char in params_str:
        if char == "{":
            depth += 1
            current_part.append(char)
        elif char == "}":
            depth -= 1
            current_part.append(char)
        elif char == "," and depth == 0:
            parts.append("".join(current_part).strip())
            current_part = []
        else:
            current_part.append(char)

    if current_part:
        parts.append("".join(current_part).strip())

    # First parameter (if present and not a key=value pair) is geometry type
    # Valid types: Point, LineString, Polygon, MultiPoint, MultiLineString, MultiPolygon, GeometryCollection
    valid_geom_types = [
        "Point",
        "LineString",
        "Polygon",
        "MultiPoint",
        "MultiLineString",
        "MultiPolygon",
        "GeometryCollection",
    ]

    positional_params = []
    for part in parts:
        if "=" not in part:
            positional_params.append(part.strip())

    # First positional parameter is geometry type
    if len(positional_params) > 0:
        geom_type = positional_params[0]
        if geom_type in valid_geom_types:
            result["geometry_type"] = geom_type

    # Second positional parameter is coordinate dimension
    # Valid dimensions: XY, XYZ, XYM, XYZM
    valid_coord_dims = ["XY", "XYZ", "XYM", "XYZM"]

    if len(positional_params) > 1:
        coord_dim = positional_params[1]
        if coord_dim in valid_coord_dims:
            result["coordinate_dimension"] = coord_dim

    return result if result else None
