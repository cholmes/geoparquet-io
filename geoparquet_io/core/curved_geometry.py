"""Detection of curved/non-linear geometry types GeoParquet cannot hold.

DuckDB's spatial extension parses only the seven linear Simple Features
types, and GeoParquet's ``geometry_types`` vocabulary is the same linear set,
so curved geometries (CircularString through MultiSurface) and the surface
family (PolyhedralSurface, TIN, Triangle) can neither be read nor represented.
The only correct conversion is linearizing them upstream. This module turns
DuckDB's opaque ``Unsupported geometry type in WKB`` into an error that names
the offending types and the remedy (issue #643).

duckdb-spatial has stated there are no plans to support circular geometry
types (duckdb/duckdb-spatial#510), so this is a permanent boundary, not a
version gap.
"""

from __future__ import annotations

import sqlite3
import struct
from pathlib import Path

#: ISO WKB type codes outside the linear Simple Features set (base code,
#: i.e. modulo the 1000/2000/3000 Z/M/ZM offsets).
NON_LINEAR_WKB_TYPES: dict[int, str] = {
    8: "CIRCULARSTRING",
    9: "COMPOUNDCURVE",
    10: "CURVEPOLYGON",
    11: "MULTICURVE",
    12: "MULTISURFACE",
    15: "POLYHEDRALSURFACE",
    16: "TIN",
    17: "TRIANGLE",
}

LINEARIZE_HINT = (
    "GeoParquet cannot represent curved geometries; linearize the source "
    "first, e.g. `ogr2ogr -nlt CONVERT_TO_LINEAR out.gpkg in.gpkg`."
)

# GPKG binary header: magic(2) version(1) flags(1) srs_id(4), then an envelope
# whose size depends on bits 1-3 of the flags byte, then ISO WKB.
_ENVELOPE_SIZES = {0: 0, 1: 32, 2: 48, 3: 48, 4: 64}


def find_non_linear_gpkg_types(path: str | Path, layer: str | None = None) -> list[str]:
    """Names of non-linear geometry types present in a GeoPackage.

    Scans the geometry blob headers of every feature table (or just ``layer``)
    with the standard library only. Returns a sorted list of type names, empty
    when all geometries are linear, and empty on any read problem — this is a
    diagnostic helper, never a gate.
    """
    found: set[str] = set()
    try:
        con = sqlite3.connect(f"file:{Path(path).resolve()}?mode=ro", uri=True)
        try:
            tables = con.execute(
                "SELECT c.table_name, g.column_name FROM gpkg_contents c "
                "JOIN gpkg_geometry_columns g USING (table_name) "
                "WHERE c.data_type = 'features'"
            ).fetchall()
            for table, col in tables:
                if layer and table != layer:
                    continue
                for (blob,) in con.execute(
                    f'SELECT "{col}" FROM "{table}" WHERE "{col}" IS NOT NULL'
                ):
                    if not blob or blob[:2] != b"GP" or len(blob) < 8:
                        continue
                    env = (blob[3] >> 1) & 0x07
                    off = 8 + _ENVELOPE_SIZES.get(env, 0)
                    if len(blob) < off + 5:
                        continue
                    endian = "<" if blob[off] == 1 else ">"
                    (wkb_type,) = struct.unpack_from(f"{endian}I", blob, off + 1)
                    name = NON_LINEAR_WKB_TYPES.get(wkb_type % 1000)
                    if name:
                        found.add(name)
        finally:
            con.close()
    except (sqlite3.Error, struct.error, OSError):
        return []
    return sorted(found)


def unsupported_wkb_error_message(input_file: str, layer: str | None, original_error: str) -> str:
    """Actionable message for DuckDB's 'Unsupported geometry type in WKB'."""
    path = Path(input_file)
    if path.suffix.lower() == ".gpkg" and path.exists():
        types = find_non_linear_gpkg_types(path, layer)
        if types:
            where = f"layer '{layer}'" if layer else path.name
            return (
                f"{where} contains non-linear geometries "
                f"({', '.join(types)}), which DuckDB cannot parse and "
                f"GeoParquet cannot represent. {LINEARIZE_HINT}"
            )
    return (
        f"The source contains non-linear geometries (curved or surface WKB "
        f"types), which DuckDB cannot parse and GeoParquet cannot represent. "
        f"{LINEARIZE_HINT} Original error: {original_error}"
    )
