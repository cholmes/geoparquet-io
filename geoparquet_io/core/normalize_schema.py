"""Normalize a GeoParquet file's schema for the tri-access layout (spec §1).

Rewrites the schema (not the data, geometry encoding, or CRS) so that:

* column names are **lowercase**;
* columns are ordered **attributes, then the geometry column, then bbox covering
  columns last** — so attributes + geom occupy a contiguous field-id block with
  no gaps (required by external Iceberg readers that prune on geom);
* every top-level column carries a contiguous ``PARQUET:field_id`` (1..N);
* an optional per-column ``description`` is attached.

The GeoParquet ``geo`` metadata (``primary_column`` and the bbox ``covering``
references) is updated to track any renamed columns. Geometry encoding and CRS
are deliberately untouched — those are governed by the existing
``--geoparquet-version`` machinery.
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from geoparquet_io.core.file_utils import handle_output_overwrite
from geoparquet_io.core.logging_config import configure_verbose, debug, success
from geoparquet_io.core.streaming import find_geometry_column_from_table

_BBOX_FIELDS = ("xmin", "ymin", "xmax", "ymax")


def _load_geo(table: pa.Table) -> dict | None:
    """Return the parsed GeoParquet ``geo`` metadata, or None if absent/invalid."""
    md = table.schema.metadata or {}
    if b"geo" not in md:
        return None
    try:
        parsed = json.loads(md[b"geo"].decode("utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _covering_bbox_columns(geo: dict | None, geom_col: str | None) -> list[str]:
    """Bbox columns referenced by the geometry column's ``covering``, in field order."""
    if not geo or not geom_col:
        return []
    covering = geo.get("columns", {}).get(geom_col, {}).get("covering", {}).get("bbox", {})
    cols: list[str] = []
    for field in _BBOX_FIELDS:
        ref = covering.get(field)
        if ref and ref[0] not in cols:
            cols.append(ref[0])
    return cols


def _struct_bbox_column(table: pa.Table, geom_col: str | None) -> list[str]:
    """A struct column whose fields include xmin/ymin/xmax/ymax, if any."""
    for field in table.schema:
        if field.name != geom_col and pa.types.is_struct(field.type):
            subfields = {f.name.lower() for f in field.type}
            if set(_BBOX_FIELDS) <= subfields:
                return [field.name]
    return []


def _top_level_bbox_columns(table: pa.Table) -> list[str]:
    """Top-level xmin/ymin/xmax/ymax columns (all four present), in canonical order."""
    by_lower = {c.lower(): c for c in table.column_names}
    if set(_BBOX_FIELDS) <= set(by_lower):
        return [by_lower[f] for f in _BBOX_FIELDS]
    return []


def _detect_bbox_columns(table: pa.Table, geo: dict | None, geom_col: str | None) -> list[str]:
    """Find the bbox covering columns via metadata, then a struct, then top-level cols."""
    return (
        _covering_bbox_columns(geo, geom_col)
        or _struct_bbox_column(table, geom_col)
        or _top_level_bbox_columns(table)
    )


def _column_order(table: pa.Table, geom_col: str | None, bbox_cols: list[str]) -> list[str]:
    """Order: attributes (original order), then geometry, then bbox columns last."""
    special = set(bbox_cols) | ({geom_col} if geom_col else set())
    attrs = [c for c in table.column_names if c not in special]
    geom = [geom_col] if geom_col else []
    return attrs + geom + bbox_cols


def _rename_map(order: list[str], lowercase: bool) -> dict[str, str]:
    """Map each column to its (optionally lowercased) name, erroring on collisions."""
    rename: dict[str, str] = {}
    used: dict[str, str] = {}
    for name in order:
        new = name.lower() if lowercase else name
        if new in used and used[new] != name:
            raise ValueError(
                f"Lowercase name collision: '{name}' and '{used[new]}' both map to '{new}'. "
                "Rename one before normalizing."
            )
        used[new] = name
        rename[name] = new
    return rename


def _field_metadata(field: pa.Field, field_id: int, description: str | None) -> dict:
    """Merge a contiguous PARQUET:field_id (and optional description) into field metadata."""
    md = dict(field.metadata or {})
    md[b"PARQUET:field_id"] = str(field_id).encode()
    if description is not None:
        md[b"description"] = description.encode()
    return md


def _rewrite_geo_metadata(geo: dict, rename: dict[str, str], geom_col: str | None) -> bytes:
    """Re-key ``primary_column``, ``columns``, and covering refs for renamed columns."""
    geo = json.loads(json.dumps(geo))  # deep copy
    if geom_col:
        geo["primary_column"] = rename.get(geom_col, geom_col)
    geo["columns"] = {rename.get(k, k): v for k, v in geo.get("columns", {}).items()}
    new_geom = rename.get(geom_col, geom_col) if geom_col else None
    covering = (
        geo.get("columns", {}).get(new_geom, {}).get("covering", {}).get("bbox")
        if new_geom
        else None
    )
    if covering:
        for field in _BBOX_FIELDS:
            ref = covering.get(field)
            if ref:
                ref[0] = rename.get(ref[0], ref[0])
    return json.dumps(geo).encode("utf-8")


def _resolve_geometry_column(
    table: pa.Table, geo: dict | None, geometry_column: str | None
) -> str | None:
    """Resolve the geometry column from the arg, geo metadata, or detection."""
    geom_col = (
        geometry_column
        or (geo or {}).get("primary_column")
        or find_geometry_column_from_table(table)
    )
    return geom_col if geom_col in table.column_names else None


def _normalized_fields(
    table: pa.Table, order: list[str], rename: dict[str, str], descriptions: dict[str, str]
) -> list[pa.Field]:
    """Build renamed fields carrying contiguous field-ids and optional descriptions."""
    return [
        table.schema.field(name)
        .with_name(rename[name])
        .with_metadata(
            _field_metadata(table.schema.field(name), idx + 1, descriptions.get(rename[name]))
        )
        for idx, name in enumerate(order)
    ]


def normalize_schema_table(
    table: pa.Table,
    lowercase: bool = True,
    descriptions: dict[str, str] | None = None,
    geometry_column: str | None = None,
) -> pa.Table:
    """Normalize an Arrow table's schema for the tri-access layout (Python API core).

    Args:
        table: Input GeoParquet-style table.
        lowercase: Lowercase all column names (default True).
        descriptions: Optional ``{column_name: description}`` (keyed by final name).
        geometry_column: Geometry column name (auto-detected from metadata if None).

    Returns:
        New table with reordered, renamed columns; contiguous ``PARQUET:field_id``;
        and updated geo metadata. Data and geometry encoding are unchanged.
    """
    descriptions = descriptions or {}
    geo = _load_geo(table)
    geom_col = _resolve_geometry_column(table, geo, geometry_column)
    bbox_cols = _detect_bbox_columns(table, geo, geom_col)

    order = _column_order(table, geom_col, bbox_cols)
    rename = _rename_map(order, lowercase)

    arrays = [table.column(name) for name in order]
    fields = _normalized_fields(table, order, rename, descriptions)
    metadata = {b"geo": _rewrite_geo_metadata(geo, rename, geom_col)} if geo is not None else None
    debug(f"normalized order: {[rename[n] for n in order]}")
    return pa.Table.from_arrays(arrays, schema=pa.schema(fields, metadata=metadata))


def _resolve_output(input_parquet: str, output_parquet: str | None) -> str:
    """Auto-name output as ``<stem>_normalized.parquet`` when unspecified."""
    if output_parquet is not None:
        return output_parquet
    src = Path(input_parquet)
    return str(src.parent / f"{src.stem}_normalized.parquet")


def normalize_schema(
    input_parquet: str,
    output_parquet: str | None = None,
    *,
    lowercase: bool = True,
    descriptions: dict[str, str] | None = None,
    compression: str = "ZSTD",
    row_group_rows: int | None = None,
    verbose: bool = False,
    overwrite: bool = False,
) -> None:
    """Normalize a GeoParquet file's schema for the tri-access layout.

    See :func:`normalize_schema_table`. Geometry encoding/CRS are preserved; the
    write is a straight PyArrow rewrite so field metadata (field-ids) survives.
    """
    configure_verbose(verbose)
    output_parquet = _resolve_output(input_parquet, output_parquet)
    handle_output_overwrite(output_parquet, overwrite, input_parquet)

    table = pq.read_table(input_parquet)
    result = normalize_schema_table(table, lowercase=lowercase, descriptions=descriptions)

    codec = compression.lower()
    codec = "none" if codec == "uncompressed" else codec
    pq.write_table(result, output_parquet, compression=codec, row_group_size=row_group_rows)
    success(f"Normalized schema ({result.num_columns} columns) to: {output_parquet}")
