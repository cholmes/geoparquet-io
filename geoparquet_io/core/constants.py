"""
Shared constants for geoparquet-io.

This module defines constants that are shared across multiple modules to ensure
consistency and make it easy to change values in one place.
"""

# Default column name for H3 cell IDs
DEFAULT_H3_COLUMN_NAME = "h3_cell"

# Default column name for A5 cell IDs
DEFAULT_A5_COLUMN_NAME = "a5_cell"

# Default column name for quadkey cells
DEFAULT_QUADKEY_COLUMN_NAME = "quadkey"

# Default resolution (zoom level) for quadkey generation
DEFAULT_QUADKEY_RESOLUTION = 13

# Default resolution for quadkey partitioning (prefix length)
DEFAULT_QUADKEY_PARTITION_RESOLUTION = 9

# Default column name for S2 cell IDs
DEFAULT_S2_COLUMN_NAME = "s2_cell"

# Default S2 level (comparable to H3 resolution 9, ~1.2 km² cells)
DEFAULT_S2_LEVEL = 13

# Default compression level for S2 temp file generation
DEFAULT_S2_COMPRESSION_LEVEL = 15

# Default target rows per partition for S2 auto-resolution
DEFAULT_S2_TARGET_ROWS = 100000

# Vecorel schema URLs
VECOREL_CORE_SCHEMA = "https://vecorel.org/specification/v0.1.0/schema.yaml"
VECOREL_ADMIN_SCHEMA = "https://vecorel.org/administrative-division-extension/v0.1.0/schema.yaml"
VECOREL_METRICS_SCHEMA = "https://vecorel.org/geometry-metrics-extension/v0.1.0/schema.yaml"
FIBOA_CORE_SCHEMA = "https://fiboa.org/specification/v0.3.0/schema.yaml"


def build_collection_metadata(
    schema_urls: list[str],
    existing_metadata: dict | None = None,
    collection_id: str = "default",
) -> dict[str, str]:
    """Build Parquet KV metadata with Vecorel collection schemas.

    Merges the given schema URLs with any existing collection metadata
    from the input file, deduplicating entries. Always includes the
    Vecorel core schema URL.

    The output follows the Vecorel GeoParquet encoding: the ``collection``
    Parquet metadata key holds a JSON object with at least ``schemas``
    (mapping collection IDs to lists of schema URLs) and ``collection``
    (the collection identifier string).

    Args:
        schema_urls: Extension schema URLs to include.
        existing_metadata: Raw Parquet KV metadata dict from the input file.
        collection_id: Collection identifier (default: "default").

    Returns:
        Dict with a "collection" key containing JSON-encoded metadata,
        ready to pass as extra_kv_metadata to write_parquet_with_metadata().
    """
    import json

    # Always include the vecorel core schema
    all_urls = [VECOREL_CORE_SCHEMA]
    for url in schema_urls:
        if url not in all_urls:
            all_urls.append(url)

    # Start from existing collection metadata if present
    collection_obj = {}
    if existing_metadata:
        raw = existing_metadata.get("collection") or existing_metadata.get(b"collection")
        if raw:
            try:
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8")
                collection_obj = json.loads(raw)
            except (json.JSONDecodeError, AttributeError):
                pass

    # Preserve existing collection ID if set
    if "collection" in collection_obj:
        collection_id = collection_obj["collection"]
    collection_obj["collection"] = collection_id

    # Merge schema URLs into the correct collection group
    existing_schemas = collection_obj.get("schemas", {})
    existing_urls = existing_schemas.get(collection_id, [])
    for url in all_urls:
        if url not in existing_urls:
            existing_urls.append(url)
    existing_schemas[collection_id] = existing_urls
    collection_obj["schemas"] = existing_schemas

    return {"collection": json.dumps(collection_obj)}


def ensure_vecorel_columns(parquet_file: str, verbose: bool = False) -> None:
    """Ensure a Parquet file has Vecorel-required columns with correct schema.

    - Adds a row-number 'id' column if one doesn't exist.
    - Sets 'id' and 'geometry' columns to non-nullable in the Parquet schema.

    Rewrites the file in place.
    """
    import os
    import tempfile

    from geoparquet_io.core.common import add_computed_column
    from geoparquet_io.core.duckdb_metadata import get_column_names
    from geoparquet_io.core.file_utils import safe_file_url

    url = safe_file_url(parquet_file, verbose=False)
    columns = get_column_names(url)

    # Add id column if missing
    if "id" not in columns:
        fd, temp_out = tempfile.mkstemp(suffix=".parquet")
        os.close(fd)
        os.unlink(temp_out)
        try:
            add_computed_column(
                parquet_file,
                temp_out,
                column_name="id",
                sql_expression="CAST(row_number() OVER () AS VARCHAR)",
            )
            os.replace(temp_out, parquet_file)
        finally:
            if os.path.exists(temp_out):
                os.unlink(temp_out)

    # Fix nullability on id and geometry columns
    _set_columns_non_nullable(parquet_file, ["id", "geometry"])


def _set_columns_non_nullable(parquet_file: str, column_names: list[str]) -> None:
    """Rewrite a Parquet file with specified columns marked non-nullable.

    Preserves all existing Parquet metadata (geo, collection, etc.).
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    pf = pq.ParquetFile(parquet_file)
    schema = pf.schema_arrow

    needs_fix = False
    new_fields = []
    for field in schema:
        if field.name in column_names and field.nullable:
            new_fields.append(field.with_nullable(False))
            needs_fix = True
        else:
            new_fields.append(field)

    if not needs_fix:
        return

    new_schema = pa.schema(new_fields, metadata=schema.metadata)
    table = pq.read_table(parquet_file)
    table = table.cast(new_schema)

    import os
    import tempfile

    fd, temp_out = tempfile.mkstemp(suffix=".parquet")
    os.close(fd)
    try:
        pq.write_table(table, temp_out, compression="ZSTD")
        os.replace(temp_out, parquet_file)
    finally:
        if os.path.exists(temp_out):
            os.unlink(temp_out)
