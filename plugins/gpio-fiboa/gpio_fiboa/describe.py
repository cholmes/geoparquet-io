"""Describe fiboa compliance of a GeoParquet file."""

from __future__ import annotations

import json

import click
import pyarrow.compute as pc
import pyarrow.parquet as pq

from geoparquet_io.core.exceptions import GeoParquetError
from geoparquet_io.core.logging_config import debug

FIBOA_COLUMNS = {
    "id": "Feature identifier",
    "collection": "Collection identifier",
    "geometry": "Field boundary geometry",
    "bbox": "Bounding box",
    "admin:country_code": "ISO 3166-1 alpha-2 country code",
    "admin:subdivision_code": "ISO 3166-2 subdivision code",
    "metrics:area": "Area in square meters",
    "metrics:perimeter": "Perimeter in meters",
    "determination:datetime": "When boundary was determined",
    "determination:method": "How boundary was determined",
    "determination:details": "Methodology details",
    "category": "Field category classification",
}

EXTENSION_URLS = {
    "https://fiboa.org/specification/": "fiboa Core",
    "https://vecorel.org/specification/": "Vecorel Core",
    "https://vecorel.org/administrative-division-extension/": "Admin Divisions",
    "https://vecorel.org/geometry-metrics-extension/": "Geometry Metrics",
}


def describe_fiboa(input_file: str, verbose: bool = False) -> None:
    """Describe fiboa-specific properties of a GeoParquet file."""
    try:
        pf = pq.ParquetFile(input_file)
    except Exception as e:
        raise GeoParquetError(f"Cannot read file: {input_file}: {e}") from e

    schema = pf.schema_arrow
    col_names = set(schema.names)
    metadata = schema.metadata or {}
    num_rows = pf.metadata.num_rows

    click.echo(f"\nfiboa description: {input_file}")
    click.echo("=" * 60)
    click.echo(f"Rows: {num_rows:,}")

    # Vecorel metadata
    click.echo("\nVecorel Metadata:")
    collection_meta = metadata.get(b"collection")
    if collection_meta:
        try:
            vecorel = json.loads(collection_meta)
            collection_id = vecorel.get("collection", "default")
            schemas = vecorel.get("schemas", {})
            schema_urls = schemas.get(collection_id, [])
            if not schema_urls and collection_id != "default":
                schema_urls = schemas.get("default", [])
            if schema_urls:
                click.echo("  Extensions detected:")
                for url in schema_urls:
                    name = _match_extension_name(url)
                    click.echo(f"    - {name}: {url}")
            else:
                click.echo("  No schema URLs found in metadata")
        except json.JSONDecodeError:
            click.echo(click.style("  Invalid JSON in vecorel metadata", fg="yellow"))
    else:
        click.echo("  Not present")

    # fiboa column coverage
    click.echo("\nfiboa Columns:")
    present = []
    missing = []
    for col, desc in FIBOA_COLUMNS.items():
        if col in col_names:
            present.append((col, desc))
        else:
            missing.append((col, desc))

    if present:
        for col, desc in present:
            click.echo(click.style(f"  + {col:30s} {desc}", fg="green"))
    if missing:
        click.echo(f"\n  Missing ({len(missing)}):")
        for col, desc in missing:
            click.echo(f"  - {col:30s} {desc}")

    # Extra columns (not in fiboa spec)
    if verbose:
        extra = [c for c in col_names if c not in FIBOA_COLUMNS]
        if extra:
            click.echo(f"\n  Additional columns ({len(extra)}):")
            for col in sorted(extra):
                click.echo(f"    {col}")

    # Read first row group once for stats, projecting only the stat columns
    # (avoids decoding geometry and other unused columns into memory).
    stat_cols = [
        c for c in ("metrics:area", "metrics:perimeter", "admin:country_code") if c in col_names
    ]
    rg_table = None
    if stat_cols:
        try:
            rg_table = pf.read_row_group(0, columns=stat_cols)
        except Exception as e:
            debug(f"Could not read row group: {e}")

    if rg_table is not None and ("metrics:area" in col_names or "metrics:perimeter" in col_names):
        click.echo("\nMetrics Summary (first row group):")
        try:
            for col_name in ["metrics:area", "metrics:perimeter"]:
                if col_name in col_names:
                    col = rg_table.column(col_name)
                    non_null = pc.filter(col, pc.is_valid(col))
                    if len(non_null) > 0:
                        unit = "m²" if "area" in col_name else "m"
                        min_v = pc.min(non_null).as_py()
                        max_v = pc.max(non_null).as_py()
                        mean_v = pc.mean(non_null).as_py()
                        click.echo(
                            f"  {col_name}: min={min_v:,.1f}{unit}, "
                            f"max={max_v:,.1f}{unit}, mean={mean_v:,.1f}{unit}"
                        )
        except Exception as e:
            debug(f"Could not read metrics summary: {e}")

    if rg_table is not None and "admin:country_code" in col_names:
        click.echo("\nAdmin Coverage (first row group):")
        try:
            col = rg_table.column("admin:country_code")
            non_null = pc.filter(col, pc.is_valid(col))
            unique = pc.unique(non_null)
            click.echo(f"  Countries: {len(unique)} unique codes")
            if len(unique) <= 10:
                codes = sorted([str(v) for v in unique])
                click.echo(f"  Codes: {', '.join(codes)}")
        except Exception as e:
            debug(f"Could not read admin coverage: {e}")

    click.echo("")


def _match_extension_name(url: str) -> str:
    """Match a schema URL to a human-readable extension name."""
    for prefix, name in EXTENSION_URLS.items():
        if url.startswith(prefix):
            return name
    return "Unknown"
