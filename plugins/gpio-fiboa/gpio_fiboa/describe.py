"""Describe fiboa compliance of a GeoParquet file."""

from __future__ import annotations

import json

import pyarrow.parquet as pq

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
        print(f"Cannot read file: {e}")
        return

    schema = pf.schema_arrow
    col_names = set(schema.names)
    metadata = schema.metadata or {}
    num_rows = pf.metadata.num_rows

    print(f"\nfiboa description: {input_file}")
    print("=" * 60)
    print(f"Rows: {num_rows:,}")

    # Vecorel metadata
    print("\nVecorel Metadata:")
    collection_meta = metadata.get(b"collection")
    if collection_meta:
        try:
            vecorel = json.loads(collection_meta)
            schema_urls = vecorel.get("schemas", {}).get("default", [])
            if schema_urls:
                print("  Extensions detected:")
                for url in schema_urls:
                    name = _match_extension_name(url)
                    print(f"    - {name}: {url}")
            else:
                print("  No schema URLs found in metadata")
        except json.JSONDecodeError:
            print("  Invalid JSON in vecorel metadata")
    else:
        print("  Not present")

    # fiboa column coverage
    print("\nfiboa Columns:")
    present = []
    missing = []
    for col, desc in FIBOA_COLUMNS.items():
        if col in col_names:
            present.append((col, desc))
        else:
            missing.append((col, desc))

    if present:
        for col, desc in present:
            print(f"  + {col:30s} {desc}")
    if missing:
        print(f"\n  Missing ({len(missing)}):")
        for col, desc in missing:
            print(f"  - {col:30s} {desc}")

    # Extra columns (not in fiboa spec)
    extra = [c for c in col_names if c not in FIBOA_COLUMNS]
    if extra and verbose:
        print(f"\n  Additional columns ({len(extra)}):")
        for col in sorted(extra):
            print(f"    {col}")

    # Summary stats for key columns
    if "metrics:area" in col_names or "metrics:perimeter" in col_names:
        print("\nMetrics Summary (first row group):")
        try:
            table = pf.read_row_group(0)
            import pyarrow.compute as pc

            for col_name in ["metrics:area", "metrics:perimeter"]:
                if col_name in col_names:
                    col = table.column(col_name)
                    non_null = pc.filter(col, pc.is_valid(col))
                    if len(non_null) > 0:
                        unit = "m²" if "area" in col_name else "m"
                        min_v = pc.min(non_null).as_py()
                        max_v = pc.max(non_null).as_py()
                        mean_v = pc.mean(non_null).as_py()
                        print(
                            f"  {col_name}: min={min_v:,.1f}{unit}, max={max_v:,.1f}{unit}, mean={mean_v:,.1f}{unit}"
                        )
        except Exception:
            pass

    if "admin:country_code" in col_names:
        print("\nAdmin Coverage (first row group):")
        try:
            table = pf.read_row_group(0)
            import pyarrow.compute as pc

            col = table.column("admin:country_code")
            non_null = pc.filter(col, pc.is_valid(col))
            unique = pc.unique(non_null)
            print(f"  Countries: {len(unique)} unique codes")
            if len(unique) <= 10:
                codes = sorted([str(v) for v in unique])
                print(f"  Codes: {', '.join(codes)}")
        except Exception:
            pass

    print()


def _match_extension_name(url: str) -> str:
    """Match a schema URL to a human-readable extension name."""
    for prefix, name in EXTENSION_URLS.items():
        if url.startswith(prefix):
            return name
    return "Unknown"
