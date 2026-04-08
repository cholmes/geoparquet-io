# Inspecting Files

The `inspect` command provides quick, human-readable summaries of GeoParquet files.

!!! tip "Need more detail?"
    For comprehensive metadata analysis including row group details and full schema information, use `gpio inspect --meta`.

## Basic Usage

=== "CLI"

    ```bash
    gpio inspect data.parquet

    # Or inspect remote file
    gpio inspect s3://bucket/data.parquet
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Print summary to stdout
    gpio.read('data.parquet').info()

    # Get as dictionary
    info = gpio.read('data.parquet').info(verbose=False)
    print(info['rows'], info['bounds'])
    ```

Shows:

- File size and row count
- CRS and bounding box
- Column schema with types

## Preview Data

=== "CLI"

    ```bash
    # First 10 rows (default when no value given)
    gpio inspect data.parquet --head

    # First 20 rows
    gpio inspect data.parquet --head 20

    # Last 10 rows (default when no value given)
    gpio inspect data.parquet --tail

    # Last 5 rows
    gpio inspect data.parquet --tail 5
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    table = gpio.read('data.parquet')

    # First 10 rows (default)
    first_10 = table.head()

    # First 20 rows
    first_20 = table.head(20)

    # Last 10 rows (default)
    last_10 = table.tail()

    # Last 5 rows
    last_5 = table.tail(5)

    # Chain with other operations
    preview = table.head(100).add_bbox()
    ```

## Statistics

=== "CLI"

    ```bash
    # Column statistics (nulls, min/max, unique counts)
    gpio inspect stats data.parquet

    # Combine with preview
    gpio inspect data.parquet --head --stats
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    table = gpio.read('data.parquet')

    # Get column statistics as dictionary
    stats = table.stats()

    # Access stats for a specific column
    print(stats['population']['min'])
    print(stats['population']['max'])
    print(stats['population']['nulls'])
    print(stats['population']['unique'])

    # Geometry columns have null counts only
    print(stats['geometry']['nulls'])
    ```

### Compression Ratios

`gpio inspect stats` also shows per-column compression ratios, including the compression codec, compressed and uncompressed sizes, and the compression ratio:

=== "CLI"

    ```bash
    gpio inspect stats data.parquet
    ```

=== "Python"

    ```python
    from geoparquet_io.api import ops

    stats = ops.compression_stats('data.parquet')
    for col in stats:
        print(f"{col['column']}: {col['compression']} ratio={col['ratio']:.2f}")
    ```

Output includes a "Compression Ratios" table showing which columns benefit most from compression.

## GeoParquet Metadata

=== "CLI"

    View the complete GeoParquet metadata from the 'geo' key:

    ```bash
    # Human-readable format
    gpio inspect data.parquet --geo-metadata

    # JSON format (exact metadata content)
    gpio inspect data.parquet --geo-metadata --json
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    table = gpio.read('data.parquet')

    # Get full metadata as dictionary
    meta = table.metadata()

    # Access specific metadata
    print(meta['geoparquet_version'])
    print(meta['geometry_column'])
    print(meta['crs'])
    print(meta['bounds'])

    # Get full geo metadata (from 'geo' key)
    geo_meta = meta.get('geo_metadata', {})
    print(geo_meta.get('columns', {}).get('geometry', {}))

    # Include raw Parquet schema metadata
    full_meta = table.metadata(include_parquet_metadata=True)
    ```

The human-readable format shows:
- GeoParquet version
- Primary geometry column
- Column-specific metadata (encoding, geometry types, CRS, bbox, covering, etc.)
- Simplified CRS display (use `--json` to see full PROJJSON definition)
- Default values for optional fields (CRS, orientation, edges, epoch, covering) when not present in the file

## Parquet File Metadata

View the complete Parquet file metadata (low-level details):

```bash
# Human-readable format
gpio inspect data.parquet --parquet-metadata

# JSON format (detailed metadata)
gpio inspect data.parquet --parquet-metadata --json
```

The metadata includes:
- Row group structure and sizes
- Column-level compression and encoding
- Physical storage details
- Schema information

## Parquet Geospatial Metadata

View geospatial metadata from the Parquet footer (column-level statistics and logical types):

```bash
# Human-readable format
gpio inspect data.parquet --parquet-geo-metadata

# JSON format
gpio inspect data.parquet --parquet-geo-metadata --json
```

This shows metadata from the Parquet specification for geospatial types:
- GEOMETRY and GEOGRAPHY logical type annotations
- Bounding box statistics (xmin, xmax, ymin, ymax, zmin, zmax, mmin, mmax)
- Geospatial types (WKB integer codes)
- Custom geospatial key-value metadata

**Note:** This is different from `--geo-metadata` which shows GeoParquet metadata from the 'geo' key.

## Bloom Filter Information

`gpio inspect meta` includes a bloom filter summary section showing which columns have bloom filters, coverage percentages, and total bloom filter bytes:

```bash
# Bloom filter info is shown automatically
gpio inspect meta data.parquet

# JSON output includes bloom filter details
gpio inspect meta data.parquet --json
```

Bloom filters enable efficient point lookups on low-cardinality columns. DuckDB 1.5+ automatically writes bloom filters when creating Parquet files.

## Per-Row-Group Geo Statistics

View bounding box statistics for each row group to verify spatial locality:

=== "CLI"

    ```bash
    # Show per-row-group geo_bbox statistics
    gpio inspect meta data.parquet --geo-stats

    # Limit to first 5 row groups
    gpio inspect meta data.parquet --geo-stats --row-groups 5

    # JSON output
    gpio inspect meta data.parquet --geo-stats --json
    ```

=== "Python"

    ```python
    from geoparquet_io.api.ops import get_row_group_geo_stats

    stats = get_row_group_geo_stats('hilbert_sorted.parquet')

    for rg in stats:
        print(f"RG {rg['row_group_id']}: {rg['num_rows']} rows, "
              f"bbox=[{rg['xmin']:.2f}, {rg['ymin']:.2f}, "
              f"{rg['xmax']:.2f}, {rg['ymax']:.2f}]")
    ```

This is useful for:

- Verifying spatial locality after Hilbert sorting
- Checking that row groups have tight bounding boxes
- Debugging spatial filter pushdown performance

**Stats Sources:** Works with native Parquet geo statistics (GeoParquet 2.0, parquet-geo-only) and bbox column statistics.

## Listing Layers

For multi-layer formats (GeoPackage, FileGDB), list available layers:

=== "CLI"

    ```bash
    # List layers in GeoPackage
    gpio inspect layers multi.gpkg

    # List layers in FileGDB
    gpio inspect layers data.gdb

    # JSON output for scripting
    gpio inspect layers multi.gpkg --json
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # List layers in a multi-layer file
    layers = gpio.list_layers('multi.gpkg')
    print(layers)  # ['buildings', 'roads', 'parcels']

    # Read a specific layer
    table = gpio.read('multi.gpkg', layer='buildings')
    ```

Returns layer names for files with 2+ layers. Single-layer files return nothing.

## JSON Output

```bash
# Machine-readable output
gpio inspect data.parquet --json

# Use with jq
gpio inspect data.parquet --json | jq '.file_info.rows'
```

## Inspecting Partitioned Data

=== "CLI"

    When inspecting a directory containing partitioned data, you can aggregate information across all files:

    ```bash
    # By default, inspects first file with a notice
    gpio inspect partitions/
    # Output: Inspecting first file (of 4 total). Use --check-all to aggregate all files.

    # Aggregate info from all files in partition
    gpio inspect partitions/ --check-all
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Read all partitions into a single Table
    table = gpio.read_partition('partitions/')

    # Get info about combined data
    table.info()

    # Access properties
    print(f"Total rows: {table.num_rows}")
    print(f"Bounds: {table.bounds}")
    ```

The `--check-all` option shows:

- Total file count and combined row count
- Total size across all files
- Combined bounding box (union of all file bounds)
- Schema consistency check
- Compression types used
- GeoParquet versions found
- Per-file breakdown (filename, rows, size)

```bash
# JSON output for scripted processing
gpio inspect partitions/ --check-all --json

# Markdown output for documentation
gpio inspect partitions/ --check-all --markdown
```

!!! note "Preview options not available with --check-all"
    The `--head`, `--tail`, and `--stats` options cannot be combined with `--check-all` since they apply to individual files.

## See Also

- [CLI Reference: inspect](../cli/inspect.md)
- [Checking Best Practices](check.md) - Validate GeoParquet files
