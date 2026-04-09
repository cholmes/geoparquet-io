# Viewing Metadata

The `gpio inspect meta` command shows comprehensive metadata from GeoParquet files, including Parquet file metadata, GeoParquet 'geo' key metadata, and row group information.

## Quick Start

=== "CLI"

    ```bash
    # Show all metadata
    gpio inspect meta data.parquet

    # GeoParquet 'geo' key only
    gpio inspect meta data.parquet --geo

    # Parquet file metadata only
    gpio inspect meta data.parquet --parquet
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    table = gpio.read("data.parquet")

    # Get all metadata (geo + parquet)
    metadata = table.metadata(include_parquet_metadata=True)

    # Access specific metadata from the result
    geo_metadata = metadata["geo"]
    parquet_metadata = metadata["parquet"]
    ```

## Metadata Types

### GeoParquet Metadata (`--geo`)

Shows the GeoParquet 'geo' key metadata, which includes:

- **Primary geometry column** - The column containing the main geometry
- **Geometry types** - Point, LineString, Polygon, etc.
- **CRS** - Coordinate Reference System (e.g., EPSG:4326)
- **Bounding box** - Spatial extent of the data
- **Encoding** - How geometries are encoded (WKB, etc.)

```bash
gpio inspect meta data.parquet --geo
```

### Parquet File Metadata (`--parquet`)

Shows Parquet-level file metadata:

- **Created by** - Library/tool that created the file
- **Row count** - Total number of rows
- **Row groups** - Number of row groups and their sizes
- **Schema** - Column names and data types
- **Compression** - Compression codec used

```bash
gpio inspect meta data.parquet --parquet
```

### Parquet Geospatial Metadata (`--parquet-geo`)

Shows geospatial metadata stored in the Parquet footer using the newer GeoParquet 1.1 approach:

- **GEOMETRY/GEOGRAPHY logical types**
- **Per-column bounding boxes**
- **Geospatial statistics**

```bash
gpio inspect meta data.parquet --parquet-geo
```

### Row Group Information (`--row-groups`)

Shows detailed information about row groups:

```bash
# Show first row group (default)
gpio inspect meta data.parquet --row-groups 1

# Show first 5 row groups
gpio inspect meta data.parquet --row-groups 5
```

## JSON Output

Use `--json` for machine-readable output:

```bash
gpio inspect meta data.parquet --geo --json | jq '.primary_column'
```

## CLI Reference

See the [CLI Reference](../cli/meta.md) for complete options.
