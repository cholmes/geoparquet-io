# Adding Spatial Indices

The `add` commands enhance GeoParquet files with spatial indices and metadata.

## Bounding Boxes

Add precomputed bounding boxes for faster spatial queries:

=== "CLI"

    ```bash
    gpio add bbox input.parquet output.parquet

    # Works with remote files
    gpio add bbox s3://bucket/input.parquet s3://bucket/output.parquet --aws-profile prod
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    gpio.read('input.parquet').add_bbox().write('output.parquet')

    # Custom column name
    gpio.read('input.parquet').add_bbox(column_name='bounds').write('output.parquet')
    ```

Creates a struct column with `{xmin, ymin, xmax, ymax}` for each feature. Bbox covering metadata is automatically added to comply with GeoParquet 1.1 spec.

### Existing Bbox Detection

The command automatically checks for existing bbox columns:

- **If bbox exists with metadata**: Informs you and exits successfully (no action needed)
- **If bbox exists without metadata**: Suggests using `gpio add bbox-metadata` instead
- **Use `--force`**: Replace existing bbox column with a freshly computed one

```bash
# Check and skip if bbox already exists
gpio add bbox input.parquet output.parquet

# Force replace existing bbox
gpio add bbox input.parquet output.parquet --force
```

**Options:**

```bash
# Custom column name
gpio add bbox input.parquet output.parquet --bbox-name bounds

# Force replace existing bbox
gpio add bbox input.parquet output.parquet --force

# With compression settings
gpio add bbox input.parquet output.parquet --compression ZSTD --compression-level 15

# Dry run (preview SQL)
gpio add bbox input.parquet output.parquet --dry-run
```

### Add Bbox Metadata Only

If your file already has a bbox column but lacks covering metadata (e.g., from external tools):

=== "CLI"

    ```bash
    gpio add bbox-metadata myfile.parquet
    ```

    This modifies the file in-place to add only the metadata, without creating a new file.

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Add bbox column, then add covering metadata
    table = gpio.read('input.parquet')
    table_with_bbox = table.add_bbox().add_bbox_metadata()
    table_with_bbox.write('output.parquet')

    # Or just add metadata if bbox column already exists
    table = gpio.read('file_with_bbox.parquet')
    table.add_bbox_metadata().write('output.parquet')
    ```

## H3 Hexagonal Cells

Add [H3](https://h3geo.org/) hexagonal cell IDs based on geometry centroids:

=== "CLI"

    ```bash
    gpio add h3 input.parquet output.parquet --resolution 9

    # From HTTPS to S3
    gpio add h3 https://example.com/data.parquet s3://bucket/indexed.parquet --resolution 9
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    gpio.read('input.parquet').add_h3(resolution=9).write('output.parquet')

    # Custom column name
    gpio.read('input.parquet').add_h3(column_name='h3_index', resolution=13).write('output.parquet')
    ```

**Resolution guide:**

--8<-- "_includes/h3-resolutions.md"

**Options:**

```bash
# Custom column name
gpio add h3 input.parquet output.parquet --h3-name h3_index

# Different resolution
gpio add h3 input.parquet output.parquet --resolution 13

# With row group sizing
gpio add h3 input.parquet output.parquet --row-group-size-mb 256MB
```

## S2 Spherical Cells

Add [S2](https://s2geometry.io/) spherical cell IDs based on geometry centroids:

=== "CLI"

    ```bash
    gpio add s2 input.parquet output.parquet --level 13

    # From HTTPS to S3
    gpio add s2 https://example.com/data.parquet s3://bucket/indexed.parquet --level 13
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    gpio.read('input.parquet').add_s2(level=13).write('output.parquet')

    # Custom column name
    gpio.read('input.parquet').add_s2(column_name='s2_index', level=18).write('output.parquet')
    ```

S2 uses Google's Spherical Geometry library which divides the Earth's surface into a hierarchy of cells using quadtree subdivision. Unlike H3's hexagonal grid, S2 cells are variable quads that provide hierarchical spatial indexing.

**Level guide:**

--8<-- "_includes/s2-levels.md"

**Options:**

```bash
# Custom column name
gpio add s2 input.parquet output.parquet --s2-name s2_index

# Different level
gpio add s2 input.parquet output.parquet --level 18

# With row group sizing
gpio add s2 input.parquet output.parquet --row-group-size-mb 256MB
```

### Technical Details

S2 cell IDs are computed using DuckDB's geography extension:

```sql
s2_cell_token(
    s2_cell_parent(
        s2_cellfromlonlat(
            ST_X(ST_Centroid(geometry)),
            ST_Y(ST_Centroid(geometry))
        ),
        level
    )
)
```

- **s2_cellfromlonlat**: Converts lon/lat to S2 cell at maximum precision (level 30)
- **s2_cell_parent**: Gets parent cell at desired level
- **s2_cell_token**: Converts to hex token string for portability

Cell IDs are stored as hex strings (e.g., `"89c25901"`) rather than integers for
maximum portability across systems.

## A5 Cells

Add [A5](https://a5geo.org/) spatial cell IDs based on geometry centroids.

=== "CLI"

    ```bash
    gpio add a5 input.parquet output.parquet --resolution 15

    # From HTTPS to S3
    gpio add a5 https://example.com/data.parquet s3://bucket/indexed.parquet --resolution 15
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    gpio.read('input.parquet').add_a5(resolution=15).write('output.parquet')

    # Custom column name
    gpio.read('input.parquet').add_a5(column_name='a5_index', resolution=12).write('output.parquet')
    ```

**Options:**

```bash
# Custom column name
gpio add a5 input.parquet output.parquet --a5-name a5_index

# Different resolution
gpio add a5 input.parquet output.parquet --resolution 12

# With row group sizing
gpio add a5 input.parquet output.parquet --row-group-size-mb 256MB
```

## KD-Tree Partitions

Add balanced spatial partition IDs using KD-tree:

=== "CLI"

    ```bash
    # Auto-select partitions (default: ~120k rows each)
    gpio add kdtree input.parquet output.parquet

    # Explicit partition count (must be power of 2)
    gpio add kdtree input.parquet output.parquet --partitions 32

    # Exact mode (deterministic but slower)
    gpio add kdtree input.parquet output.parquet --partitions 16 --exact
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Add kdtree column with default settings (9 iterations = 512 partitions)
    gpio.read('input.parquet').add_kdtree().write('output.parquet')

    # Custom column name and iterations
    gpio.read('input.parquet').add_kdtree(
        column_name='partition_id',
        iterations=5  # 2^5 = 32 partitions
    ).write('output.parquet')
    ```

**Auto mode** (default):
- Targets ~120k rows per partition
- Uses approximate computation (O(n))
- Fast on large datasets

**Explicit mode**:
- Specify partition count (2, 4, 8, 16, 32, ...)
- Control granularity

**Exact vs Approximate**:
- Approximate: O(n), samples 100k points
- Exact: O(n × log₂(partitions)), deterministic

**Options:**

```bash
# Custom target rows per partition
gpio add kdtree input.parquet output.parquet --auto 200000

# Custom sample size for approximate mode
gpio add kdtree input.parquet output.parquet --approx 200000

# Track progress
gpio add kdtree input.parquet output.parquet --verbose
```

## Administrative Divisions

Add administrative division columns via spatial join with remote boundaries datasets:

### How It Works

Performs spatial intersection between your data and remote admin boundaries to add admin division columns. Uses efficient spatial extent filtering to query only relevant boundaries from remote datasets.

### Quick Start

=== "CLI"

    ```bash
    # Add all GAUL levels (continent, country, department)
    gpio add admin-divisions input.parquet output.parquet --dataset gaul

    # Preview SQL before execution
    gpio add admin-divisions input.parquet output.parquet --dataset gaul --dry-run
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Add country codes using Overture dataset
    table = gpio.read('input.parquet')
    enriched = table.add_admin_divisions(
        dataset='overture',
        levels=['country']
    )
    enriched.write('output.parquet')
    ```

### Multi-Level Admin Divisions

Add multiple hierarchical administrative levels:

=== "CLI"

    ```bash
    # Add all GAUL levels (adds admin:continent, admin:country, admin:department)
    gpio add admin-divisions buildings.parquet output.parquet --dataset gaul

    # Add specific levels only
    gpio add admin-divisions buildings.parquet output.parquet --dataset gaul \
      --levels continent,country

    # Use Overture Maps dataset
    gpio add admin-divisions buildings.parquet output.parquet --dataset overture \
      --levels country,region
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Add multiple levels
    table = gpio.read('buildings.parquet')
    enriched = table.add_admin_divisions(
        dataset='gaul',
        levels=['continent', 'country', 'department']
    )
    enriched.write('output.parquet')

    # With country filter for faster processing
    enriched = table.add_admin_divisions(
        dataset='overture',
        levels=['country', 'region'],
        country_filter='US'
    )
    ```

### Datasets

--8<-- "_includes/admin-datasets.md"

### Caching

Admin datasets (GAUL, Overture) are automatically cached locally on first use:

- **First run**: Downloads and caches the full dataset (~5-50MB depending on dataset)
- **Subsequent runs**: Uses cached version (instant startup)
- **Cache location**: `~/.geoparquet-io/cache/admin/`
- **Warning**: Shown if cache is older than 6 months

**Cache management options:**

=== "CLI"

    ```bash
    # Skip cache and use remote directly
    gpio add admin-divisions input.parquet output.parquet --dataset gaul --no-cache

    # Clear all cached datasets (prompts for confirmation)
    gpio add admin-divisions input.parquet output.parquet --dataset gaul --clear-cache
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio
    from geoparquet_io.core.admin_datasets import get_cache_dir, clear_cache

    # Default: uses cached datasets automatically
    gpio.read('input.parquet').add_admin_divisions(
        dataset='overture',
        levels=['country', 'admin1']
    ).write('output.parquet')

    # Check cache location (~/.geoparquet-io/cache/admin/)
    cache_dir = get_cache_dir()
    print(f"Cache location: {cache_dir}")

    # Clear all cached admin datasets
    result = clear_cache(confirm=True)
    print(f"Cleared {result['files_deleted']} files, {result['bytes_freed'] / 1024 / 1024:.2f} MB freed")
    ```

!!! tip "When to clear cache"
    Clear your cache when you need fresh admin boundary data, such as after a
    new Overture Maps release. The cache files are named with version numbers
    (e.g., `gaul-2024-12-19.parquet`, `overture-2025-10-22.0.parquet`).

## Common Options

All `add` commands support:

--8<-- "_includes/common-cli-options.md"

```bash
--add-bbox         # Auto-add bbox if missing (some commands)
```

## See Also

- [CLI Reference: add](../cli/add.md)
- [partition command](partition.md)
- [sort command](sort.md)
