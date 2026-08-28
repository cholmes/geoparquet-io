# Sorting Data

The `sort` command reorders GeoParquet files for optimal performance and query efficiency.

## Sorting Methods

- **Hilbert curve** - Optimal spatial ordering using Hilbert space-filling curve
- **Column** - Sort by any column(s) for non-spatial ordering needs

## Hilbert Curve Ordering

=== "CLI"

    ```bash
    gpio sort hilbert input.parquet output.parquet
    ```

    <!-- doctest: skip="needs cloud credentials" -->
    ```bash
    # From HTTPS to S3
    gpio --aws-profile prod sort hilbert https://example.com/data.parquet s3://bucket/sorted.parquet
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    gpio.read('input.parquet').sort_hilbert().write('output.parquet')
    ```

    <!-- doctest: skip="needs cloud credentials" -->
    ```python
    # With upload to S3
    gpio.read('https://example.com/data.parquet') \
        .sort_hilbert() \
        .upload('s3://bucket/sorted.parquet', profile='prod')
    ```

Reorders rows using a [Hilbert space-filling curve](https://en.wikipedia.org/wiki/Hilbert_curve), which:

- Improves spatial locality
- Increases compression ratios
- Optimizes cloud-native access patterns
- Enhances query performance

!!! warning "GeoParquet version matters"
    Sorting only pays off if readers can *use* the resulting spatial locality. Without `--geoparquet-version`, the output keeps the input's version, so a v1.1 file stays v1.1 — and v1.1 has no native `geo_bbox` row group statistics. Either write v2.0 for native statistics, or add a `bbox` covering column that engines can push predicates down onto:

    ```bash
    # Native row group statistics (recommended)
    gpio sort hilbert input.parquet output.parquet --geoparquet-version 2.0

    # A bbox covering column instead (or as well — it also prunes pages within a row group)
    gpio sort hilbert input.parquet output.parquet --add-bbox
    ```

    `--add-bbox` writes the column *and* the `covering` metadata that points at it, at every output version. GeoParquet 2.0 keeps 1.1's optional bbox covering ([geoparquet#302](https://github.com/opengeospatial/geoparquet/pull/302)) precisely because the native statistics only prune whole row groups, while a covering column's page index prunes pages inside one.

## Options

<!-- doctest: menu -->
```bash
# Add bbox column if missing
gpio sort hilbert input.parquet output.parquet --add-bbox

# Custom compression
gpio sort hilbert input.parquet output.parquet --compression GZIP --compression-level 9

# Row group sizing
gpio sort hilbert input.parquet output.parquet --row-group-size-mb 256

# Verbose output
gpio sort hilbert input.parquet output.parquet --verbose
```

<!-- doctest: skip="filters on 'geom', a column the sample data does not have" -->
```bash
# Specify geometry column
gpio sort hilbert input.parquet output.parquet -g geom
```

## Compression Options

--8<-- "_includes/compression-options.md"

## Row Group Sizing

Control row group sizes for optimal performance:

<!-- doctest: menu -->
```bash
# Recommended for spatial filter pushdown (GeoParquet 2.0)
gpio sort hilbert input.parquet output.parquet --row-group-size 30000 --geoparquet-version 2.0

# Target size in MB/GB
gpio sort hilbert input.parquet output.parquet --row-group-size-mb 256MB
gpio sort hilbert input.parquet output.parquet --row-group-size-mb 1GB
```

!!! tip "Optimal row group size for spatial queries"
    For GeoParquet 2.0 or parquet-geo-only files with Hilbert sorting, use **10,000-50,000 rows per group**. Smaller row groups create tighter bounding boxes that enable more row group skipping during spatial queries. Benchmarks show 10k rows + Hilbert + v2.0 enables ~67% row group skipping vs 0% with large row groups.

## Column Ordering

Sort by any column(s) for non-spatial ordering needs:

=== "CLI"

    ```bash
    # Sort by a single column
    gpio sort column input.parquet output.parquet name
    ```

    <!-- doctest: skip="sorts on 'country', a column the sample data does not have" -->
    ```bash
    # Sort by multiple columns (comma-separated)
    gpio sort column input.parquet output.parquet country,city

    # Sort in descending order
    gpio sort column input.parquet output.parquet date --descending
    ```

=== "Python"

    <!-- doctest: skip="sorts on 'date', a column the sample data does not have" -->
    ```python
    import geoparquet_io as gpio
    from geoparquet_io.api import ops

    # Sort by a single column (fluent API)
    gpio.read('input.parquet').sort_column('name').write('output.parquet')

    # Sort in descending order
    gpio.read('input.parquet').sort_column('date', descending=True).write('output.parquet')

    # Multi-column sorting (requires ops API)
    table = gpio.read('input.parquet')
    sorted_arrow = ops.sort_column(table.to_arrow(), ['country', 'city'])
    gpio.Table(sorted_arrow).write('output.parquet')
    ```

!!! note "Multi-column sorting"
    `Table.sort_column()` accepts a single column. For multi-column sorting, use `ops.sort_column()` which accepts a list of column names.

Column sorting:

- Accepts one or more column names (comma-separated)
- Validates that columns exist before sorting
- Preserves all original columns and metadata
- Useful for time-series data or alphabetical ordering

## Output Format

The output file:

- Defaults to GeoParquet 1.1 spec (use `--geoparquet-version 2.0` for native spatial stats)
- Preserves CRS information
- Includes bbox covering metadata
- Uses optimal row group sizes

!!! note "Version options"
    Use `--geoparquet-version` to control the output format: `1.1` (default), `2.0` (recommended for spatial filter pushdown), or `parquet-geo-only` (Parquet native geo types without GeoParquet metadata).

## See Also

- [CLI Reference: sort](../cli/sort.md)
- [check spatial](check.md#spatial-ordering)
- [add bbox](add.md#bounding-boxes)
