# Sorting Data

The `sort` command reorders GeoParquet files for optimal performance and query efficiency.

## Sorting Methods

- **Hilbert curve** - Optimal spatial ordering using Hilbert space-filling curve
- **Sort-Tile-Recursive (STR)** - Snake through X strips, each sorted on Y
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

    # row_group_rows is what the CLI defaults to; the Python API does not
    # apply it for you (see "Optimal row group size for spatial queries" below).
    gpio.read('input.parquet').sort_hilbert().write('output.parquet', row_group_rows=50000)
    ```

    <!-- doctest: skip="needs cloud credentials" -->
    ```python
    # With upload to S3
    gpio.read('https://example.com/data.parquet') \
        .sort_hilbert() \
        .upload('s3://bucket/sorted.parquet', row_group_rows=50000, profile='prod')
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
    gpio sort hilbert input.parquet output-bbox.parquet --add-bbox
    ```

    `--add-bbox` writes the column *and* the `covering` metadata that points at it, because gpio computed that column from the geometry here and can vouch for it. The `covering` key is not part of the GeoParquet 2.0 specification text — it was introduced in 1.1 and removed in 2.0 in favour of the native statistics. 2.0 readers must tolerate unknown fields, so a covering stays legal to carry, and [geoparquet#302](https://github.com/opengeospatial/geoparquet/pull/302) *proposes* reinstating it as an option (still open at time of writing). The motivation is real either way: native statistics prune whole row groups, while a bbox column's page index also prunes pages within one.

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
    Every `gpio sort` subcommand's CLI defaults to **50,000 rows per group** - the top of the 10,000-50,000 band that suits GeoParquet 2.0 or parquet-geo-only files with Hilbert sorting. Smaller row groups create tighter bounding boxes that enable more row group skipping during spatial queries. Benchmarks show 10k rows + Hilbert + v2.0 enables ~67% row group skipping vs 0% with large row groups, so pass a smaller `--row-group-size` when query selectivity matters more than file size.

    The writer rounds a row-group target up to a multiple of 2048, so the 50,000 default lands as 51,200-row groups. The default applies to the sort commands only; other write paths (`convert`, `add`, `partition`) leave the choice to the Parquet writer unless you pass `--row-group-size` yourself.

    **The Python API does not apply it.** `Table.write()` hands the writer whatever `row_group_rows` you give it, and `None` means the writer's own default (122,880 rows for DuckDB-backed writes) - so `gpio.read(...).sort_hilbert().write(out)` is *not* the equivalent of `gpio sort hilbert in out`. Pass `row_group_rows=50000` explicitly, as the Python examples in this guide do.

## Sort-Tile-Recursive Ordering

STR is an alternative spatial ordering. It sorts geometry bounding-box centers
into X strips, sorts each strip on Y, and alternates the Y direction between
strips so that neighbouring strips stay close.

=== "CLI"

    ```bash
    gpio sort str input.parquet output.parquet --row-group-size 50000
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    gpio.read('input.parquet') \
        .sort_str(tile_size=50000) \
        .write('output.parquet', row_group_rows=50000)
    ```

### What `--row-group-size` does here

`--row-group-size` does two separate things, and only one of them is exact:

- It is the writer's row-group target, as it is for every other gpio command.
- It selects how many X strips STR builds, as
  `ceil(sqrt(num_rows / row-group-size))`.

The second use is coarse. The strip count is a rounded square root, so nearby
values collapse onto the same layout: on 20,000 points, `--row-group-size 800`
and `--row-group-size 1000` produce a byte-identical ordering, as do 1,500 and
2,000. STR does not pack rows into row-group-sized tiles either - within a
strip, rows are simply sorted on Y.

Strips and row groups do not line up in general. The writer rounds the
row-group size up to a multiple of 2048, so `--row-group-size 100000` writes
100,352-row groups; strips are a whole number of `--row-group-size` rows, which
means they land on row-group boundaries only when you pass a multiple of 2048
(for example `--row-group-size 102400`).

Left unset, both uses take the sort default of 50,000 rows, so the strip size
and the row-group target at least agree with each other - though 50,000 is not
a multiple of 2048, so the groups land at 51,200 rows and the two still do not
line up exactly. Pass a multiple of 2048 (for example `--row-group-size 51200`)
when you want them to. With `--row-group-size-mb`, STR falls back to 50,000
rows per tile, because the row count of a byte-sized group is not known before
writing.

### How much does it help?

Modestly, and it depends on the data. On 2 million uniformly distributed points
written with `--row-group-size 100000`, STR's mean row-group bounding-box area
was 3,846 square degrees against Hilbert's 4,426 - about 13% tighter. The
[benchmark linked from the design presentation](https://github.com/Kanahiro/spatial-sort-benchmark)
reports lower row-group bbox overlap and fewer candidate row groups than
Hilbert on its 30-million-row POI dataset. Results depend on the dataset, so
Hilbert remains a good general default.

Like Hilbert sorting, STR places empty and NULL geometries at the end and can
write GeoParquet 2.0 native row-group statistics or add a bbox covering:

```bash
gpio sort str input.parquet output.parquet \
  --row-group-size 50000 \
  --geoparquet-version 2.0

gpio sort str input.parquet output-bbox.parquet --add-bbox
```

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

    # Sort by a single column (fluent API). row_group_rows matches the CLI
    # default; the Python API does not apply it for you.
    gpio.read('input.parquet').sort_column('name').write('output.parquet', row_group_rows=50000)

    # Sort in descending order
    gpio.read('input.parquet') \
        .sort_column('date', descending=True) \
        .write('output.parquet', row_group_rows=50000)

    # Multi-column sorting (requires ops API)
    table = gpio.read('input.parquet')
    sorted_arrow = ops.sort_column(table.to_arrow(), ['country', 'city'])
    gpio.Table(sorted_arrow).write('output.parquet', row_group_rows=50000)
    ```

!!! note "Multi-column sorting"
    `Table.sort_column()` accepts a single column. For multi-column sorting, use `ops.sort_column()` which accepts a list of column names.

Column sorting:

- Accepts one or more column names (comma-separated)
- Validates that columns exist before sorting
- Preserves all original columns and metadata
- Useful for time-series data or alphabetical ordering

## Multi-File Input

`sort column` and `sort quadkey` read a whole dataset — a directory of GeoParquet
files, or a quoted glob — and write one sorted file:

=== "CLI"

    <!-- doctest: skip="needs a directory of parquet files the sample data does not have" -->
    ```bash
    # Every .parquet file in the directory
    gpio sort column parts/ sorted.parquet name

    # Or a glob. Quote it, or the shell expands it before gpio sees it
    gpio sort quadkey 'parts/*.parquet' sorted.parquet
    ```

=== "Python"

    <!-- doctest: skip="needs a directory of parquet files the sample data does not have" -->
    ```python
    import geoparquet_io as gpio

    gpio.read_partition('parts/') \
        .sort_column('name') \
        .write('sorted.parquet', row_group_rows=50000)
    ```

The output's `bbox` and `geometry_types` are recomputed over everything written,
rather than carried from the first file — a merged extent that under-covered the
result would make conformant readers skip data.

`sort hilbert` and `sort str` take a single file only; they stop with a message
pointing at `gpio extract` to consolidate first.

## Output Format

The output file:

- Defaults to GeoParquet 1.1 spec (use `--geoparquet-version 2.0` for native spatial stats)
- Carries a non-default CRS through unchanged
- Includes bbox covering metadata
- Uses optimal row group sizes

!!! note "An explicit default CRS is normalized, not preserved"
    GeoParquet writes its default CRS (OGC:CRS84, equivalently EPSG:4326) by
    *omitting* the `crs` key. An input that spells that default out therefore
    comes back without the key: the coordinates and their meaning are unchanged,
    but a byte-for-byte diff of the `geo` metadata will show the key gone. Every
    gpio write path that rebuilds the `geo` block does this, not just `sort`
    (a metadata-only rewrite like `add bbox-metadata` carries the block as-is).
    Run with `--verbose` to see a
    note when it happens, and use `gpio inspect meta` to confirm the result.

!!! note "Version options"
    Use `--geoparquet-version` to control the output format: `1.1` (default), `2.0` (recommended for spatial filter pushdown), or `parquet-geo-only` (Parquet native geo types without GeoParquet metadata).

## See Also

- [CLI Reference: sort](../cli/sort.md)
- [check spatial](check.md#spatial-ordering)
- [add bbox](add.md#bounding-boxes)
