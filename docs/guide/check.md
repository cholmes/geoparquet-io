# Checking Best Practices

The `check` commands validate GeoParquet files against [best practices](https://github.com/opengeospatial/geoparquet/pull/254/files).

## Run All Checks

=== "CLI"

    ```bash
    gpio check all myfile.parquet
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    table = gpio.read('myfile.parquet')
    result = table.check()

    if result.passed():
        print("All checks passed!")
    else:
        for failure in result.failures():
            print(f"Failed: {failure}")

    # Get full results as dictionary
    details = result.to_dict()
    ```

Runs all validation checks:

- Spatial ordering
- Compression settings
- Bbox structure and metadata
- Row group optimization
- Bloom filter detection
- GeoParquet v2.0 upgrade recommendation (for v1.1 files)
- Spec validation

## Individual Checks

### Spatial Ordering

=== "CLI"

    ```bash
    gpio check spatial myfile.parquet
    ```

=== "Python"

    ```python
    result = table.check_spatial()
    print(f"Spatially ordered: {result.passed()}")
    ```

Checks if data is spatially ordered. Spatially ordered data improves:

- Query performance (10-100x faster for spatial queries)
- Compression ratios
- Cloud access patterns

**Method Selection:**

- **GeoParquet 2.0+ files** (with bbox column): Uses fast bbox-stats method by analyzing row group metadata (~10-100x faster)
- **GeoParquet 1.x files** (no bbox column): Falls back to sampling method which analyzes actual geometry data

!!! tip "For faster spatial order checks"
    Add a bbox column to your file with `gpio add bbox` to enable the fast bbox-stats method.

**How it works:**

- **Bbox-stats method**: Checks if consecutive row groups have overlapping bounding boxes. Non-overlapping row groups indicate good spatial ordering. Passes if < 30% of row group pairs overlap.
- **Sampling method**: Compares average distance between consecutive features vs random feature pairs. Lower ratio indicates better spatial clustering. Passes if ratio < 0.5.

### Compression

=== "CLI"

    ```bash
    gpio check compression myfile.parquet
    ```

=== "Python"

    ```python
    result = table.check_compression()
    print(f"Compression optimal: {result.passed()}")
    ```

Validates geometry column compression settings.

### Bbox Structure

=== "CLI"

    ```bash
    gpio check bbox myfile.parquet
    ```

=== "Python"

    ```python
    result = table.check_bbox()
    if not result.passed():
        # Add bbox if missing
        table = table.add_bbox().add_bbox_metadata()
    ```

Verifies:

- Bbox column structure
- GeoParquet metadata version
- Bbox covering metadata

### Row Groups

=== "CLI"

    ```bash
    gpio check row-group myfile.parquet
    ```

=== "Python"

    ```python
    result = table.check_row_groups()
    for rec in result.recommendations():
        print(rec)
    ```

Checks row group size optimization for cloud-native access.

!!! tip "Spatial filter pushdown and row group sizing"
    For GeoParquet 2.0 or parquet-geo-only files with Hilbert sorting, row groups of 10,000-50,000 rows create tighter bounding boxes that enable more row group skipping during spatial queries.

### Optimization Check

=== "CLI"

    ```bash
    gpio check optimization myfile.parquet
    ```

=== "Python"

    ```python
    result = table.check_optimization()
    print(f"Score: {result.to_dict()['score']}/5")
    ```

Evaluates five factors affecting spatial query performance and returns a score from 0 to 5:

1. **Native Geo Types** - Uses native Parquet geo types (GeoParquet 2.0 or parquet-geo-only)
2. **Geo Bbox Stats** - Per-row-group geo bbox statistics present
3. **Spatial Sorting** - Data is spatially sorted (Hilbert or similar)
4. **Row Group Size** - Appropriate for file size (10k-50k rows for spatial pushdown)
5. **Compression** - ZSTD compression on geometry column

**Scoring levels:**

- `fully_optimized` (5/5) - All checks pass
- `partially_optimized` (3-4/5) - Some improvements possible
- `not_optimized` (0-2/5) - Significant improvements needed

### Spatial Filter Pushdown Readiness

The `gpio check spatial` command also reports spatial filter pushdown readiness when bbox data is available:

=== "CLI"

    ```bash
    gpio check spatial myfile.parquet
    ```

=== "Python"

    ```python
    result = table.check_spatial_pushdown()
    details = result.to_dict()
    print(f"Skip rate: {details['estimated_skip_rate']}")
    ```

Shows:

- **Row group count** and bbox coverage
- **Estimated skip rate** - percentage of row groups that can be skipped for representative spatial queries
- **Avg bbox area ratio** - how tight the row group bounding boxes are

!!! note "Requires bbox data"
    Pushdown readiness requires GeoParquet 2.0 native geo stats or a bbox column. For v1.1 files, add a bbox column with `gpio add bbox`.

### Bloom Filters

Bloom filter detection is included in `gpio check all` and `gpio inspect meta`:

=== "CLI"

    ```bash
    # Included automatically in check all
    gpio check all myfile.parquet
    ```

=== "Python"

    ```python
    result = table.check_bloom_filters()
    details = result.to_dict()
    ```

Reports which columns have bloom filters, coverage percentages, and total bloom filter bytes. DuckDB 1.5+ automatically writes bloom filters for low-cardinality columns.

### Spec Validation

=== "CLI"

    ```bash
    # Auto-detect version
    gpio check spec data.parquet

    # Validate against specific version
    gpio check spec data.parquet --geoparquet-version 1.1

    # JSON output for CI/CD
    gpio check spec data.parquet --json
    ```

=== "Python"

    ```python
    result = table.check_spec()
    if result.passed():
        print("Valid GeoParquet!")
    ```

Validates file structure and metadata against the GeoParquet specification:

- Supports GeoParquet 1.0, 1.1, 2.0, and Parquet native geo types
- Auto-detects version unless `--geoparquet-version` is specified
- Optional data validation against metadata claims

**Exit codes:**

- `0` - All checks passed
- `1` - One or more checks failed
- `2` - Warnings only (all required checks passed)

### STAC Validation

=== "CLI"

    ```bash
    gpio check stac output.json
    ```

=== "Python"

    ```python
    from geoparquet_io import validate_stac

    result = validate_stac('output.json')
    if result.passed():
        print("Valid STAC!")
    ```

Validates STAC Item or Collection JSON:

- STAC spec compliance
- Required fields
- Asset href resolution (local files)
- Best practices

## Options

=== "CLI"

    ```bash
    # Verbose output with details
    gpio check all myfile.parquet --verbose

    # Custom sampling for spatial check
    gpio check spatial myfile.parquet --random-sample-size 200 --limit-rows 1000000
    ```

=== "Python"

    ```python
    # Custom sampling for spatial check
    result = table.check_spatial(sample_size=200, limit_rows=1000000)
    ```

## Checking Partitioned Data

When checking a directory containing partitioned data, you can control how many files are checked:

```bash
# By default, checks only the first file
gpio check all partitions/
# Output: Checking first file (of 4 total). Use --check-all or --check-sample N for more.

# Check all files in the partition
gpio check all partitions/ --check-all

# Check a sample of files (first N files)
gpio check all partitions/ --check-sample 3
```

!!! note "--fix not available for partitions"
    The `--fix` option only works with single files. To fix issues in partitioned data, first consolidate with `gpio extract`, apply fixes, then re-partition if needed.

## See Also

- [CLI Reference: check](../cli/check.md)
- [add command](add.md) - Add spatial indices
- [sort command](sort.md)
