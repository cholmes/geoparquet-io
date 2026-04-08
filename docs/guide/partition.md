# Partitioning Files

The `partition` commands split GeoParquet files into separate files based on column values or spatial indices.

**Smart Analysis**: All partition commands automatically analyze your strategy before execution, providing statistics and recommendations.

## Auto-Resolution Mode

All spatial partitioning commands (H3, S2, A5, Quadkey) support **automatic resolution calculation** using the `--auto` flag. This eliminates the need to manually specify resolution levels by calculating the optimal value based on your data.

### How It Works

Auto-resolution analyzes your dataset and calculates the optimal spatial index resolution to achieve your target partition size:

1. Counts total rows in your input file
2. Calculates how many partitions are needed to achieve `--target-rows` per partition
3. Selects the resolution that produces approximately that many partitions
4. Respects `--max-partitions` as an upper bound

### Common Options

| Option | Default | Description |
|--------|---------|-------------|
| `--auto` | off | Enable auto-resolution calculation |
| `--target-rows` | 100,000 | Target rows per partition |
| `--max-partitions` | 10,000 | Maximum partitions to create |

### Quick Examples

```bash
# H3 with ~100K rows per partition (default)
gpio partition h3 input.parquet output/ --auto

# S2 with ~50K rows per partition
gpio partition s2 input.parquet output/ --auto --target-rows 50000

# Quadkey with partition limit
gpio partition quadkey input.parquet output/ --auto --max-partitions 1000

# A5 with preview
gpio partition a5 input.parquet --auto --preview
```

### Resolution Formulas

The auto-resolution calculation uses these cell count formulas:

| Index | Formula | Notes |
|-------|---------|-------|
| **H3** | `cells ≈ 122 × 7^resolution` | Hexagonal cells |
| **S2** | `cells = 6 × 4^level` | Spherical cells |
| **A5** | `cells = 6 × 4^resolution` | Equal-area cells |
| **Quadkey** | `tiles = 4^zoom` | Square tiles |

## By String Column

Partition by string column values or prefixes:

=== "CLI"

    ```bash
    # Preview partitions
    gpio partition string input.parquet --column region --preview

    # Partition by full column values
    gpio partition string input.parquet output/ --column category

    # Partition by first 2 characters
    gpio partition string input.parquet output/ --column mgrs_code --chars 2

    # Hive-style partitioning
    gpio partition string input.parquet output/ --column region --hive

    # To cloud storage
    gpio partition string s3://bucket/input.parquet s3://bucket/output/ --column region --aws-profile prod
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Partition by full column values
    gpio.read('input.parquet').partition_by_string('output/', column='category')

    # Partition by first 2 characters
    gpio.read('input.parquet').partition_by_string(
        'output/',
        column='mgrs_code',
        chars=2
    )

    # Hive-style with options
    gpio.read('input.parquet').partition_by_string(
        'output/',
        column='region',
        hive=True,
        overwrite=True
    )
    ```

## By H3 Cells

Partition by H3 hexagonal cells:

=== "CLI"

    ```bash
    # Auto-calculate optimal resolution for ~100K rows per partition
    gpio partition h3 input.parquet output/ --auto

    # Auto with custom target partition size
    gpio partition h3 input.parquet output/ --auto --target-rows 50000

    # Preview at resolution 7 (~5km² cells)
    gpio partition h3 input.parquet --resolution 7 --preview

    # Partition at specific resolution 9
    gpio partition h3 input.parquet output/ --resolution 9

    # Keep H3 column in output files
    gpio partition h3 input.parquet output/ --resolution 9 --keep-h3-column

    # Hive-style (H3 column included by default)
    gpio partition h3 input.parquet output/ --resolution 8 --hive
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Partition by H3 (Hive-style by default)
    gpio.read('input.parquet').partition_by_h3('output/')

    # Custom resolution
    gpio.read('input.parquet').partition_by_h3('output/', resolution=7)

    # With options
    gpio.read('input.parquet').partition_by_h3(
        'output/',
        resolution=8,
        compression='ZSTD',
        overwrite=True
    )
    ```

**Column behavior:**

- Non-Hive: H3 column excluded by default (redundant with path)
- Hive: H3 column included by default
- Use `--keep-h3-column` to explicitly keep

If H3 column doesn't exist, it's automatically added.

### Auto-Resolution for H3

Use `--auto` to let gpio calculate the optimal H3 resolution:

=== "CLI"

    ```bash
    # Auto-select resolution for ~100k rows per partition (default)
    gpio partition h3 input.parquet output/ --auto

    # Target 50k rows per partition
    gpio partition h3 input.parquet output/ --auto --target-rows 50000

    # Limit maximum partitions created
    gpio partition h3 input.parquet output/ --auto --max-partitions 5000

    # Preview auto-selected partitions
    gpio partition h3 input.parquet --auto --preview
    ```

!!! note "CLI-Only Feature"
    Auto-resolution is currently CLI-only. For Python, use `partition_by_h3()` with an explicit `resolution` parameter (see examples above).

Auto-resolution calculates the optimal H3 resolution using the formula: `cells ≈ 122 × 7^resolution`. The algorithm targets your specified rows per partition while respecting the `--max-partitions` constraint.

## By S2 Cells

Partition by S2 spherical cells:

=== "CLI"

    ```bash
    # Auto-calculate optimal level for ~100K rows per partition
    gpio partition s2 input.parquet output/ --auto

    # Auto with custom target partition size
    gpio partition s2 input.parquet output/ --auto --target-rows 500000

    # Preview at level 10 (~78 km² cells)
    gpio partition s2 input.parquet --level 10 --preview

    # Partition at specific level 13 (~1.2km² cells)
    gpio partition s2 input.parquet output/ --level 13

    # Keep S2 column in output files
    gpio partition s2 input.parquet output/ --level 12 --keep-s2-column

    # Hive-style (S2 column included by default)
    gpio partition s2 input.parquet output/ --auto --hive
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Partition by S2 (Hive-style by default)
    gpio.read('input.parquet').partition_by_s2('output/')

    # Custom level
    gpio.read('input.parquet').partition_by_s2('output/', level=10)

    # With options
    gpio.read('input.parquet').partition_by_s2(
        'output/',
        level=10,
        compression='ZSTD',
        overwrite=True
    )
    ```

**Column behavior:**

- Non-Hive: S2 column excluded by default (redundant with path)
- Hive: S2 column included by default
- Use `--keep-s2-column` to explicitly keep

If S2 column doesn't exist, it's automatically added.

### Auto-Resolution for S2

Use `--auto` to let gpio calculate the optimal S2 level:

=== "CLI"

    ```bash
    # Auto-select level for ~100k rows per partition (default)
    gpio partition s2 input.parquet output/ --auto

    # Target 50k rows per partition
    gpio partition s2 input.parquet output/ --auto --target-rows 50000

    # Limit maximum partitions created
    gpio partition s2 input.parquet output/ --auto --max-partitions 5000

    # Preview auto-selected partitions
    gpio partition s2 input.parquet --auto --preview
    ```

!!! note "CLI-Only Feature"
    Auto-resolution is currently CLI-only. For Python, use `partition_by_s2()` with an explicit `level` parameter (see examples above).

Auto-resolution calculates the optimal S2 level using the formula: `cells = 6 × 4^level`. The algorithm targets your specified rows per partition while respecting the `--max-partitions` constraint.

## By A5 Cells

Partition by A5 spatial cells:

=== "CLI"

    ```bash
    # Auto-calculate optimal resolution for ~100K rows per partition
    gpio partition a5 input.parquet output/ --auto

    # Auto with custom target partition size
    gpio partition a5 input.parquet output/ --auto --target-rows 500000

    # Preview at resolution 10 (~41km² cells)
    gpio partition a5 input.parquet --resolution 10 --preview

    # Partition at specific resolution 15
    gpio partition a5 input.parquet output/ --resolution 15

    # Keep A5 column in output files
    gpio partition a5 input.parquet output/ --resolution 12 --keep-a5-column

    # Hive-style (A5 column included by default)
    gpio partition a5 input.parquet output/ --auto --hive
    ```

=== "Python"

    !!! note "CLI-Only"
        A5 partitioning is currently CLI-only. Use S2 partitioning in Python as an alternative:
        ```python
        gpio.read('input.parquet').partition_by_s2('output/', level=10)
        ```

**Column behavior:**

- Non-Hive: A5 column excluded by default (redundant with path)
- Hive: A5 column included by default
- Use `--keep-a5-column` to explicitly keep

If A5 column doesn't exist, it's automatically added.

### Auto-Resolution for A5

Use `--auto` to let gpio calculate the optimal A5 resolution:

=== "CLI"

    ```bash
    # Auto-select resolution for ~100k rows per partition (default)
    gpio partition a5 input.parquet output/ --auto

    # Target 50k rows per partition
    gpio partition a5 input.parquet output/ --auto --target-rows 50000

    # Limit maximum partitions created
    gpio partition a5 input.parquet output/ --auto --max-partitions 5000

    # Preview auto-selected partitions
    gpio partition a5 input.parquet --auto --preview
    ```

!!! note "CLI-Only Feature"
    Auto-resolution is currently CLI-only. A5 partitioning in Python is not yet available.

Auto-resolution calculates the optimal A5 resolution using the formula: `cells = 6 × 4^resolution`. The algorithm targets your specified rows per partition while respecting the `--max-partitions` constraint.

## By Quadkey Cells

Partition by Bing Maps quadkey tiles:

=== "CLI"

    ```bash
    # Auto-calculate optimal resolution for ~100K rows per partition
    gpio partition quadkey input.parquet output/ --auto

    # Auto with custom target partition size
    gpio partition quadkey input.parquet output/ --auto --target-rows 500000

    # Preview with auto-resolution
    gpio partition quadkey input.parquet --auto --preview

    # Partition at specific resolutions (column at 13, partition at 9)
    gpio partition quadkey input.parquet output/ --resolution 13 --partition-resolution 9

    # Keep quadkey column in output files
    gpio partition quadkey input.parquet output/ --resolution 13 --partition-resolution 9 --keep-quadkey-column

    # Hive-style (quadkey column included by default)
    gpio partition quadkey input.parquet output/ --auto --hive
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Partition by quadkey
    gpio.read('input.parquet').partition_by_quadkey('output/')

    # Custom resolution
    gpio.read('input.parquet').partition_by_quadkey('output/', partition_resolution=8)

    # With options
    gpio.read('input.parquet').partition_by_quadkey(
        'output/',
        partition_resolution=10,
        compression='ZSTD',
        overwrite=True
    )
    ```

**Column behavior:**

- Non-Hive: Quadkey column excluded by default (redundant with path)
- Hive: Quadkey column included by default
- Use `--keep-quadkey-column` to explicitly keep

The quadkey column is created at `--resolution` (for full precision) but partitions are created using the first `--partition-resolution` characters, allowing coarser partitioning while retaining full precision in the column.

### Auto-Resolution for Quadkey

Use `--auto` to let gpio calculate the optimal quadkey zoom level:

=== "CLI"

    ```bash
    # Auto-select zoom level for ~100k rows per partition (default)
    gpio partition quadkey input.parquet output/ --auto

    # Target 50k rows per partition
    gpio partition quadkey input.parquet output/ --auto --target-rows 50000

    # Limit maximum partitions created
    gpio partition quadkey input.parquet output/ --auto --max-partitions 5000

    # Preview auto-selected partitions
    gpio partition quadkey input.parquet --auto --preview
    ```

=== "Python"

!!! note "CLI-Only Feature"
    Auto-resolution is currently CLI-only. For Python, use `partition_by_quadkey()` with an explicit `partition_resolution` parameter (see examples above).

Auto-resolution calculates the optimal quadkey zoom level using the formula: `tiles = 4^zoom`. The algorithm targets your specified rows per partition while respecting the `--max-partitions` constraint.

## By KD-Tree

Partition by balanced spatial partitions:

=== "CLI"

    ```bash
    # Auto-partition (default: ~120k rows each)
    gpio partition kdtree input.parquet output/

    # Preview auto-selected partitions
    gpio partition kdtree input.parquet --preview

    # Explicit partition count (must be power of 2)
    gpio partition kdtree input.parquet output/ --partitions 32

    # Exact computation (deterministic)
    gpio partition kdtree input.parquet output/ --partitions 16 --exact

    # Hive-style with progress tracking
    gpio partition kdtree input.parquet output/ --hive --verbose
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Partition using KD-tree (creates 2^iterations partitions)
    gpio.read('input.parquet').partition_by_kdtree('output/')

    # 64 partitions (2^6)
    gpio.read('input.parquet').partition_by_kdtree('output/', iterations=6)

    # With options
    gpio.read('input.parquet').partition_by_kdtree(
        'output/',
        iterations=5,  # 32 partitions
        hive=True,
        overwrite=True
    )
    ```

    !!! note "CLI vs Python API"
        The Python API uses `iterations` which creates 2^iterations partitions (power-of-two semantics).
        The CLI uses `--partitions N` to specify an absolute count directly. For example:

        - Python `iterations=6` → 64 partitions (2^6)
        - CLI `--partitions 64` → 64 partitions

**Column behavior:**
- Similar to H3: excluded by default, included for Hive
- Use `--keep-kdtree-column` to explicitly keep

If KD-tree column doesn't exist, it's automatically added.

## By Admin Boundaries

Split by administrative boundaries via spatial join with remote datasets:

### How It Works

This command performs **two operations**:

1. **Spatial Join**: Queries remote admin boundaries using spatial extent filtering, then spatially joins them with your data
2. **Partition**: Splits the enriched data by administrative levels

### Quick Start

=== "CLI"

    ```bash
    # Preview GAUL partitions by continent
    gpio partition admin input.parquet --dataset gaul --levels continent --preview

    # Partition by continent
    gpio partition admin input.parquet output/ --dataset gaul --levels continent

    # Hive-style partitioning
    gpio partition admin input.parquet output/ --dataset gaul --levels continent --hive
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Partition by country using GAUL dataset
    gpio.read('input.parquet').partition_by_admin(
        'output/',
        dataset='gaul',
        levels=['country']
    )

    # Hive-style partitioning
    gpio.read('input.parquet').partition_by_admin(
        'output/',
        dataset='gaul',
        levels=['country'],
        hive=True
    )
    ```

### Multi-Level Hierarchical Partitioning

Partition by multiple administrative levels:

=== "CLI"

    ```bash
    # Hierarchical: continent → country
    gpio partition admin input.parquet output/ --dataset gaul --levels continent,country

    # All GAUL levels: continent → country → department
    gpio partition admin input.parquet output/ --dataset gaul --levels continent,country,department

    # Hive-style multi-level (creates continent=Africa/country=Kenya/department=Accra/)
    gpio partition admin input.parquet output/ --dataset gaul \
        --levels continent,country,department --hive

    # Overture Maps by country and region
    gpio partition admin input.parquet output/ --dataset overture --levels country,region
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    # Multi-level hierarchical
    gpio.read('input.parquet').partition_by_admin(
        'output/',
        dataset='gaul',
        levels=['continent', 'country', 'department'],
        hive=True
    )

    # Using Overture Maps dataset
    gpio.read('input.parquet').partition_by_admin(
        'output/',
        dataset='overture',
        levels=['country', 'region']
    )
    ```

### Datasets

--8<-- "_includes/admin-datasets.md"

## Common Options

All partition commands support:

--8<-- "_includes/common-cli-options.md"

```bash
--preview-limit 15     # Number of partitions to show (default: 15)
--force                # Override analysis warnings
--skip-analysis        # Skip analysis (performance-sensitive cases)
--prefix PREFIX        # Custom filename prefix (e.g., 'fields' → fields_USA.parquet)
```

## Output Structures

### Standard Partitioning

```
output/
├── partition_value_1.parquet
├── partition_value_2.parquet
└── partition_value_3.parquet
```

### Hive-Style Partitioning

```
output/
├── column=value1/
│   └── data.parquet
├── column=value2/
│   └── data.parquet
└── column=value3/
    └── data.parquet
```

### Custom Filename Prefix

Add `--prefix NAME` to prepend a custom prefix to partition filenames:

```bash
# Standard: fields_USA.parquet, fields_Kenya.parquet
gpio partition admin input.parquet output/ --dataset gaul --levels country --prefix fields

# Hive: country=USA/fields_USA.parquet, country=Kenya/fields_Kenya.parquet
gpio partition admin input.parquet output/ --dataset gaul --levels country --prefix fields --hive
```

## Partition Analysis

Before creating files, analysis shows:

- Total partition count
- Rows per partition (min/max/avg/median)
- Distribution statistics
- Recommendations and warnings

**Warnings trigger for:**
- Very uneven distributions
- Too many small partitions
- Single-row partitions

Use `--force` to override warnings or `--skip-analysis` for performance.

## Preview Workflow

### With Auto-Resolution

```bash
# 1. Preview with auto-resolution
gpio partition h3 large.parquet --auto --preview

# 2. Adjust target rows if needed
gpio partition h3 large.parquet --auto --target-rows 50000 --preview

# 3. Execute when satisfied
gpio partition h3 large.parquet output/ --auto --target-rows 50000
```

### With Manual Resolution

```bash
# 1. Preview to understand partitioning
gpio partition h3 large.parquet --resolution 7 --preview

# 2. Adjust resolution if needed
gpio partition h3 large.parquet --resolution 8 --preview

# 3. Execute when satisfied
gpio partition h3 large.parquet output/ --resolution 8
```

## Sub-Partitioning Large Files

After partitioning by admin boundaries or string columns, some files may still be too large. Use `--min-size` with directory input to sub-partition only the oversized files:

```bash
# Sub-partition files >100MB with H3
gpio partition h3 by_country/ --min-size 100MB --resolution 7 --in-place
```

See [Sub-Partitioning Large Files](sub-partitioning.md) for details.

## See Also

- [CLI Reference: partition](../cli/partition.md)
- [add command](add.md) - Add spatial indices before partitioning
