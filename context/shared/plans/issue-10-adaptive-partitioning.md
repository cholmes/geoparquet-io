# Issue #10: Adaptive Partitioning Based on Max Size

**Status**: Planning
**Created**: 2026-02-02
**Related Issues**: #10, #23, #11, #14
**Reference**: https://github.com/opengeos/open-buildings, https://dewey.dunnington.ca/post/2024/partitioning-strategies-for-bigger-than-memory-spatial-data/

## Overview

Enhance gpio's partitioning capabilities to support:
1. **Adaptive spatial partitioning** - automatically adjust cell resolution based on data density
2. **Size-based constraints** - specify max partition size (rows or MB)
3. **Hierarchical partitioning** - combine admin boundaries with spatial partitioning

## Current State

### Existing Partitioning Methods

| Method | Type | Resolution | Adaptive? | Notes |
|--------|------|------------|-----------|-------|
| `partition-h3` | Spatial (hexagonal) | Fixed | ❌ | Single H3 resolution level |
| `partition-a5` | Spatial (S2 cells) | Fixed | ❌ | Single S2/A5 resolution level |
| `partition-quadkey` | Spatial (Bing tiles) | Fixed | ❌ | Single quadkey resolution level |
| `partition-kdtree` | Spatial (rectangular) | Adaptive | ✅ | Splits on median until target partitions reached |
| `partition-admin` | Administrative | N/A | ❌ | Country/state/etc. boundaries |
| `partition-string` | Generic | N/A | ❌ | Any string column |

### Key Architecture

All partition methods follow this pattern:
1. **Add column** (if needed) - e.g., `add_h3_column`, `add_a5_column`
2. **Analyze** - use `partition_common.analyze_partition_strategy()` to check for pathological cases
3. **Partition** - use `partition_common.partition_by_column()` to write files

**Shared utilities** in `partition_common.py`:
- `analyze_partition_strategy()` - detects tiny partitions, imbalance, etc.
- `partition_by_column()` - generic partitioning by any column
- `preview_partition()` - shows what partitions would be created
- `calculate_partition_stats()` - reports size/count metrics

## Proposed Enhancement

### Three Scenarios to Support

#### 1. Fixed-Level Size-Constrained Partitioning

**Use case**: "Partition my data at H3 resolution 6, but if any cell exceeds 1M rows, use resolution 7 for that area"

**Approach**:
- Start with target resolution
- Identify cells exceeding max size
- For oversized cells, create partitions at next finer resolution
- Repeat recursively until all partitions are under max size

**Example**:
```bash
gpio partition-h3 input.parquet output/ \
    --resolution 6 \
    --max-rows 1000000 \
    --adaptive
```

#### 2. Automatic Resolution Selection

**Use case**: "Partition my data into ~500 files of ~100K rows each, using H3"

**Approach**:
- Calculate optimal starting resolution based on total rows and target partition count
- Apply adaptive subdivision as needed
- Similar to how `partition-kdtree --auto` works today

**Example**:
```bash
gpio partition-h3 input.parquet output/ \
    --auto \
    --target-rows 100000 \
    --max-partitions 1000
```

#### 3. Hierarchical Admin + Spatial Partitioning

**Use case**: "Partition by country first, then subdivide large countries using quadkeys"

**Approach**:
- First partition by admin boundary (country/state/etc.)
- For partitions exceeding max size, apply spatial partitioning within that boundary
- This is the open-buildings pattern

**Example**:
```bash
gpio partition-admin input.parquet output/ \
    --dataset naturalearth_countries \
    --max-rows 10000000 \
    --subdivide-with h3 \
    --subdivide-resolution 7
```

## Implementation Plan

**Revised Order** (based on 2026-02-02 discussion):
1. **Auto-resolution first** - Simpler, immediate value
2. **Fixed-level adaptive second** - More complex recursive logic
3. **Admin subdivision** - Special case of adaptive (apply to admin partitions)
4. **Skip A5/S2** - Focus on H3 and quadkey only

### Phase 1: Auto-Resolution Mode (START HERE)

**Goals**:
- Add `--max-rows` and `--max-size-mb` flags to `partition-h3`, `partition-a5`, `partition-quadkey`
- Add `--adaptive` flag to enable recursive subdivision
- Implement adaptive logic in `partition_common.py` as shared utility

**New shared function**:
```python
def partition_adaptive_by_spatial_column(
    input_parquet: str,
    output_folder: str,
    column_name: str,
    initial_prefix_length: int,
    max_rows: int | None = None,
    max_size_mb: float | None = None,
    max_depth: int = 5,  # prevent infinite recursion
    **kwargs
) -> int:
    """
    Partition adaptively by increasing prefix length (cell resolution)
    for oversized partitions.

    For H3: prefix_length = resolution * 2 (H3 cells use 2 hex digits per level)
    For quadkey: prefix_length = resolution (1 char per zoom level)
    For A5: prefix_length varies (need to check S2 token format)
    """
```

**Algorithm**:
```python
def adaptive_partition_recursive(
    con, input_url, column_name, prefix_length, max_constraint, depth=0, prefix=""
):
    # Get partition sizes at current prefix length
    partition_stats = get_partition_stats(con, input_url, column_name, prefix_length, prefix)

    for partition_value, row_count, size_mb in partition_stats:
        if exceeds_constraint(row_count, size_mb, max_constraint):
            if depth >= max_depth:
                warn(f"Max depth reached for {partition_value}, writing oversized partition")
                write_partition(partition_value, prefix_length)
            else:
                # Recursive subdivision - increase prefix length (finer resolution)
                adaptive_partition_recursive(
                    con, input_url, column_name,
                    prefix_length + 1,  # For quadkey/H3
                    max_constraint,
                    depth + 1,
                    prefix=partition_value
                )
        else:
            # Within constraint, write partition
            write_partition(partition_value, prefix_length)
```

**CLI changes**:
```python
@click.option('--max-rows', type=int, help='Max rows per partition (enables adaptive mode)')
@click.option('--max-size-mb', type=float, help='Max size in MB per partition (enables adaptive mode)')
@click.option('--max-depth', default=5, type=int, help='Max recursion depth for adaptive partitioning')
```

**Challenges**:
- Different spatial indexes have different resolution/prefix relationships:
  - **H3**: 16 resolutions (0-15), cell IDs are variable-length hex strings
  - **Quadkey**: 23 zoom levels (0-23), 1 char per level, predictable
  - **A5/S2**: Need to investigate S2 token format and level encoding
- Need to handle edge cases where subdivision doesn't reduce size enough
- Need clear error messages when max depth reached with oversized partitions

### Phase 2: Fixed-Level Adaptive Partitioning

**Goals**:
- Add `--auto` flag to spatial partitioners (like kdtree has)
- Automatically calculate optimal starting resolution based on constraints
- Estimate partition counts and sizes before starting

**Algorithm**:
```python
def calculate_optimal_resolution(
    input_parquet: str,
    spatial_index_type: str,  # 'h3', 'quadkey', 'a5'
    target_rows: int,
    max_partitions: int,
    total_rows: int
) -> int:
    """
    Calculate optimal starting resolution for target constraints.

    For H3: Average cell area decreases by ~7x per resolution level
    For Quadkey: Area decreases by 4x per zoom level
    For A5/S2: Area decreases by 4x per level
    """
    target_partition_count = total_rows / target_rows

    if target_partition_count > max_partitions:
        warn("Target partition count exceeds max, adjusting...")
        target_partition_count = max_partitions

    # Use spatial index math to estimate resolution
    if spatial_index_type == 'h3':
        # H3 has ~122 cells at level 0, ~7^n cells at level n
        estimated_resolution = math.ceil(math.log(target_partition_count / 122) / math.log(7))
    elif spatial_index_type == 'quadkey':
        # Quadkey has 4^n cells at level n
        estimated_resolution = math.ceil(math.log(target_partition_count, 4))
    elif spatial_index_type == 'a5':
        # S2 has 6 faces, 4^n cells per face at level n
        estimated_resolution = math.ceil(math.log(target_partition_count / 6, 4))

    return clamp(estimated_resolution, min_level, max_level)
```

**CLI changes**:
```python
@click.option('--auto', is_flag=True, help='Automatically calculate optimal resolution')
@click.option('--target-rows', type=int, default=100000, help='Target rows per partition (with --auto)')
@click.option('--max-partitions', type=int, default=1000, help='Max number of partitions (with --auto)')
```

**Note**: This should integrate with adaptive mode - auto calculates starting point, adaptive refines it.

### Phase 3: Hierarchical Admin + Spatial Partitioning

**Goals**:
- Extend `partition-admin` to support spatial subdivision
- Add `--subdivide-with` option (h3, quadkey, a5, kdtree)
- Support constraints: `--max-rows`, `--max-size-mb`

**New module**: `geoparquet_io/core/partition_admin_spatial.py`

**Algorithm**:
```python
def partition_admin_with_spatial_subdivision(
    input_parquet: str,
    output_folder: str,
    admin_dataset: str,
    admin_levels: list[str],
    subdivide_method: str,  # 'h3', 'quadkey', 'a5', 'kdtree'
    subdivide_resolution: int | None = None,
    max_rows: int | None = None,
    max_size_mb: float | None = None,
    **kwargs
) -> int:
    """
    Hierarchical partitioning: admin boundaries → spatial cells.

    1. Partition by admin boundaries (country/state/etc.)
    2. For each admin partition exceeding max constraint:
       a. Add spatial column (H3/quadkey/etc.) for features in that admin area
       b. Apply adaptive spatial partitioning
    3. Output: country.parquet OR country_h3cell.parquet
    """
    # First pass - partition by admin
    admin_partitions = partition_by_admin(input_parquet, output_folder, admin_dataset, admin_levels)

    # Second pass - subdivide large partitions
    for admin_partition_file in admin_partitions:
        partition_stats = get_file_stats(admin_partition_file)

        if exceeds_constraint(partition_stats, max_rows, max_size_mb):
            # Subdivide this admin partition spatially
            subdivided_files = partition_adaptive_by_spatial(
                input_parquet=admin_partition_file,
                output_folder=output_folder,
                method=subdivide_method,
                resolution=subdivide_resolution,
                max_rows=max_rows,
                max_size_mb=max_size_mb
            )

            # Remove original oversized file
            os.remove(admin_partition_file)
        else:
            # Keep original partition, it's within constraints
            pass

    return count_total_partitions(output_folder)
```

**CLI changes**:
```python
@main.command("partition-admin")
# ... existing options ...
@click.option('--subdivide-with', type=click.Choice(['h3', 'quadkey', 'a5', 'kdtree']),
              help='Subdivide large admin partitions using spatial index')
@click.option('--subdivide-resolution', type=int,
              help='Resolution for spatial subdivision (required with --subdivide-with)')
@click.option('--subdivide-auto', is_flag=True,
              help='Auto-calculate subdivision resolution')
@click.option('--max-rows', type=int,
              help='Max rows per partition (triggers subdivision)')
@click.option('--max-size-mb', type=float,
              help='Max size per partition in MB (triggers subdivision)')
```

**Output structure** (with Hive partitioning):
```
output/
├── country_iso=USA/
│   ├── h3=8a2a1072b59ffff.parquet   # Subdivided
│   ├── h3=8a2a1072b5affff.parquet   # Subdivided
│   └── h3=8a2a1072b5bffff.parquet   # Subdivided
├── country_iso=CAN/
│   ├── h3=8a2a1072b59ffff.parquet   # Subdivided
│   └── h3=8a2a1072b5affff.parquet   # Subdivided
└── country_iso=MEX/
    └── MEX.parquet                    # Small enough, not subdivided
```

### Phase 4: Documentation and Examples

**New documentation files**:
- `docs/guide/adaptive-partitioning.md` - Guide for adaptive partitioning
- `docs/guide/hierarchical-partitioning.md` - Guide for admin + spatial hierarchies
- `examples/05_adaptive_partitioning.ipynb` - Jupyter notebook examples

**Update existing docs**:
- `docs/cli/partition.md` - Add new flags and examples
- `docs/guide/partition.md` - Explain adaptive vs. fixed strategies
- `docs/concepts/spatial-indices.md` - Explain resolution/cell relationships

## Testing Strategy

### Unit Tests

**New test files**:
- `tests/test_partition_adaptive.py` - Test adaptive subdivision logic
- `tests/test_partition_admin_spatial.py` - Test hierarchical partitioning

**Test cases**:
1. **Fixed-level adaptive**:
   - Single oversized cell triggers subdivision
   - Multiple oversized cells handled correctly
   - Max depth prevents infinite recursion
   - Empty partitions skipped

2. **Auto-resolution**:
   - Correct resolution calculated for various total_rows/target_rows
   - Handles edge cases (very small/large datasets)
   - Respects max_partitions constraint

3. **Hierarchical partitioning**:
   - Small admin regions not subdivided
   - Large admin regions subdivided correctly
   - Hive partitioning structure correct
   - Mixed output (some subdivided, some not) works

4. **Constraints**:
   - `--max-rows` respected
   - `--max-size-mb` respected (estimated)
   - Both constraints can be used together (AND logic)

### Integration Tests

**Test with real-world scenarios**:
1. Small dataset (< 1M rows) - should not partition or partition minimally
2. Medium dataset (1M-10M rows) - adaptive subdivision should work smoothly
3. Large dataset with density clusters - should create fine-grained partitions in dense areas only
4. Global dataset - hierarchical country → spatial partitioning

**Performance benchmarks**:
- Compare fixed vs. adaptive partitioning performance
- Measure overhead of partition analysis
- Test with `--skip-analysis` for large datasets

## Open Questions

### 1. Spatial Index Resolution Encoding

**H3**:
- H3 cell IDs are 16-character hex strings (e.g., `8a2a1072b59ffff`)
- Resolution encoded in first 2 hex chars
- Can partition by prefix: `LEFT(h3, 2*resolution)`

**Quadkey**:
- Quadkey strings have 1 char per zoom level (e.g., `0231102`)
- Length = zoom level
- Can partition by prefix: `LEFT(quadkey, zoom_level)`

**A5 (S2)**:
- Need to investigate S2 cell token format
- S2 has 31 levels (0-30)
- Need to understand how to extract prefix for a given level

**Action**: Research S2/A5 token format before implementing adaptive A5 partitioning.

### 2. Size Estimation Accuracy

**Challenge**: `--max-size-mb` requires estimating partition sizes before writing.

**Approaches**:
1. **Sampling**: Sample N% of rows, measure serialized size, extrapolate
2. **Row-based proxy**: Use `--max-rows` as primary constraint, estimate MB from average row size
3. **Approximate file size**: Use input file size / total rows * partition rows

**Recommendation**: Start with approach #3 (simplest), add sampling if accuracy issues arise.

### 3. Subdivision Stop Conditions

What happens when subdivision doesn't help?

**Scenarios**:
- Dense point cluster - all points have same H3 cell even at highest resolution
- Large polygons - geometry column dominates size, cell subdivision doesn't help

**Solutions**:
1. Set reasonable `--max-depth` (default 5 levels of subdivision)
2. Warn user when max depth reached with oversized partition
3. Support `--force` to write oversized partitions anyway
4. Document that adaptive partitioning works best for point data

### 4. API Design for Python

How should adaptive partitioning look in the Python API?

**Option 1**: New functions
```python
from geoparquet_io.api import adaptive_partition_h3

adaptive_partition_h3(
    input_parquet='input.parquet',
    output_folder='output/',
    initial_resolution=6,
    max_rows=1000000,
    max_depth=5
)
```

**Option 2**: Extend existing functions
```python
from geoparquet_io.api import partition_h3

partition_h3(
    input_parquet='input.parquet',
    output_folder='output/',
    resolution=6,
    adaptive=True,        # Enable adaptive mode
    max_rows=1000000,
    max_depth=5
)
```

**Recommendation**: Option 2 - extend existing API with optional parameters. More discoverable and consistent with CLI.

## Success Criteria

### Minimum Viable Product (MVP)

- [ ] Adaptive H3 partitioning working with `--max-rows`
- [ ] Adaptive quadkey partitioning working with `--max-rows`
- [ ] CLI tests passing for adaptive mode
- [ ] Basic documentation in `docs/guide/partition.md`

### Full Feature Set

- [ ] All 3 scenarios implemented (fixed-adaptive, auto-resolution, hierarchical)
- [ ] Support for H3, quadkey, A5/S2
- [ ] Both `--max-rows` and `--max-size-mb` constraints
- [ ] Python API support
- [ ] Comprehensive test coverage (>80%)
- [ ] Full documentation with examples
- [ ] Performance benchmarks documented

### Nice to Have

- [ ] Progress reporting for long-running partitioning jobs
- [ ] Parallel partition writing (using multiprocessing)
- [ ] Dry-run mode showing what partitions would be created
- [ ] Integration with `partition-kdtree` for hybrid kdtree+spatial partitioning

## Timeline Estimate

- **Phase 1** (Adaptive spatial): 3-4 days
  - 1 day: Core adaptive logic in `partition_common.py`
  - 1 day: Integrate with `partition-h3` and `partition-quadkey`
  - 1 day: Tests and bug fixes
  - 0.5 day: A5/S2 research and implementation

- **Phase 2** (Auto-resolution): 1-2 days
  - 0.5 day: Resolution calculation logic
  - 0.5 day: CLI integration
  - 0.5 day: Tests

- **Phase 3** (Hierarchical): 2-3 days
  - 1 day: Core hierarchical logic
  - 1 day: CLI integration and output structure
  - 0.5 day: Tests

- **Phase 4** (Documentation): 1-2 days
  - 0.5 day: Update existing docs
  - 1 day: New guides and examples

**Total**: 7-11 days of focused development

## References

1. **Issue #10**: https://github.com/user/geoparquet-io/issues/10
2. **Dewey Dunnington blog post**: https://dewey.dunnington.ca/post/2024/partitioning-strategies-for-bigger-than-memory-spatial-data/
3. **open-buildings implementation**: https://github.com/opengeos/open-buildings/blob/main/open_buildings/google/partition.py
4. **H3 documentation**: https://h3geo.org/docs/
5. **Quadkey documentation**: https://learn.microsoft.com/en-us/bingmaps/articles/bing-maps-tile-system
6. **S2 Geometry documentation**: https://s2geometry.io/

## Next Steps

1. **Review this plan** with Chris and get feedback on approach
2. **Research A5/S2 token format** for adaptive partitioning
3. **Start with Phase 1** - implement adaptive mode for H3 and quadkey
4. **Add tests incrementally** as features are implemented
5. **Document as you go** - update docs for each completed phase
