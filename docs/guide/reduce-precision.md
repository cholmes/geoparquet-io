# Reducing Coordinate Precision

The `reduce-precision` command snaps geometry coordinates to a fixed grid using
DuckDB's `ST_ReducePrecision`. This shrinks the dominant geometry column
substantially — typically ~37% at ~0.11 m on EPSG:4326 data, more at coarser
grids — which lowers file size and the bytes a query has to read.

## Basic Usage

=== "CLI"

    ```bash
    # Snap to ~0.11 m on EPSG:4326 (lon/lat) data
    gpio reduce-precision input.parquet output.parquet --grid 1e-6

    # If no output is given, writes <input>_reduced.parquet
    gpio reduce-precision input.parquet --grid 1e-6
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    gpio.read('input.parquet').reduce_precision(grid=1e-6).write('output.parquet')
    ```

## What it does

The pipeline runs in a specific order, because each step depends on the last:

1. **Repair** — invalid geometry is fixed with `ST_MakeValid` *before* reducing.
   `ST_ReducePrecision` aborts on invalid input, so this must come first. Opt out
   with `--no-repair-geometry` (the reduce is then guarded so invalid input
   degrades to `NULL` instead of failing the whole file).
2. **Reduce** — coordinates are snapped to `--grid`.
3. **Drop empty** — precision reduction routinely collapses thin slivers to
   `POLYGON EMPTY` / `LINESTRING EMPTY`. These (and any pre-existing null/empty
   geometry) are dropped by default, with a count warning. Use `--keep-empty` to
   retain them.

A stored bbox covering column is regenerated from the reduced geometry so it
never goes stale. The native CRS is **preserved** — precision reduction never
reprojects.

## Choosing a grid

`--grid` is **required** and is expressed in the geometry's CRS units, so the
right value depends on the CRS:

| CRS | Grid | Approx. resolution |
|-----|------|--------------------|
| EPSG:4326 (degrees) | `1e-6` | ~0.11 m (recommended) |
| EPSG:4326 (degrees) | `1e-5` | ~1.1 m (smaller files, coarser) |
| Projected / UTM (metres) | `0.1` | 0.1 m |
| Projected / UTM (metres) | `1.0` | 1 m |

!!! warning "Lossy operation"
    Precision reduction permanently discards coordinate detail and may drop
    geometries that collapse to empty. Keep the grid conservative if the same
    file is also an analytical source.

## Options

```bash
# Keep geometries that collapse to empty
gpio reduce-precision input.parquet output.parquet --grid 1e-6 --keep-empty

# Skip the make-valid step (preserve invalid geometry exactly)
gpio reduce-precision input.parquet output.parquet --grid 1e-6 --no-repair-geometry

# Preview the SQL without writing
gpio reduce-precision input.parquet output.parquet --grid 1e-6 --dry-run
```

Reads and writes work with local and remote (S3, GCS, Azure) paths, and the
command supports Unix piping (`-` for stdin/stdout).

## Compression Options

--8<-- "_includes/compression-options.md"
