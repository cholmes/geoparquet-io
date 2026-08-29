# sort Command

For detailed usage and examples, see the [Sort User Guide](../guide/sort.md).

## Quick Reference

```bash
gpio sort --help
```

This will show all available subcommands and options.

## Subcommands

### hilbert

Sort by Hilbert space-filling curve for optimal spatial ordering:

```bash
gpio sort hilbert input.parquet output.parquet [OPTIONS]
```

Options:

| Option | Default | Description |
|--------|---------|-------------|
| `-g, --geometry-column` | auto-detect | Geometry column name |
| `--add-bbox` | - | Add bbox column if missing |
| `--compression` | ZSTD | Compression codec (ZSTD, SNAPPY, GZIP, etc.) |
| `--compression-level` | - | Compression level |
| `--row-group-size` | - | Exact row count per group (10k-50k recommended for spatial pushdown) |
| `--row-group-size-mb` | - | Target group size in MB/GB |
| `--geoparquet-version` | 1.1 | Output version: `1.1`, `2.0`, or `parquet-geo-only` |
| `--overwrite` | - | Overwrite existing output file |
| `--verbose` | - | Verbose output |
| `--show-sql` | - | Show generated SQL |

### str

Pack rows with Sort-Tile-Recursive ordering for compact row-group bounding boxes:

```bash
gpio sort str input.parquet output.parquet --row-group-size 50000 [OPTIONS]
```

STR uses geometry bounding-box centers, creates approximately square X strips,
and sorts each strip on Y with alternating direction. `--row-group-size`
controls both the spatial tile capacity and the output row-group target, so an
exact row count is recommended.

The `str` subcommand supports the same geometry, bbox, compression, row-group,
GeoParquet version, overwrite, verbosity, and SQL-display options as `hilbert`.

### quadkey

Sort by quadkey for spatial locality:

```bash
gpio sort quadkey input.parquet output.parquet [OPTIONS]
```

### column

Sort by any column(s):

```bash
gpio sort column input.parquet output.parquet COLUMNS [OPTIONS]
```

Arguments:
- `COLUMNS` - Comma-separated column names to sort by

Options:
- `--descending` - Sort in descending order
- `--compression` - Compression codec
- `--geoparquet-version` - Output GeoParquet version
- `--overwrite` - Overwrite existing output
