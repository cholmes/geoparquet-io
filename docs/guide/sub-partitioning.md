# Sub-Partitioning Large Files

After partitioning by administrative boundaries or string columns, some partitions may still be too large for efficient querying. Use `--min-size` to automatically sub-partition oversized files.

## Quick Start

```bash
# First, partition by country
gpio partition admin input.parquet by_country/ --dataset gaul --levels country

# Then sub-partition large files (>100MB) with H3
gpio partition h3 by_country/ --min-size 100MB --resolution 7 --in-place
```

This finds all parquet files over 100MB in `by_country/` and partitions them by H3 cells, replacing the original files with sub-partition directories.

## How It Works

When you pass a directory to a partition command with `--min-size`:

1. Scans the directory recursively for `.parquet` files
2. Filters to files exceeding the size threshold
3. Partitions each large file into a sibling subdirectory
4. With `--in-place`, removes the original file after success

## Result Structure

```
by_country/
├── country=USA/
│   └── USA_h3/           ← Sub-partitioned (was >100MB)
│       ├── 872a1008fffffff.parquet
│       └── ...
├── country=Vatican/
│   └── Vatican.parquet   ← Unchanged (under threshold)
└── country=Monaco/
    └── Monaco.parquet    ← Unchanged (under threshold)
```

## Options

| Option | Description |
|--------|-------------|
| `--min-size` | Size threshold (e.g., '100MB', '1GB'). Required for directory input. |
| `--in-place` | Delete original files after successful sub-partitioning |
| `--resolution` / `--level` | Spatial index resolution (or use `--auto`) |
| `--auto` | Auto-calculate optimal resolution |

Sub-partitioning is available on `gpio partition h3`, `gpio partition s2`, and
`gpio partition quadkey`. `gpio partition a5` does not accept `--min-size` or
`--in-place` yet ([#733](https://github.com/geoparquet/geoparquet-io/issues/733));
partition A5 files one at a time instead.

## Examples

=== "H3"

    ```bash
    gpio partition h3 by_country/ --min-size 100MB --resolution 7 --in-place
    ```

=== "S2"

    ```bash
    gpio partition s2 by_country/ --min-size 100MB --level 10 --in-place
    ```

=== "Quadkey"

    ```bash
    gpio partition quadkey by_country/ --min-size 100MB --auto --in-place
    ```

## Preview Mode

Preview what would be processed without making changes:

```bash
# See which files would be sub-partitioned (no --in-place)
gpio partition h3 by_country/ --min-size 100MB --resolution 7
```

Files are processed but originals are kept when `--in-place` is not specified.

## Size Threshold Examples

| Threshold | Use Case |
|-----------|----------|
| `50MB` | Aggressive splitting for web delivery |
| `100MB` | Balanced (recommended default) |
| `250MB` | Light splitting for local analysis |
| `1GB` | Only split very large files |

## See Also

- [Partitioning Files](partition.md) - All partition command options
- [Command Piping](piping.md) - Chaining commands
