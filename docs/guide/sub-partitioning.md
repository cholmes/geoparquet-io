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

Sub-partitioning is accepted by `gpio partition h3`, `gpio partition a5`,
`gpio partition quadkey` and `gpio partition s2`. Three of them run today: S2
alone stops on a missing extension in this release (see the warning below), so
reach for **H3**, **A5** or **Quadkey**.

!!! warning "S2 sub-partitioning is unavailable in this release"
    `gpio partition s2` needs the `geography` DuckDB community extension, which is
    published only up to DuckDB 1.5.1 while gpio requires DuckDB 1.5.2 or newer, so
    it stops with an explanation instead of partitioning — including when it is
    reached through `--min-size`. Use the **H3**, **A5** or **Quadkey** tabs below
    until the extension is republished upstream; see
    [S2 Spherical Cells](add.md#s2-spherical-cells) for the details.

## Examples

=== "H3"

    <!-- doctest: setup="gpio partition quadkey input.parquet by_country/ --resolution 6 --partition-resolution 2" -->
    ```bash
    gpio partition h3 by_country/ --min-size 100MB --resolution 7 --in-place
    ```

=== "A5"

    <!-- doctest: setup="gpio partition quadkey input.parquet by_country/ --resolution 6 --partition-resolution 2" -->
    ```bash
    gpio partition a5 by_country/ --min-size 100MB --resolution 10 --in-place
    ```

=== "S2"

    Unavailable in this release (see the warning above). Kept for reference — this
    is the shape the command takes once the `geography` extension is republished.

    <!-- doctest: skip="gpio partition s2 needs the 'geography' extension, unpublished past DuckDB 1.5.1 (#737); fenced as inert because no sample partition exceeds the threshold, so the harness would score this as passing without ever invoking S2" -->
    ```text
    gpio partition s2 by_country/ --min-size 100MB --level 10 --in-place
    ```

=== "Quadkey"

    <!-- doctest: setup="gpio partition quadkey input.parquet by_country/ --resolution 6 --partition-resolution 2" -->
    ```bash
    gpio partition quadkey by_country/ --min-size 100MB --auto --in-place
    ```

## Preview Mode

Preview what would be processed without making changes:

<!-- doctest: setup="gpio partition quadkey input.parquet by_country/ --resolution 6 --partition-resolution 2" -->
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
