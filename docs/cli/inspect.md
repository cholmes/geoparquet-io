# inspect Command

For detailed usage and examples, see the [Inspect User Guide](../guide/inspect.md).

## Quick Reference

```bash
gpio inspect --help
```

This will show all available options for the `inspect` command.

## Subcommands

- `inspect summary` - File summary (default)
- `inspect head` - Preview first N rows
- `inspect tail` - Preview last N rows
- `inspect stats` - Column statistics and compression ratios
- `inspect meta` - Parquet metadata, GeoParquet metadata, and bloom filter info
- `inspect layers` - List layers in multi-layer formats (GeoPackage, FileGDB)

## Global Options

- `--json` - Output as JSON for scripting
- `--verbose` - Show detailed output

### inspect summary Options

- `--check-all-files` - For partitioned datasets, check all files

### inspect meta Options

- `--geo` - Show only GeoParquet 'geo' metadata
- `--parquet` - Show only Parquet file metadata
- `--parquet-geo` - Show only Parquet geospatial metadata
- `--geo-stats` - Show per-row-group geo_bbox bounding box statistics
- `--row-groups N` - Number of row groups to display (default: 1)
- `--json` - Output as JSON

## Examples

```bash
# Basic inspection (runs summary by default)
gpio inspect data.parquet

# Preview first 10 rows (default)
gpio inspect head data.parquet

# Preview first 20 rows
gpio inspect head data.parquet 20

# Preview last 10 rows (default)
gpio inspect tail data.parquet

# Preview last 5 rows
gpio inspect tail data.parquet 5

# Show column statistics
gpio inspect stats data.parquet

# Comprehensive metadata
gpio inspect meta data.parquet

# GeoParquet 'geo' key metadata only
gpio inspect meta data.parquet --geo

# JSON output for scripting
gpio inspect meta data.parquet --json
```

## Metadata Flags Comparison

Use these flags with `gpio inspect meta`:

- `--geo`: Shows GeoParquet metadata from the 'geo' key (application-level metadata)
- `--parquet`: Shows complete Parquet file metadata (row groups, compression, schema)
- `--parquet-geo`: Shows geospatial metadata from Parquet footer (GEOMETRY/GEOGRAPHY logical types, bounding boxes, geospatial statistics)
