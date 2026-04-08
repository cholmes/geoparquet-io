# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.1.0-beta] - 2026-03-05

Minor version bump for ongoing beta improvements.

## [1.0.0-beta] - 2026-02-10

This is the first beta release of geoparquet-io 1.0, featuring major new spatial indexing systems, auto-resolution partitioning, comprehensive `--overwrite` support, and significant performance improvements.

### Added

#### New Spatial Indexing Systems
- **S2 support**: Add S2 cell indexing with `gpio add s2` and `gpio partition s2`
  - Full S2 geometry library integration for spherical indexing
  - Auto-resolution support for optimal cell sizing
- **A5Geo support**: Add A5 hexagonal indexing with `gpio add a5` and `gpio partition a5`
  - Efficient pentagonal/hexagonal global grid system
  - Auto-resolution partitioning support

#### Auto-Resolution Partitioning
- Automatic resolution selection for H3, S2, A5, and quadkey partitioning
  - Analyzes data extent and density to choose optimal resolution
  - Use `--resolution auto` or omit resolution for automatic selection
  - Verbose output shows resolution selection reasoning

#### Sub-Partitioning for Large Files
- `--min-size` option to find and re-partition oversized partition files
- `--in-place` option for in-place sub-partitioning
- Directory input support for batch sub-partitioning operations
- New `find_large_files()` and `sub_partition_directory()` Python API functions

#### Admin Dataset Caching
- `--cache` / `--no-cache` options for `gpio add admin-divisions`
- Automatic caching of downloaded admin boundary datasets
- `--prefix` option for custom column naming in admin-divisions

#### CLI Improvements
- `--show-sql` option on all DuckDB-based commands for query transparency
- `--verbose` option added to inspect subcommands and publish upload
- Progress reporting for add h3, add quadkey, and sort column commands
- `--row-group-size` and `--row-group-size-mb` options for convert command
- `--overwrite` option added to all extract, sort, and add commands
- Shell completion documentation for bash, zsh, and fish

#### Performance & Benchmarking
- Comprehensive benchmark suite for performance testing
- Persistent baseline storage and trend analysis for releases
- Profiling integration with benchmark suite

#### Spatial Order Detection
- `bbox-stats` based spatial order checking
- Auto-detection of spatial clustering in check command
- Bbox overlap detection for order validation

### Changed

- **BREAKING**: Renamed `--profile` to `--aws-profile` for clarity
  - Only affects AWS S3 operations (convert, extract, upload commands)
  - Local operations no longer have this flag

- **BREAKING**: Removed `--profile` flag from local commands
  - Affects: add, partition, sort, check, inspect, publish stac
  - Follows Arrow-based pipeline: extract/convert → transform locally → upload

- Improved inspect performance via DuckDB connection reuse
- Set `arrow_large_buffer_size=true` by default for large dataset support
- Better handling of larger files with faster writes

### Removed

- **BREAKING**: Removed `gpio inspect legacy` command
  - Use subcommands: `gpio inspect head/tail/stats/meta`
- Removed deprecated CLI commands and guide documentation

### Fixed

- Fix CRS export for GDAL formats (fixes #189, #190)
  - Projected CRS now correctly roundtrips through FlatGeobuf and GeoPackage
- Fix crash on non-numeric CRS codes like IGNF:LAMB93 (#193)
- Fix inspect metadata performance regression (#232)
- Fix CRS extraction when geoarrow-pyarrow is imported
- Fix Windows file locking errors in tests
- Fix DuckDB connection leak in convert_to_geoparquet
- Improved error messages for common user mistakes (#140)
  - Invalid Parquet files now show helpful hints

### Internal

- Reduced complexity in 6 functions from Grade E/D to Grade C
- Comprehensive test coverage improvements
- Plugin system documentation
- Dependency updates (actions/checkout v6, astral-sh/setup-uv v7, etc.)
