# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


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

## v1.2.0 (2026-06-02)

### Feat

- **s3**: wire all remaining commands with _activate_s3()
- **api**: add S3 endpoint kwargs to read_partition()
- **cli**: wire global S3 config into all commands via ambient config scope
- **cli**: add global --s3-endpoint, --s3-region, --s3-no-ssl, --aws-profile flags
- **s3**: add ambient S3 config scope and explicit kwargs to get_duckdb_connection()
- **s3**: add resolve_s3_config() with env var fallbacks and SSL detection
- **wfs**: add --auto-tile to bypass server startIndex limits

### Fix

- **sort**: use quote_identifier for SQL identifiers and fix cleanup call
- **sort**: address adversarial review findings for PR #446
- **sort**: handle empty/null geometries in Hilbert ordering
- **test**: skip DuckDB tests that crash xdist workers
- **wfs**: handle empty/null properties in GeoJSON features
- **write**: handle remote URLs and clean up dead code in no-geometry path
- **write**: handle geometry_column=None in all write strategies (#440)
- **add**: address CodeRabbit review comments
- **add**: preserve ALL metadata in bbox-metadata operation
- **add**: preserve bloom filters and GEOMETRY type in bbox-metadata (#433)
- **convert**: support GeoArrow native-encoded GeoParquet as input
- **validate**: guard against None logical_type in schema lookups
- **arcgis**: use CRS84 for metadata when extracting via f=geojson (#427)
- **cli**: reconfigure stdout/stderr to UTF-8 at CLI entry
- **tests**: mark flaky WFS test as xfail
- **s3**: connection leak in admin_datasets + add CLI docs
- **deps**: update vulnerable dependencies
- **s3**: address adversarial review findings
- **deps**: upgrade pip to 26.1 for CVE-2026-6357
- **wfs**: add retry logic for transient network errors
- **wfs**: address adversarial review findings
- **wfs**: adaptive pagination and reliability fixes for auto-tile
- **wfs**: detect server startIndex limits and improve error messages
- **wfs**: auto-paginate large datasets and fix parallel worker crash
- **pmtiles**: address adversarial review of #422
- **pmtiles**: handle BrokenPipe and surface upstream stderr (#421)

### Refactor

- **wfs**: extract property probe and query builder helpers
- **geoarrow**: move coord-expr helpers to duckdb_utils, remove circular import
- **geo_metadata**: deduplicate _get_query_column_type by importing from duckdb_utils
- **admin**: use s3_config_scope() instead of configure_s3(con)
- **cli**: hide per-command S3 and profile flags (now global)
- **wfs**: unify fetch to always use httpx + local parsing

## v1.1.1 (2026-04-29)

### Fix

- **windows**: release file handles before replace in bbox_metadata

## v1.1.0 (2026-04-29)

### Feat

- integrate gpio-pmtiles into core
- **api**: expose axis_order and strict_crs in Python API
- **wfs**: add WFS 2.0 support, axis order fix, and CRS validation

### Fix

- **wfs**: address adversarial review findings for --output-crs
- **wfs**: honor --output-crs by reprojecting when server returns different CRS
- skip non-GeoParquet files early in check --pmtiles (#408)
- address PR #417 adversarial review findings
- PMTiles pipeline bugs found in adversarial review
- extend bbox fixes to streaming code paths
- GeoParquet 2.0 bbox handling for reproject, check, and add commands
- **tests**: use tuple comparison for pip version check
- **tests**: resolve CI failures for pip-audit CVE and wfs import
- **inspect**: scope fixtures to module, add markers, fix multi-geometry stats
- **inspect**: default --geo-stats to 1 row group with "... and N more" hint
- **inspect**: adapt geo_bbox precision to fit large projected coordinates
- **inspect**: use 2 decimal places for geo_bbox display and remove truncation
- **inspect**: show geo_bbox stats in row group tables and --geo-stats
- **wfs**: add version guard for srsName and improve test coverage
- **wfs**: include srsName parameter without bbox filter
- **test**: correct import path for list_layers module
- **convert**: apply gc.collect() on all platforms for multi-layer reads
- **wfs**: move type inference after concat, fix resource leak
- **wfs**: infer column types from string values
- **wfs**: address review findings
- **deps**: bump lxml>=6.1.0 for CVE-2026-41066 (#395)
- **deps**: remove pyarrow version cap
- **ci**: rename release.yml back to publish.yml for PyPI trusted publisher

## v1.0.1 (2026-04-20)

### Fix

- **arcgis**: address code review feedback for adaptive batch
- **arcgis**: add adaptive batch size and --batch-size flag (#382)
- **reproject**: use PROJJSON for ST_Transform when CRS lacks authority id
- **inspect**: eliminate false-positive CRS mismatch warnings for GeoParquet 2.0
- **deps**: bump pytest to 9.0.3 for CVE-2025-71176

### Refactor

- **release**: adopt portolan-cli release workflow

## v1.0.0 (2026-04-13)

### BREAKING CHANGE

- `geography_as_geometry` parameter no longer accepted.
BREAKING CHANGE: `gt` CLI alias removed, use `gpio` instead.

### Feat

- **core**: add framework-agnostic exception classes and CLI handler
- **api**: add partition_by_a5() and fix documentation drift
- **convert**: support multiple geometry columns in GeoParquet files
- **skills**: Package skill with gpio for all LLM users
- **docs**: Add menard documentation anti-drift system

### Fix

- **release**: use PyPI menard instead of git dependency
- **test**: align slow test assertions with core exception types
- **test**: handle Windows path separators in test_file_utils
- **security**: address adversarial review findings for v1.0
- **imports**: update disk_rewrite.py to import compute_bbox_via_sql from geo_metadata
- **cli**: unify exception handling across all CLI commands
- **exceptions**: unify GeoParquetError and update tests for new exception types
- **docs**: update mkdocstrings paths for reorganized modules
- **security**: sanitize URLs in logs to prevent credential leakage
- **security**: escape single quotes in SQL to prevent injection
- import-linter config and broken test monkeypatch
- **imports**: update all imports for partition/ and add/ module reorganization
- Remove inconsistent force/skip_analysis params and fix test marker
- **tests**: Relax performance threshold for macOS CI runners
- **tests**: Mark all transportforcairo WFS integration tests as xfail
- **tests**: Narrow WFS xfail to only catch WFSError exceptions
- **tests**: mark unreliable transportforcairo WFS tests as xfail
- update remaining tests to use dynamic geometry column detection
- geometry column detection and SQL identifier quoting
- **convert**: preserve original geometry column names and fix multi-geometry for all write strategies
- **core**: address CodeRabbit findings and centralize geometry detection
- **bigquery**: Address PR review findings
- **arcgis**: handle schema mismatch between paginated batches
- **benchmark**: use shared DuckDB connection helper and add test markers
- address CodeRabbit review feedback
- support native geo stats and honor row_groups limit
- **test**: update assertion to avoid conflict with v1.1 warning
- **docs**: Fix README badges, mkdocs build, and add changelog sync
- **docs**: Fix stale documentation and menard links
- **bigquery**: Add Python API parity and fix edges semantic bug
- **deps**: resolve security audit failures
- **deps**: resolve security audit failures
- **wfs**: switch integration tests from offline USGS to Cairo WFS
- address review issues in mutmut PR
- **test**: improve commitizen test robustness and coverage

### Refactor

- **core**: remove duplicate functions from common.py
- **core/partition**: replace click exceptions with core exceptions
- **core/add**: replace click exceptions with core exceptions
- **core**: replace click exceptions with core exceptions
- **core**: remove duplicated code from common.py, add re-exports
- **core**: extract modules from common.py monolith
- **core**: extract remote, geometry_detection, file_utils from common.py
- **docs**: Compress contributing.md and add auto-sync
- **docs**: Compress CLAUDE.md and add deterministic enforcement
- **scripts**: Consolidate doc generators and pre-commit hooks
- **wfs**: remove ~900 lines of dead code, fix SQL injection, add parallel pagination

## v1.0.0b2 (2026-03-06)

### Feat

- allow specifying --profile web for web specific parquet structure checks

### Fix

- Address code review issues for FileGDB CRS detection
- Add FileGDB CRS detection workaround

## v1.0-beta (2026-02-10)

### Feat

- add directory input support with --min-size to partition s2 and quadkey commands
- add directory input support with --min-size to partition h3 command
- add sub_partition_directory function for batch sub-partitioning
- add find_large_files function for directory scanning
- add --min-size and --in-place options to partition_options decorator

### Fix

- add min_size and in_place params to all partition function signatures

## v0.9.0 (2026-01-17)

## v0.8.0 (2026-01-04)

## v0.7.0 (2025-12-28)

## v0.6.1 (2025-12-11)

## v0.6.0 (2025-12-06)

### Feat

- enhance inspect command with geometry types and WKT preview

## v0.5.1 (2025-12-04)

## v0.5.0 (2025-12-02)

### Feat

- **io**: add remote-to-remote support with consolidated write infrastructure
- **auth**: add automatic AWS credential discovery for S3
- **io**: add remote-to-remote operations and automatic AWS auth
- **remote**: handle edge cases for remote file operations                                                                                                                             │ │                                                                                                                                                                                                         │ │   Edge case improvements:                                                                                                                                                                               │ │   - fix(convert): support remote parquet files via read_parquet() instead of ST_Read()                                                                                                                  │ │   - fix(convert): validate only local files, allow remote URLs to pass through                                                                                                                          │ │   - feat(stac): block remote files with clear error message and TODO for future                                                                                                                         │ │   - feat(common): add progress indicator for remote operations (shows protocol)                                                                                                                         │ │   - feat(common): add get_remote_error_hint() for better error messages                                                                                                                                 │ │   - docs: update limitations section with what works vs doesn't work                                                                                                                                    │ │                                                                                                                                                                                                         │ │   Closes edge cases for remote reads before tests/docs phase.
- **upload**: add upload command to remote buckets using obstore - Upload command with obstore, supporting parallelism and progress tracking - Single file and directory uploads - Support for s3, GCS, Azure, HTTP - Pattern filtering - Dry run mode
- **check**: add --fix flag to automatically correct issues detected by check command - add --fix, --fix-output, and --no-backup flags to all check commands - refactor check functions to return structured results enabling fixes via new core/check_fixes.py module.
- **cli**: add benchmark command for conversion performance testing

### Fix

- **check**: remove format command group - remove format command group; consolidate under check - move add-bbox-metadata to add command group - simplify add-bbox command - update + reorg tests

## v0.4.0 (2025-11-17)

### Feat

- **cli**: add ability to pass custom basename for partition output file, e.g., fields_NL.parquet instead of NL.parquet
- **stac**: add STAC Item and Collection generation
- **cli**: Add convert command for optimized GeoParquet conversion

### Fix

- **tests**: correct issue with failing test on windows

## v0.3.0 (2025-11-06)

## v0.2.0 (2025-10-24)

### Refactor

- **cli**: consolidate repetitive option decorators

## v0.1.0 (2025-10-24)

### Feat

- **cli**: add inspect command for fast file examination
- **partition**: add KD-tree spatial partitioning
- Add intelligent partition analysis with recommendations and H3 column exclusion
- **partition**: add H3 partitioning with auto-column creation
- **add**: add H3 support with computed column abstraction

### Fix

- Add Windows compatibility for hive partition tests
- Resolve Windows file locking issues in tests
- **tests**: Ensure DuckDB connections are closed before file cleanup
- Update test_partition_format.py import to use geoparquet_io
