# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


This is the first beta release of geoparquet-io 1.0, featuring major new spatial indexing systems, auto-resolution partitioning, comprehensive `--overwrite` support, and significant performance improvements.

### Added

- **`gpio pmtiles pyramid` (#570)**: bake an aggregate and its overview levels
  into a single zoom-banded PMTiles archive. Each level is tiled once with
  tippecanoe, pinned to the zoom band where its worst tile fits the
  `--max-tile-kb` budget, and the bands are merged with `tile-join` and
  recorded under a `gpio:pyramid` key in the archive metadata. Existing
  `_r*` overview siblings are reused; missing levels are built automatically.
  `--include-features` appends the raw features as the final band
  (`--features-min-zoom` defaults to base band max + 1); `--layer-mode
  single|grouped|per-level` controls layer naming for client styling. Python
  API: `ops.create_pmtiles_pyramid`.

- **`gpio process overview` (#570)**: derive coarser aggregate levels from an
  existing `gpio process aggregate` output. The scheme (`a5_cell`/`h3_cell`/
  `admin_code`) and base level are detected from the file; cells roll up by
  true hierarchy (`a5_cell_to_parent`/`h3_cell_to_parent`; admin region→country
  via ISO code prefix with cached Overture country polygons). `count`, `sum_*`,
  `min_*`, `max_*`, and breakdown `count_*` columns roll up exactly; `avg_*` is
  count-weighted (exact when the metric has no NULLs). Levels are explicit
  (`--levels 4,7`) or auto-selected against a tile-size budget
  (`--max-tile-kb`, default 500) using a worst-tile probe of parent-cell
  centroids in DuckDB. Outputs are siblings (`cells_r7.parquet`,
  `by_region_country.parquet`). Python API: `ops.create_overviews`,
  `Table.overview`.

- **`--bucket-point` on `gpio process aggregate` (#567).** Grid/admin keying
  can now derive its per-feature point from a bbox covering column
  (`--bucket-point bbox`, auto-detected or via `--bbox-column`) or an existing
  point column, instead of the geometry centroid. Since the default output
  synthesizes cell polygons from the cell id, the (usually huge) geometry
  column is excluded from the scan entirely — Parquet projection pushdown
  skips its column chunks, making low-zoom aggregation of large polygon
  datasets (e.g. 225 GB of building footprints with a `bbox` column)
  dramatically cheaper, with no DuckDB pre-step. Also on the Python API as
  `aggregate_a5/h3/admin(..., bucket_point=..., bbox_column=...)`.

- **`--where` row filter on `gpio process aggregate` (#568).** All three
  subcommands (`a5`, `h3`, `admin`) accept a DuckDB WHERE clause that filters
  input rows before aggregation — slice a dataset by year, category, or
  attribute in the same single-command pass, with no pre-filter rewrite.
  The filter applies to the source scan, so counts, `--metric` rollups,
  `--breakdown` pivots, and `--auto` resolution sizing all reflect only the
  matching rows. Semantics and safety validation match `gpio extract --where`;
  also available on the Python API (`aggregate_a5/h3/admin(..., where=...)`).
  On a hive-partitioned input the clause can filter on partition columns
  (`--where "year = 2025"`). A `;` statement separator outside a quoted string
  is now rejected in every `--where` clause (`extract` included), since DuckDB
  executes multi-statement strings.

- **`--metric-nodata` NoData sentinel handling in `gpio process aggregate` (#566).**
  Real-world numeric columns often encode "no value" as a sentinel like `-999`
  instead of SQL `NULL`, which silently poisons `--metric` rollups (an average
  of building heights comes back at `-313 m`). All three subcommands (`a5`,
  `h3`, `admin`) now accept `--metric-nodata "-999"` (comma-separate multiple
  sentinels) to map those values to `NULL` before aggregation: `sum`/`avg`/
  `min`/`max` ignore them while `count` still counts every feature. `nan` is
  accepted for NaN-encoded nodata, and sentinels are compared at the metric
  column's own precision so the classic float32 nodata `-3.4028235e+38`
  matches `REAL` columns. Also on the Python API as
  `aggregate_a5/h3/admin(..., metric_nodata=...)`.

- **New spec-validation checks in `gpio check spec` (#586).** Validation now
  fails on unknown `geo` metadata versions (e.g. `99.0.0`) even in auto mode;
  on version/feature mismatches (1.0 metadata using GeoParquet 2.0 native geo
  types or the 1.1-only `covering` key); on PROJJSON `crs` objects missing the
  required `type` member or using an unknown PROJJSON CRS type; and on a `geo`
  key containing invalid JSON or a non-object value (clean failure instead of
  a crash). Epoch validation is now datum-aware via pyproj: a coordinate
  `epoch` on a datum ensemble (e.g. EPSG:4326, or the CRS84 default) fails, on
  a specific static frame (e.g. GDA2020) warns, and on a dynamic frame (e.g.
  ITRF) passes; an explicit `"crs": null` plus epoch warns since the datum
  cannot be verified.

- **`gpio process aggregate a5`**, **`gpio process aggregate h3`**, and
  **`gpio process aggregate admin`**: aggregate large GeoParquet datasets into A5 grid
  cells, H3 hexagonal cells, or administrative regions with per-bucket `count`,
  `--metric` rollups (`sum`/`avg`/`min`/`max`), and `--breakdown` category pivots, for
  low-zoom visualization. Output geometry is selectable
  (`--out-geometry polygon|centroid|both|none`); every row carries the bucket id
  (`a5_cell`/`h3_cell`/`admin_code`) so `none` output can be re-joined to geometry later.
  Features outside all admin regions go into an `unassigned` bucket. Python API:
  `Table.aggregate_a5`, `Table.aggregate_h3`, `Table.aggregate_admin`, `ops.aggregate_a5`,
  `ops.aggregate_h3`, `ops.aggregate_admin`.

- **`gpio convert reproject --assume-crs84`**: treat a file's unknown (explicit
  `crs: null`) CRS as OGC:CRS84 and rewrite it so the `crs` key is omitted (the
  spec default). No coordinates are changed. Also available as the
  `assume_crs84=` argument on the `reproject` Python API.

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

- **Coordinate/CRS mismatch heuristic downgraded to WARNING.** The
  `gpio check spec` heuristic that flags geographic-looking coordinates (values
  within ±180/±90) under a projected CRS now reports a WARNING instead of
  FAILED, so affected files exit with code `2` (warnings) instead of `1`.
  Callers gating on exit codes should update. The deterministic CRS-consistency
  check for GeoParquet 2.0 native geo statistics still fails on a real
  mismatch.

- **`--geoparquet-version` auto mode preserves the input version (#587,
  #594).** When the flag is omitted, `gpio convert` and `gpio convert
  reproject` now preserve GeoParquet 2.0 inputs (previously silently
  downgraded to 1.1), upgrade bare native-geo Parquet (no `geo` metadata) to
  2.0, and write 1.1 for 1.x inputs. An explicit `--geoparquet-version` always
  wins. The Python API resolves auto the same way, so
  `gpio.read('native.parquet').write('out.parquet')` writes true 2.0 native
  output like the CLI.

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

- **Clear errors for missing `--metric`/`--breakdown` columns in
  `gpio process aggregate`.** Requesting a column that doesn't exist now
  reports the column name and the available columns instead of a raw DuckDB
  binder error — and `--metric count` specifically explains that `count` is
  emitted automatically for every bucket (use `--breakdown` for per-category
  counts). A literal `count` column in the input (e.g. re-aggregating an
  aggregate) still works.

- **Non-planar edges metadata survives rewrites (#588).** `"edges":
  "spherical"` (e.g. from BigQuery GEOGRAPHY extracts) is now preserved across
  `convert`, `extract`, `sort`, `convert reproject`, and `partition`
  rewrites — including remote (S3/GCS/Azure) outputs — instead of being
  silently demoted to planar. GeoParquet 2.0 ellipsoidal edges algorithms
  (`vincenty`, `karney`, `andoyer`, `thomas`) are mapped to `"spherical"` with
  a warning when writing 1.x output; 2.0 outputs keep the algorithm verbatim.
- **Z/M geometry types written and validated dimension-aware (#583, #589).**
  Written `geometry_types` metadata now carries the spec's dimension suffixes
  (`"Point Z"`, `"LineString ZM"`), and `gpio check spec` matches declared
  suffixes against the actual coordinate dimensions in both directions
  (declared-but-absent and present-but-undeclared).
- **`gpio partition … --auto` is now extent-aware (#524).** Auto-resolution
  previously assumed data was spread uniformly across the entire globe, so
  regional/national datasets got a far-too-coarse resolution — collapsing into a
  handful of giant partitions instead of the requested `--target-rows`-sized
  ones (off by ~2 orders of magnitude). It now probes a bounded sample of the
  actual data, counts non-empty cells at each candidate resolution, and picks
  the resolution closest to the target partition count. One fix covers
  `a5`/`h3`/`s2`/`quadkey`; it falls back to the old global estimate when the
  data can't be probed.
- **Spatial operations are now CRS-aware (#525).** `gpio add`/`partition` for the
  lon/lat grids (`a5`, `h3`, `s2`, `quadkey`) and the admin spatial joins
  (`add admin-divisions`, `partition admin`) assumed OGC:CRS84 input. On data in
  another CRS this either errored (admin joins: `ST_Intersects` CRS mismatch) or
  silently produced wrong cells (grids: projected metres keyed as degrees). A
  shared helper now detects the input CRS and reprojects to the operation's
  expected CRS (lon/lat for grids, OGC:CRS84 for admin) before the spatial work;
  CRS84/CRS-less input is untouched, so the common path is unchanged. `add quadkey`
  no longer rejects a known projected CRS — it reprojects instead.
- Partition commands now route all rows in a **single** `COPY … PARTITION_BY`
  scan instead of re-scanning the input per partition value (#478).
  `gpio partition string`, `gpio partition admin`, and the cell-id partitioners
  (`a5`/`h3`/`s2`/`quadkey`/`kdtree`) all shared an
  `O(num_partitions × input_size)` per-value loop that made partitioning large
  datasets into many partitions infeasible (e.g. ~195 country partitions of a
  220 GB input meant ~43 TB of reads). They now stream into a staging dir, then
  rewrite each (small) partition into its final file with correct per-partition
  metadata. Output layout, naming, flags, and per-partition
  `geo`/`bbox`/`covering` metadata (plus passthrough KV like vecorel
  `collection` and non-nullable vecorel columns) are unchanged.
  - Distinct partition values that sanitize to the **same** filename (e.g.
    `"São Paulo"` / `"São-Paulo"`) now raise a clear error instead of silently
    dropping or overwriting rows; values that sanitize to empty fall back to
    `_empty` rather than `.parquet`.
  - `gpio partition admin` warns when features match a coarser admin level but
    are missing a finer one (they cannot be placed in any partition).
  - Note: row order *within* a partition is no longer guaranteed (sort each
    output afterward if needed); with `--no-overwrite` the returned/printed
    partition count reflects only files actually written this run; and when
    partition analysis runs (the default) the input is read once more for that
    cheap aggregate before the COPY.
- Distinguish an explicit `crs: null` (CRS *unknown*) from an omitted `crs` key
  (defaults to OGC:CRS84), per the GeoParquet spec:
  - Reading a file with `crs: null` now logs a warning (once per input) from the
    shared CRS read path, and `gpio check spec` reports it as a WARNING instead
    of incorrectly passing it as "defaults to OGC:CRS84".
  - Reprojecting to a default CRS now omits the `crs` key instead of writing an
    explicit CRS84 object (or, if CRS parsing failed, a stray `null`) into the
    output geo metadata. Affected the Arrow/Python-API and streaming paths.
- Fix out-of-memory crash in `gpio add admin-divisions --dataset overture` on
  large inputs (#461)
- Fix `gpio add admin-divisions`/`gpio partition admin` with the Overture
  dataset producing ~2.6x as many output rows as the input. Overture stores a
  separate maritime (EEZ) polygon per division whose geometry spans the entire
  territory including the landmass, so every land feature matched both the land
  and maritime polygon at each level (~2x for country, ~1.3x for region). The
  per-level caches are now filtered to land polygons (`is_land IS NOT FALSE`),
  making them genuinely non-overlapping so the memory-safe plain spatial join no
  longer multiplies rows. Cache files gain a `-land` suffix, which invalidates
  stale maritime-contaminated caches automatically (the old unsuffixed files are
  also removed on the next download).
  - `gpio partition admin` now joins each level against its own land-only cache
    (chained per-level joins), the same memory-bounded approach as
    `gpio add admin-divisions`; previously it joined the raw remote dataset and
    still multiplied rows / risked OOM.
  - Behavior change: genuinely offshore features (outside any land polygon, e.g.
    buoys or platforms that previously matched the surrounding EEZ) now receive
    no admin code — `ZZ` in `--vecorel` mode, `NULL` otherwise. This is correct
    for land datasets; a maritime opt-in could be added later if needed.
  - Overture now uses a **per-level cache** (separate country and region cache
    files instead of one combined file), with simplified boundaries, to keep
    memory bounded.
  - The spatial join is a plain streaming `LEFT JOIN` again (its original
    design), which spills to disk and scales to multi-million-feature inputs. The
    interim attempt to de-duplicate overlapping border matches inside the join
    used a `QUALIFY ROW_NUMBER()` window; DuckDB's window operator cannot spill,
    so it ran out of memory (~14 GiB) on large files. De-duplication of features
    that straddle overlapping admin boundaries will return later as a dedicated
    operation rather than being folded into this join.
  - Native-geometry inputs (GeoParquet 2.0 and 1.1-geoarrow) use native Parquet
    column statistics for the join; the bbox pre-filter is retained only for
    non-native 1.x inputs that carry a `bbox` column.
  - Admin joins spill to disk via a DuckDB temp directory and run with the
    memory-control settings (single-threaded, no order preservation) DuckDB
    needs to spill reliably.
- Fix out-of-memory crash when writing large results with the default
  `duckdb-kv` write strategy. The Parquet writer no longer buffers the entire
  result in memory to preserve row order; it streams row groups to disk instead.
  Output order is unchanged — single-threaded writes and explicit `ORDER BY`
  clauses (e.g. `gpio sort hilbert`) are preserved.
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

## v1.3.0 (2026-06-11)

### Feat

- **extract arcgis**: add --max-allowable-offset for server-side generalization
- **api**: add output_crs to from_arcgis and extract_arcgis
- **extract arcgis**: add --output-crs CLI option
- **arcgis**: thread output_crs through convert_arcgis_to_geoparquet
- **arcgis**: tag native CRS from returned SR with mismatch warning
- **arcgis**: thread output_crs through streaming and capture returned SR
- **arcgis**: add EsriJSON page-to-table converter via ST_Read
- **arcgis**: request EsriJSON+outSR in fetch_features_page when output_crs set
- **arcgis**: add CRS parsing helpers for output-crs
- **wfs**: add typed exception subclasses for downstream consumers
- add fiboa plugin (gpio fiboa) (#451)
- add Vecorel specification support (#450)
- fetch latest Overture Maps release dynamically (#455)
- **convert**: add --geoparquet-version 1.1-geoarrow output format (#436)
- **extract**: add Carto SQL API extractor

### Fix

- **test**: xfail GeoPackage sequential conversion test on Windows/macOS
- **arcgis**: anchor maxAllowableOffset units by setting outSR
- **arcgis**: unset CRS for unresolvable WKID, resolve native WKT to EPSG
- **partition**: single-pass COPY PARTITION_BY instead of per-value re-scan (#478) (#480)
- **check**: assert dict result to satisfy mypy in optimization check
- **arcgis**: read layer SR from extent/sourceSpatialReference fallbacks
- **check**: scale locality threshold by row-group count, report uncomputed metrics as None
- **arcgis**: normalize WKIDs, validate output-crs upfront, dedupe page converters
- **deps**: update mutmut config for 3.6.0 breaking changes
- **deps**: pin duckdb<1.5.2 for geography extension compatibility
- **check**: use spatial locality metrics instead of row-count heuristic (#456)
- **add**: filter Overture admin caches to land — stop ~2.6x row multiplication (#474)
- **wfs**: handle uint64 overflow in type promotion
- **wfs**: harden schema unification with edge case handling
- **wfs**: handle type mismatches across paginated pages
- **crs**: distinguish null CRS (unknown) from omitted CRS (default) (#471)
- **ci**: repair recurring slow-tests failures on main (#472)
- **add**: admin-divisions OOM fix — restore plain spatial join, retire in-join dedup (#461)
- **add**: restore bbox pre-filter in spatial join ON clause (#460)
- **add**: remove bbox pre-filter from spatial join ON clauses (#457)
- **ci**: separate blocking security checks from proactive CVE alerts
- **carto**: address code review findings
- **carto**: address security and robustness issues in Carto extractor

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
