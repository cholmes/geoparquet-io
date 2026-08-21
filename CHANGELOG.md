# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


This is the first beta release of geoparquet-io 1.0, featuring major new spatial indexing systems, auto-resolution partitioning, comprehensive `--overwrite` support, and significant performance improvements.

### Breaking

- **BREAKING (Python API): CLI and Python API defaults aligned.** `gpio <cmd>`
  and its `geoparquet_io.api` twin are advertised as the same operation, but
  several defaults had silently diverged, so the "same" call did two different
  things. The API now follows the CLI. **These change library behaviour for
  callers that relied on the old defaults** (the CLI is unaffected):
  - `ops.add_admin_divisions` / `Table.add_admin_divisions`: `dataset` now
    defaults to `"gaul"` (was `"overture"`), matching
    `gpio add admin-divisions --dataset`. **This changes output column names**
    — the prefix is derived from the dataset name, so what used to land as
    `overture_country` now lands as `gaul_country`, and downstream
    `df["overture_country"]` raises `KeyError`. The new `prefix` parameter
    (mirroring the CLI's `--prefix`) pins the old names:
    `add_admin_divisions(dataset="overture", prefix="overture")`.
  - `ops.add_admin_divisions` / `Table.add_admin_divisions`: `levels=None` now
    adds **every level the dataset provides** (GAUL: `continent`, `country`,
    `department`), matching the CLI with no `--levels`. It previously added
    `country` only.
  - `Table.partition_by_h3/quadkey/s2/a5/string/kdtree/admin`: `hive` now
    defaults to `False` (was `True`), matching `gpio partition <scheme>
    --hive`, which is off by default. Pass `hive=True` explicitly for
    Hive-style `key=value/` output directories. **With `hive=False` the
    partition value lives only in the file name, so the generated index column
    (`quadkey`, `h3_cell`, `s2_cell`, `a5_cell`, `kdtree_cell`) is dropped from
    the output files.** The new `keep_<scheme>_column` parameters mirror the
    CLI's `--keep-*-column` and restore it without switching to Hive layout.
  - `ops.from_wfs` / `ops.from_wfs_layers` / `Table.from_wfs`: `auto_tile` now
    defaults to `True`, matching `gpio extract wfs`. With it off, a server that
    caps responses (`maxFeatures` / `startIndex` limits) returned a **silently
    truncated** table and reported success; the CLI tiled and fetched
    everything. Pass `auto_tile=False` to opt back out.
  - `Table.from_wfs`: `page_size` now defaults to `100000` (was `10000`),
    matching `ops.from_wfs` and `gpio extract wfs --page-size`. Every entry
    point — the CLI option, `wfs_to_table`, `convert_wfs_to_geoparquet`,
    `convert_wfs_layers_to_directory`, `fetch_all_features_duckdb`,
    `_fetch_with_spatial_tiles` and both API wrappers — now references a single
    `DEFAULT_WFS_PAGE_SIZE` constant in `core/wfs.py`.
  - `Table.check_spatial`: `limit_rows` now defaults to `500000` (was
    `100000`), matching `gpio check spatial --limit-rows`; the API previously
    analysed 5x fewer rows and could report a different verdict on the same
    file.
  - `Table.partition_by_string/kdtree/admin`: `compression_level` now defaults
    to `None` (was a hardcoded `15`), letting each codec pick its own default.
    The pinned value made every non-ZSTD codec raise —
    `partition_by_string(..., compression="GZIP")` failed with "GZIP
    compression level must be between 1 and 9, got 15" while the equivalent
    CLI command succeeded.

  `tests/test_cli_api_default_parity.py` now walks every Click command, resolves
  its API twin and diffs the two default sets, so a *new* mismatch fails the
  suite rather than waiting to be noticed.

### Added

- **CLI surface regression test (#664)**: `tests/test_cli_surface.py` walks the
  whole Click command tree into a structural snapshot at
  `tests/data/cli_surface.json` — every group, command, option and argument
  with its opts, its *secondary* opts, type, default, `required`, `is_flag` and
  `multiple` — and fails naming the exact field that drifted. Recording
  `--warmup` and `--no-warmup` in separate fields matters: concatenated, a
  boolean flag pair is indistinguishable from two aliases for the same switch,
  which is a user-visible behavior change. Help prose is deliberately not
  pinned. Plugin-contributed commands are excluded, so an installed plugin
  cannot break the test. Intentional changes are accepted by re-recording with
  `GPIO_UPDATE_SNAPSHOT=1 uv run pytest tests/test_cli_surface.py` — honored
  only for an affirmative value, so `GPIO_UPDATE_SNAPSHOT=0` means no, and
  refused outright when `CI` is set so a stray env var cannot rewrite the
  baseline green. The snapshot also pins each group's argv rewriting: the
  subcommand a default-dispatch group falls back to (`check` → `all`, `inspect`
  → `summary`) and the output-extension map `gpio convert` dispatches on
  (`.gpkg` → `geopackage`, `.fgb` → `flatgeobuf`, ...). Both lived only in a
  closure and were previously unobservable. Companion parametrized tests render
  `--help` for every built-in leaf command and every group. The snapshot
  comparison needs click >= 8.2's `UNSET` sentinel to distinguish an undeclared
  default from an explicit `None`, and skips below that; the help-render cases
  still run.

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

#### Testing
- Geo-metadata reader agreement suite (`tests/test_geo_metadata_parity.py`).
  Pins the current behavior of the five `geo` metadata call sites — four
  distinct implementations, since `core/add/quadkey.py` is now a one-line
  delegate — so the planned metadata consolidation is verifiable: all five must
  produce an identical parsed dict on every geo fixture in `tests/data/` and on
  a representative slice of the geoparquet-testing corpus. Edge cases where the
  readers legitimately diverge (str-keyed metadata, invalid JSON, absent key,
  empty dict) are pinned per reader rather than forced equal, and the
  CRS-equality trio (`validate._crs_equals`, `validate._is_crs84_equivalent`,
  `inspect_utils._crs_are_equivalent`) is pinned over a 5x5 CRS matrix with the
  cells where the helpers disagree recorded as a consolidation decision list.
  Three cells contradict the GeoParquet specification rather than merely
  disagreeing with each other — an explicit `"crs": null` reported as CRS84, an
  omitted `crs` reported as unequal to OGC:CRS84, and OGC:CRS84 reported as
  unequal to EPSG:4326 — and are recorded as `xfail(strict=True)` cases
  asserting the spec-correct verdict, tracked by #699. Also covers the
  pyproj-fallback axis-order split between the two binary helpers and the
  boundary behavior of `validate._version_at_least`.

### Changed

- **Local `pytest` runs are no longer instrumented for coverage (#665).**
  `--cov`/`--cov-fail-under` moved out of `addopts` in `pyproject.toml` and into
  the CI job that actually reports coverage. A plain `uv run pytest` is now fast
  (39-49% quicker on a single file) and exits 0 instead of failing a whole-suite
  67% gate that a partial run could never clear; pass `--cov=geoparquet_io
  --cov-report=term-missing --cov-fail-under=0` to opt in (the `0` matters:
  `[tool.coverage.report].fail_under` re-arms the 67% floor on any `--cov` run).
  CI is unchanged in strictness: the ubuntu/3.11 fast-test job still enforces the
  67% floor, uploads to Codecov, and runs the 90% diff-cover gate. The other nine
  matrix jobs (and the two non-reporting slow-suite jobs) no longer compute
  coverage they discarded, and `fetch-depth: 0` is now limited to the one job
  that needs git history. Note the 39-49% figure is a local single-file
  measurement; total CI matrix wall time was unchanged within noise. Which leg
  reports coverage is now decided once, by a job-level `COVERAGE_JOB` flag that
  the four dependent steps read, so moving the baseline Python version can no
  longer silently disable both coverage gates while every check stays green
  (`tests/test_coverage_job.py` asserts the flag names a combination the matrix
  actually schedules).

- **Repo-tooling tests moved to a `meta` lane, out of the fast suite (#665).**
  The codespell, commitizen, doc-sync, mutmut, mypy, validate-CLAUDE.md and
  security-tool-availability checks now carry `@pytest.mark.meta`. They spawn
  `uv run` subprocesses and were the fast suite's only source of contention
  flakes. Most of them re-run something CI checks elsewhere — codespell,
  doc-sync, mypy and validate-claude-md are pre-commit hooks the lint job runs,
  and the bandit/pip-audit availability checks are covered for real by the
  `security` job — while the commitizen and mutmut tests validate configuration
  that no hook in the lint job touches (commitizen is a `commit-msg`-stage hook,
  mutmut has no hook at all), which is why the lane still runs in CI rather than
  being deleted. The fast selection is now
  `-m "not slow and not network and not meta"` and the slow/nightly job runs
  `-m "(slow or meta) and not network"` with no file-level exclusions, so
  nothing stops being checked in CI. Run them locally with
  `uv run pytest -m meta`. `tests/test_validate_claude_md.py` also drops five of
  its seven `uv run` spawns in favour of calling the validator's `main()` in
  process, keeping the happy path and one detected-error path as real
  subprocesses.

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

- **`tests/test_wfs.py` no longer reaches the network.** Nine tests made live
  requests against the fake host. Six exercise `wfs_to_table` and mocked the
  version negotiation, layer lookup and feature fetch, but not
  `_get_feature_count` — which `wfs_to_table` calls twice before the mocked
  fetch as part of its auto-tiling probe. Three more, in
  `TestAutoPageSingleWorker`, call `fetch_all_features_duckdb` directly, which
  probes the server's startIndex support before paging; that probe drives httpx
  itself, so mocking the page fetcher never covered it. Both `_get_feature_count`
  and `_probe_startindex_limit` swallow every exception, so all nine passed
  while silently making live requests, contrary to the module's stated "mocked
  HTTP responses to avoid network dependencies". A shared `offline_wfs_probes`
  fixture now completes the mock and installs a tripwire on `_make_request` that
  fails the test if a future unmocked request escapes through that path. No
  assertion changed meaning — the stubs return the `None` the failed requests
  already produced. Measured over the file's fast lane: 12 outbound connection
  attempts and 24 `time.sleep` calls totalling 36.00s of retry backoff both drop
  to zero, and serial runtime goes from 43.4s to 6.3s.
- **Six internal DuckDB connections now route through the shared connection
  factory.** `benchmark_duckdb`, `get_file_info`, `wkb_to_wkt_preview`,
  `get_column_statistics`, `add country-codes`'s connection setup, and the
  disk-rewrite write strategy previously called bare `duckdb.connect()` and
  re-applied session settings by hand, skipping `get_duckdb_connection()`.
  All six now gain `arrow_large_buffer_size` (required for >2GB string/WKB
  Arrow exports), which none of them had set, alongside the
  `geometry_always_xy` axis-order setting they had each been reimplementing
  inline. `gpio inspect stats` also now loads httpfs when the file it is
  inspecting lives on cloud storage, matching `gpio inspect head`.
  To prevent regressions, the `duckdb-antipatterns` pre-commit check now bans
  `duckdb.connect(`, `.sql(`, `.query(`, `.execute(` and `.read_parquet(`
  outside `core/duckdb_utils.py` — including via an aliased or
  `from duckdb import ...` import, which would otherwise slip past the check.
  A trailing `# allow-bare-connect` comment marks a deliberate exception.
- **Library-safe writes: no global state leaks.** Side effects that made `gpio`
  unsafe to embed in a host application are fixed. (1) `write_parquet_with_metadata()`
  no longer mutates a caller-supplied `extra_kv_metadata` dict in place — a dict
  reused across writes (e.g. a partition loop) no longer accumulates preserved
  keys from prior files — and caller-supplied keys now explicitly win over keys
  preserved from the input file. (2) `.write(profile=...)` and
  `.upload(profile=...)` to S3 no longer set `AWS_PROFILE` in the host process
  environment at all, including the non-parquet (`.fgb`/`.gpkg`/`.csv`/`.shp`/
  `.geojson`) write path. The profile was already handed explicitly to the
  uploader, which resolves credentials from it directly, so the environment
  mutation was redundant as well as leaky — and, being an unlocked
  process-global, wrong under concurrency. The CLI's existing save/restore
  behavior is unchanged. (3) `configure_verbose()` no longer stamps a level onto
  the shared `geoparquet_io` logger: a default (`verbose=False`) call leaves a
  level the host application chose untouched, and a nested default call no
  longer truncates the output of an outer `--verbose` run. (4) The first
  library call no longer hijacks a host application's logging: when the host
  has already configured logging, `gpio` attaches a `logging.NullHandler` and
  propagates instead of installing its own stream handler, so messages are no
  longer emitted twice. (5) Two environment variables are no longer set
  permanently by a library call: the ArcGIS reader lifts GDAL's per-feature
  GeoJSON size cap (`OGR_GEOJSON_MAX_OBJ_SIZE`) only for the duration of the
  read that needs it, and the unused `get_bigquery_connection()` helper — which
  set `GOOGLE_APPLICATION_CREDENTIALS` with no restore — is removed in favour of
  the `BigQueryConnection` context manager the extract path already used.
- **`--write-memory` now honored by every command that offers it (previously
  silently ignored on twelve of them).** `add h3`/`a5`/`s2`/`kdtree`/`quadkey`/
  `bbox`/`geometry-metrics`/`admin-divisions`, `sort column`/`quadkey`,
  `convert geoparquet` and `extract bigquery` accepted the flag but never
  forwarded it, so the value was dropped and the auto-detected default limit
  was used instead — the same bug class fixed for `sort hilbert` in #627. The
  flag is now forwarded by all 22 commands that expose it (`convert
  geoparquet`/`reproject`, `extract geoparquet`/`bigquery`, `sort
  hilbert`/`column`/`quadkey`, `add admin-divisions`/`geometry-metrics`/`bbox`/
  `h3`/`a5`/`s2`/`kdtree`/`quadkey`, and all seven `partition` subcommands).
  It remains absent — by design — from commands with no DuckDB write engine to
  configure: `convert geojson`/`geopackage`/`flatgeobuf`/`csv`/`shapefile`,
  `extract arcgis`/`wfs`/`carto`, `add bbox-metadata`, `pmtiles
  create`/`pyramid`, `process overview`/`aggregate`, and `publish
  stac`/`upload`. On `extract bigquery` the limit is applied to the DuckDB
  connection that runs the BigQuery scan (the memory-heavy step); the Parquet
  write itself is a PyArrow write.

- **`--write-memory` is validated instead of reaching SQL unchecked.** The
  value is interpolated into DuckDB's `SET memory_limit = '…'` (a SET value
  cannot be parameterised), and DuckDB executes multi-statement strings, so an
  unvalidated value could append arbitrary SQL — reachable from any library
  caller passing a config-supplied `memory_limit`. Values must now match a
  plain size literal (`512MB`, `2GB`, `4.5GB`, `1GiB`); anything else is a
  clean Click parameter error rather than a raw DuckDB `ParserException`
  traceback.

- **`--write-memory` no longer aborts with `--geoparquet-version
  1.1-geoarrow`.** That version reroutes WKB input to the arrow-streaming
  write strategy, which cannot honour a memory limit; the combination raised a
  raw `ValueError` traceback on `sort hilbert` (since #627) and on the seven
  commands above. gpio now warns and ignores the limit when *it* rerouted the
  strategy, and reports a clean parameter error only when the user explicitly
  chose an incompatible `--write-strategy`. Streaming Arrow IPC to stdout also
  warns rather than dropping the limit silently.
- **Stale geo metadata after reproject, filtered extract, and multi-file
  merges.** The per-column `bbox` and `geometry_types` are derived from the
  data, so every path that changes which rows or coordinates get written now
  invalidates them and lets the write machinery recompute them from the output:

  - `gpio convert reproject` no longer carries the input's degree-space
    collection `bbox` into output reprojected to another CRS.
  - `gpio extract` with a row filter (`--bbox`/`--geometry`/`--where`/`--limit`)
    no longer advertises the full pre-filter extent. A zero-row extract omits
    `bbox` entirely (it is optional per spec) rather than claiming a non-empty
    one.
  - `gpio extract` over a glob or directory no longer stamps the FIRST input
    file's `bbox`/`geometry_types` on the merged output. That was an
    *under-covering* bbox, which is worse than an over-covering one: conformant
    readers skip data that falls outside it.
  - `geometry_types` (required in GeoParquet 1.1) is recomputed alongside
    `bbox` rather than being carried through unchanged — filtering out the only
    polygon no longer leaves `["Point", "Polygon"]` behind.
  - `--write-strategy streaming` now emits a `bbox` for GeoParquet 1.0/1.1
    output instead of only for 2.0. (This strategy is also selected implicitly
    for `1.1-geoarrow`.)

  An unfiltered, untransformed single-file copy still preserves its carried
  stats. Both file-based and streaming/stdout paths are fixed.

  Recomputing costs one extra aggregate scan of the (already filtered) query —
  measured ~33% slower on a 3M-row/50MB file (0.96s vs 0.72s) — and reproject
  runs `ST_Transform` over every row a second time to do it. `bbox` and
  `geometry_types` are now derived in a single grouped scan rather than two
  independent ones, which claws back part of that.

- **`gpio extract --exclude-cols` left dangling geo metadata.** Dropping the
  `bbox` column left the `covering` entry pointing at a column that is no
  longer in the schema (`gpio check spec` failed on the result); dropping the
  geometry column produced a file that still advertised itself as GeoParquet
  with a geometry column that did not exist. Geo metadata entries and
  `covering` references are now pruned to the output schema, and an output with
  no geometry column is written as plain Parquet with a warning.

- **`gpio extract` metadata-preservation failures are visible.** `extract` now
  warns instead of silently dropping all `geo`/KV metadata when the input
  footer cannot be read for preservation, for any failure mode (a corrupt
  footer can surface as `OSError`, `GeoParquetError`, or an Arrow exception).
- **Geometry-column identifiers are now quoted at every raw SQL interpolation
  (todo #008).** Files whose primary geometry column name has a space,
  uppercase letter, reserved word, or embedded quote — a name read verbatim
  from the file's own `geo.primary_column` metadata — previously crashed
  several commands with a DuckDB `ParserException`, since the column name was
  interpolated unquoted into a generated SQL identifier. Fixed in `stream_io`'s
  WKB-conversion wrapper and across every `add bbox` code path — the CLI's
  file-based `STRUCT_PACK` expression, its stdin/stdout streaming query
  builder, the `Table.add_bbox()` / `add_bbox_table()` Python-API path, and
  the shared `add_bbox` helper used by `admin-divisions`/`country-codes` —
  plus `check spatial`'s sampling-method queries. All now use the existing
  `quote_identifier` helper, which also closes a second, narrower gap: two of
  the `add bbox` builders were already hand-quoting with `"{col}"` and so
  tolerated spaces, but didn't double an embedded `"`, which still broke
  them. Separately, the native Parquet-geo-stats getters in `duckdb_metadata`
  (and sibling `bbox`/`compression` lookups) compared the column name as a
  SQL **string literal** (`WHERE path_in_schema = '...'`) rather than an
  identifier; a space there was always harmless, but an embedded `'` broke
  the literal and was silently swallowed by a broad `except Exception`,
  returning `None`/empty stats instead of raising or the correct value.
  Fixed with `_escape_sql_string`. Since a hostile filename can carry such a
  name via `geo.primary_column`, this was also a latent SQL-injection vector,
  not just a crash bug.
- **Commands no longer break on unusual column names or paths with an
  apostrophe.** A geometry column named `geom col`, `Geometry` or `geo"m` — a
  name read verbatim from the file's own `geo.primary_column` — used to crash
  `add bbox/h3/s2/a5/quadkey/geometry-metrics/kdtree`, `inspect stats`,
  `sort hilbert`, `sort column`, `extract geoparquet` and `convert
  geojson/csv` with a DuckDB parser error, and made `check spec` report a
  valid file as failing. Column names are now quoted wherever they are
  interpolated into SQL, so a crafted file can no longer inject SQL through
  its own metadata, and `--column`/`--bbox-name` accept any name the format
  allows. Separately, a file under a directory containing `'` (e.g.
  `o'brien/data.parquet`) was escaped twice and reported as not found by
  `check`, `inspect`, `add bbox` and `convert`; paths are now escaped exactly
  once.
- **`--where` validation is now parser-based, and reaches `gpio extract
  bigquery` / `gpio extract carto`.** Three fixes to the shared
  `--where` guard:

    - The statement gate no longer walks the clause looking for an
      unquoted `;`. That walker only understood `'` and `"`, so a `;`
      hidden behind dollar quoting (`$$'$$`), a block comment, a line
      comment, or an `E'\''` escape slipped through and executed as extra
      statements. The clause is now composed into the exact `WHERE
      (<clause>\n)` shape the callers emit and handed to DuckDB's parser;
      anything that parses as more than one statement — or does not parse
      at all — is rejected.
    - Blocklisted keywords are matched against real SQL word tokens
      instead of the uppercased clause text, so ordinary filters that
      merely contain one inside a string literal are accepted again:
      `name = 'Grant County'`, `street LIKE '%Alter Markt%'`,
      `descr ILIKE '%drop off%'`, `status = 'DELETE'`, `name = 'Merge
      Lane'`. `REPLACE` is no longer blocklisted at all — it is a standard
      scalar function in DuckDB, BigQuery and Postgres/Carto, and a
      function call cannot modify data.
    - `gpio extract bigquery` and `gpio extract carto` now run their
      `--where` through that check (on the dry-run/build path too, before
      any network request), and build their SQL through the shared
      condition helper. Previously they inlined `({where})` on one line, so
      a clause ending in `--` commented out everything after it: a
      `--bbox`-and-`--limit` request silently became a full-table
      download. The bbox condition and the `LIMIT` now survive.

  Scope, stated plainly: this guard is a safety net for **trusted** input,
  not a security boundary. It stops a clause from becoming additional
  statements; it does not stop abuse that stays inside a single expression
  (e.g. `1=1 AND length((SELECT content FROM read_text('/etc/hosts')))>0`
  still passes). `gpio extract arcgis --where` remains unvalidated by
  design — it is sent as an HTTP query parameter to the Feature Service,
  never interpolated into a local SQL statement.

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
