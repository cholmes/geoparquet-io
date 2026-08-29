# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## Unreleased

A test-infrastructure overhaul, aggregate overviews and PMTiles pyramids, and a
long run of write-path, metadata and CRS correctness fixes.

### Breaking

- **BREAKING (Python API): CLI and Python API defaults aligned.** Several `geoparquet_io.api` defaults had silently diverged from their CLI twins; the API now follows the CLI, and a parity test fails on any new mismatch. ([#661](https://github.com/geoparquet/geoparquet-io/issues/661))
  - `ops.add_admin_divisions` / `Table.add_admin_divisions`: `dataset` now defaults to `"gaul"` (was `"overture"`), changing output column prefixes; `dataset="overture", prefix="overture"` restores the old names.
  - `ops.add_admin_divisions` / `Table.add_admin_divisions`: `levels=None` now adds every level the dataset provides (was `country` only).
  - `Table.partition_by_h3/quadkey/s2/a5/string/kdtree/admin`: `hive` now defaults to `False` (was `True`); pass `hive=True`, or `keep_<scheme>_column=True` to keep the index column without Hive layout.
  - `ops.from_wfs` / `ops.from_wfs_layers` / `Table.from_wfs`: `auto_tile` now defaults to `True` (was off, which silently truncated results from capped servers); pass `auto_tile=False` to opt out.
  - `Table.from_wfs`: `page_size` now defaults to `100000` (was `10000`), matching `ops.from_wfs` and the CLI.
  - `Table.check_spatial`: `limit_rows` now defaults to `500000` (was `100000`), matching the CLI.
  - `Table.partition_by_string/kdtree/admin`: `compression_level` now defaults to `None` (was a hardcoded `15` that broke every non-ZSTD codec).

### Added

- **The guides' examples now run as tests.** Every fenced bash/python block in `docs/guide/*.md` runs verbatim as a pytest item, which found and fixed real bugs in the docs. ([#667](https://github.com/geoparquet/geoparquet-io/issues/667), [#732](https://github.com/geoparquet/geoparquet-io/issues/732))
- **Honest notebook CI signal.** The `notebooks` job no longer reports green for notebooks that execute no real `gpio` call. ([#667](https://github.com/geoparquet/geoparquet-io/issues/667), [#720](https://github.com/geoparquet/geoparquet-io/issues/720))
- **Canonical sample dataset.** `tests/data/canonical/` holds one small, spec-clean dataset (~405 KB) shared by the docs examples, e2e journeys and notebooks. ([#667](https://github.com/geoparquet/geoparquet-io/issues/667), [#719](https://github.com/geoparquet/geoparquet-io/issues/719))
- **End-to-end journey matrix.** Ten multi-command user journeys run through the installed `gpio` binary, each ending in `gpio check all`. ([#722](https://github.com/geoparquet/geoparquet-io/issues/722), [#723](https://github.com/geoparquet/geoparquet-io/issues/723), [#667](https://github.com/geoparquet/geoparquet-io/issues/667), [#725](https://github.com/geoparquet/geoparquet-io/issues/725))
- **CLI surface regression test.** The whole Click command tree is pinned in a structural snapshot; re-record intentional changes with `GPIO_UPDATE_SNAPSHOT=1`. ([#664](https://github.com/geoparquet/geoparquet-io/issues/664), [#679](https://github.com/geoparquet/geoparquet-io/issues/679))
- **Spatial-index golden-value tests.** Pins the cell values every index family assigns to a fixed fixture, identically across the add, partition, streaming and sort paths. ([#664](https://github.com/geoparquet/geoparquet-io/issues/664), [#682](https://github.com/geoparquet/geoparquet-io/issues/682))
- **CLI↔API parity harness.** Diffs what `gpio <cmd>`, `ops.<fn>` and `Table.<method>` hand to core for 13 commands. ([#664](https://github.com/geoparquet/geoparquet-io/issues/664), [#685](https://github.com/geoparquet/geoparquet-io/issues/685))
- **Every CLI command must have a Python API twin.** The rule is now test-enforced. ([#664](https://github.com/geoparquet/geoparquet-io/issues/664))
- **Write-contract characterization suite.** Pins what gpio's write paths put on disk; building it surfaced six write-path defects, all fixed in this release. ([#686](https://github.com/geoparquet/geoparquet-io/issues/686)–[#691](https://github.com/geoparquet/geoparquet-io/issues/691), [#664](https://github.com/geoparquet/geoparquet-io/issues/664), [#693](https://github.com/geoparquet/geoparquet-io/issues/693))
- **`gpio pmtiles pyramid`.** Bake an aggregate and its overview levels into a single zoom-banded PMTiles archive; Python API `ops.create_pmtiles_pyramid`. ([#570](https://github.com/geoparquet/geoparquet-io/issues/570), [#615](https://github.com/geoparquet/geoparquet-io/issues/615))
- **`gpio process overview`.** Derive coarser aggregate levels from an existing aggregate output; Python API `ops.create_overviews`, `Table.overview`. ([#570](https://github.com/geoparquet/geoparquet-io/issues/570), [#615](https://github.com/geoparquet/geoparquet-io/issues/615))
- **`--bucket-point` on `gpio process aggregate`.** Key buckets from a bbox covering or point column instead of the geometry centroid. ([#567](https://github.com/geoparquet/geoparquet-io/issues/567), [#614](https://github.com/geoparquet/geoparquet-io/issues/614))
- **`--where` row filter on `gpio process aggregate`.** All three subcommands accept a DuckDB WHERE clause on input rows. ([#568](https://github.com/geoparquet/geoparquet-io/issues/568), [#612](https://github.com/geoparquet/geoparquet-io/issues/612))
- **`--metric-nodata` sentinel handling in `gpio process aggregate`.** Map sentinel values like `-999` to `NULL` before aggregation. ([#566](https://github.com/geoparquet/geoparquet-io/issues/566), [#613](https://github.com/geoparquet/geoparquet-io/issues/613))
- **New spec-validation checks in `gpio check spec`.** Fails on unknown `geo` versions, version/feature mismatches, malformed PROJJSON `crs` and invalid `geo` JSON. ([#586](https://github.com/geoparquet/geoparquet-io/issues/586))
- **`gpio process aggregate a5/h3/admin`.** Aggregate large datasets into A5, H3 or admin-region buckets with `count`, `--metric` rollups and `--breakdown` pivots. ([#529](https://github.com/geoparquet/geoparquet-io/issues/529))

#### Testing
- **Geo-metadata reader agreement suite.** Pins the five `geo`-metadata call sites and the CRS-equality helpers against shared fixtures. ([#699](https://github.com/geoparquet/geoparquet-io/issues/699), [#664](https://github.com/geoparquet/geoparquet-io/issues/664), [#681](https://github.com/geoparquet/geoparquet-io/issues/681))

### Changed

- **Local `pytest` runs are no longer instrumented for coverage.** `--cov` moved out of `addopts` into the one CI job that reports coverage; plain `uv run pytest` is 39–49% faster. ([#665](https://github.com/geoparquet/geoparquet-io/issues/665), [#677](https://github.com/geoparquet/geoparquet-io/issues/677))
- **Repo-tooling tests moved to a `meta` lane, out of the fast suite.** Run them locally with `uv run pytest -m meta`. ([#665](https://github.com/geoparquet/geoparquet-io/issues/665), [#684](https://github.com/geoparquet/geoparquet-io/issues/684))
- **Coordinate/CRS mismatch heuristic downgraded to WARNING.** Affected files now exit `2` instead of `1`; callers gating on exit codes should update. ([#586](https://github.com/geoparquet/geoparquet-io/issues/586))
- **`--geoparquet-version` auto mode preserves the input version.** 2.0 inputs stay 2.0 (previously silently downgraded to 1.1), bare native-geo Parquet upgrades to 2.0, 1.x inputs write 1.1. ([#587](https://github.com/geoparquet/geoparquet-io/issues/587), [#594](https://github.com/geoparquet/geoparquet-io/issues/594))

### Fixed

- **`gpio convert geojson - out.geojson` now reads stdin into a named output file** instead of failing with `File not found: -`. The other four converters still reject `-`; they have no stdin-consuming path to route to ([#749](https://github.com/geoparquet/geoparquet-io/issues/749)). ([#723](https://github.com/geoparquet/geoparquet-io/issues/723))
- **`convert geojson --write-bbox` and `--id-field` no longer truncate every Feature,** and no longer abort the whole conversion on a NULL id value or an EMPTY geometry — each member is omitted instead. ([#726](https://github.com/geoparquet/geoparquet-io/issues/726))
- **Windows native-geo statistics: the zeros are a read, not a write.** pyarrow reads the real bounds from the same file DuckDB's `parquet_metadata()` reports as `[0, 0, 0, 0]`, so files gpio writes on Windows are correct and it is gpio's own reader that misreports them. ([#721](https://github.com/geoparquet/geoparquet-io/issues/721), [#748](https://github.com/geoparquet/geoparquet-io/issues/748))
- **Guide examples no longer use flags the CLI does not accept.** Nonexistent options are corrected and missing option-table rows filled in. ([#735](https://github.com/geoparquet/geoparquet-io/issues/735))
- **`tests/test_wfs.py` no longer reaches the network.** Nine tests silently made live requests; a tripwire now fails any unmocked request. ([#676](https://github.com/geoparquet/geoparquet-io/issues/676))
- **`gpio add quadkey --quadkey-name` now accepts any column name Parquet accepts.** Output and bbox column names now go through `quote_identifier()`. ([#695](https://github.com/geoparquet/geoparquet-io/issues/695))
- **Validation accepts the GeoArrow encodings GeoParquet 1.1 permits,** so gpio's own `1.1-geoarrow` output passes its own spec check; 1.0 and 2.0 stay WKB-only per spec. ([#691](https://github.com/geoparquet/geoparquet-io/issues/691), [#715](https://github.com/geoparquet/geoparquet-io/issues/715))
- **`gpio extract wfs` no longer hides a failed feature-count probe** that silently disabled auto-tiling and pagination. ([#696](https://github.com/geoparquet/geoparquet-io/issues/696))
- **`--row-group-rows` / `--row-group-size-mb` now apply to the `disk-rewrite` write strategy,** which accepted both options and used neither. ([#689](https://github.com/geoparquet/geoparquet-io/issues/689), [#698](https://github.com/geoparquet/geoparquet-io/issues/698))
- **GeoParquet 1.0 output no longer carries the 1.1-only `covering` key;** `gpio add bbox-metadata` fails with a clear error on a 1.0 input. ([#686](https://github.com/geoparquet/geoparquet-io/issues/686), [#714](https://github.com/geoparquet/geoparquet-io/issues/714))
- **`write_geoparquet_table` honors an explicit `geoparquet_version="parquet-geo-only"`,** dropping the input's `geo` key and stale `ARROW:schema`. ([#687](https://github.com/geoparquet/geoparquet-io/issues/687), [#702](https://github.com/geoparquet/geoparquet-io/issues/702))
- **The arrow-streaming write strategy no longer emits a different file depending on what the process imported;** geometry encoding is normalized per target version. ([#688](https://github.com/geoparquet/geoparquet-io/issues/688), [#707](https://github.com/geoparquet/geoparquet-io/issues/707))
- **`gpio convert geoparquet` and `Table.write` keep the input's non-geo key/value metadata** (`fiboa`, `vecorel`, STAC fragments) across every entry point and write strategy. ([#690](https://github.com/geoparquet/geoparquet-io/issues/690), [#710](https://github.com/geoparquet/geoparquet-io/issues/710))
- **GeoParquet 2.0 output no longer drops the `bbox` covering.** The fast path now carries a declared covering verbatim, keeping the native GEOMETRY type and its statistics; a covering is written only for a column gpio computed, one the input declared, or a carried conventional `bbox`, so an unrelated `tile_bounds` column can no longer become one. ([#738](https://github.com/geoparquet/geoparquet-io/issues/738), [#744](https://github.com/geoparquet/geoparquet-io/issues/744))
- **`--compression-level` is no longer silently ignored on the `duckdb-kv` write path,** where output fell back to DuckDB's ZSTD default of 3 against gpio's default of 15; it is now validated before reaching SQL. ([#744](https://github.com/geoparquet/geoparquet-io/issues/744))
- **A write no longer leaves `threads`, `preserve_insertion_order` and `memory_limit` clamped on the caller's DuckDB connection,** which had throttled every later query on it. ([#744](https://github.com/geoparquet/geoparquet-io/issues/744))
- **Six internal DuckDB connections now route through the shared connection factory;** bare `duckdb.connect()` is now banned outside `core/duckdb_utils.py`. ([#659](https://github.com/geoparquet/geoparquet-io/issues/659))
- **Library-safe writes: no global state leaks.** Embedding gpio no longer mutates caller dicts, sets env vars in the host process, or hijacks host logging. ([#660](https://github.com/geoparquet/geoparquet-io/issues/660))
- **`--write-memory` is now honored by every command that offers it;** twelve commands accepted the flag but never forwarded it. ([#663](https://github.com/geoparquet/geoparquet-io/issues/663))
- **`--write-memory` is validated instead of reaching SQL unchecked;** anything but a plain size literal (`512MB`, `2GB`) is a clean parameter error. ([#663](https://github.com/geoparquet/geoparquet-io/issues/663))
- **`--write-memory` no longer aborts with `--geoparquet-version 1.1-geoarrow`;** gpio warns and ignores the limit when it rerouted the strategy itself. ([#663](https://github.com/geoparquet/geoparquet-io/issues/663))
- **Stale geo metadata after reproject, filtered extract, and multi-file merges.** Derived `bbox` and `geometry_types` are recomputed on every path that changes rows or coordinates. ([#658](https://github.com/geoparquet/geoparquet-io/issues/658))
- **`gpio extract --exclude-cols` left dangling geo metadata;** covering entries and geometry references are now pruned to the output schema. ([#658](https://github.com/geoparquet/geoparquet-io/issues/658))
- **`gpio extract` metadata-preservation failures are visible;** an unreadable footer now warns instead of silently dropping all `geo`/KV metadata. ([#658](https://github.com/geoparquet/geoparquet-io/issues/658))
- **Geometry-column identifiers are now quoted at every raw SQL interpolation,** closing crashes and a latent injection vector via `geo.primary_column`. ([#662](https://github.com/geoparquet/geoparquet-io/issues/662))
- **Commands no longer break on unusual column names or paths with an apostrophe;** names are now quoted and paths escaped exactly once. ([#662](https://github.com/geoparquet/geoparquet-io/issues/662))
- **`--where` validation is now parser-based, and reaches `gpio extract bigquery`/`carto`,** so `name = 'Grant County'` passes again and a trailing `--` cannot comment out the bbox filter. ([#657](https://github.com/geoparquet/geoparquet-io/issues/657))
- **Clear errors for missing `--metric`/`--breakdown` columns in `gpio process aggregate`** instead of a raw DuckDB binder error. ([#617](https://github.com/geoparquet/geoparquet-io/issues/617))
- **Non-planar edges metadata survives rewrites;** `"edges": "spherical"` is preserved across convert, extract, sort, reproject and partition. ([#588](https://github.com/geoparquet/geoparquet-io/issues/588))
- **Z/M geometry types written and validated dimension-aware;** metadata carries the spec's dimension suffixes (`"Point Z"`), checked in both directions. ([#583](https://github.com/geoparquet/geoparquet-io/issues/583), [#589](https://github.com/geoparquet/geoparquet-io/issues/589))
- **`gpio partition … --auto` is now extent-aware,** probing a sample of the actual data instead of assuming globally uniform coverage. ([#524](https://github.com/geoparquet/geoparquet-io/issues/524), [#526](https://github.com/geoparquet/geoparquet-io/issues/526))
- **Spatial operations are now CRS-aware;** non-CRS84 inputs are detected and reprojected before grid operations and admin joins. ([#525](https://github.com/geoparquet/geoparquet-io/issues/525), [#530](https://github.com/geoparquet/geoparquet-io/issues/530))

### Internal

- **The `disk-rewrite` metadata rewrite can now merge row groups, not only split them.** It issued one `write_table` per *source* row group, and each of those starts a new group, so a `row_group_rows` request larger than the source's groups returned the source's shape. Reachable only through the helper today — the query path pre-sizes its temporary file through DuckDB's `ROW_GROUP_SIZE`, which is what masked it — and the "coarsen" direction is now pinned for all four strategies in the write-contract suite. ([#697](https://github.com/geoparquet/geoparquet-io/issues/697), [#757](https://github.com/geoparquet/geoparquet-io/issues/757))
- Reduced complexity in 6 functions from Grade E/D to Grade C
- Comprehensive test coverage improvements
- Plugin system documentation
- Dependency updates (actions/checkout v6, astral-sh/setup-uv v7, etc.)

## v1.3.0 (2026-06-11)

### Feat

- **extract arcgis**: add --max-allowable-offset for server-side generalization ([#484](https://github.com/geoparquet/geoparquet-io/issues/484))
- **api**: add output_crs to from_arcgis and extract_arcgis
- **extract arcgis**: add --output-crs CLI option
- **arcgis**: thread output_crs through convert_arcgis_to_geoparquet
- **arcgis**: tag native CRS from returned SR with mismatch warning
- **arcgis**: thread output_crs through streaming and capture returned SR
- **arcgis**: add EsriJSON page-to-table converter via ST_Read
- **arcgis**: request EsriJSON+outSR in fetch_features_page when output_crs set
- **arcgis**: add CRS parsing helpers for output-crs
- **wfs**: add typed exception subclasses for downstream consumers ([#481](https://github.com/geoparquet/geoparquet-io/issues/481))
- add fiboa plugin (gpio fiboa) ([#451](https://github.com/geoparquet/geoparquet-io/issues/451))
- add Vecorel specification support ([#450](https://github.com/geoparquet/geoparquet-io/issues/450))
- fetch latest Overture Maps release dynamically ([#455](https://github.com/geoparquet/geoparquet-io/issues/455))
- **convert**: add --geoparquet-version 1.1-geoarrow output format ([#436](https://github.com/geoparquet/geoparquet-io/issues/436))
- **extract**: add Carto SQL API extractor ([#449](https://github.com/geoparquet/geoparquet-io/issues/449))

### Fix

- **test**: xfail GeoPackage sequential conversion test on Windows/macOS ([#486](https://github.com/geoparquet/geoparquet-io/issues/486))
- **arcgis**: anchor maxAllowableOffset units by setting outSR
- **arcgis**: unset CRS for unresolvable WKID, resolve native WKT to EPSG ([#482](https://github.com/geoparquet/geoparquet-io/issues/482))
- **partition**: single-pass COPY PARTITION_BY instead of per-value re-scan ([#478](https://github.com/geoparquet/geoparquet-io/issues/478)) ([#480](https://github.com/geoparquet/geoparquet-io/issues/480))
- **check**: assert dict result to satisfy mypy in optimization check
- **arcgis**: read layer SR from extent/sourceSpatialReference fallbacks
- **check**: scale locality threshold by row-group count, report uncomputed metrics as None
- **arcgis**: normalize WKIDs, validate output-crs upfront, dedupe page converters
- **deps**: update mutmut config for 3.6.0 breaking changes
- **deps**: pin duckdb<1.5.2 for geography extension compatibility
- **check**: use spatial locality metrics instead of row-count heuristic ([#456](https://github.com/geoparquet/geoparquet-io/issues/456))
- **add**: filter Overture admin caches to land — stop ~2.6x row multiplication ([#474](https://github.com/geoparquet/geoparquet-io/issues/474))
- **wfs**: handle uint64 overflow in type promotion
- **wfs**: harden schema unification with edge case handling ([#476](https://github.com/geoparquet/geoparquet-io/issues/476))
- **wfs**: handle type mismatches across paginated pages ([#476](https://github.com/geoparquet/geoparquet-io/issues/476))
- **crs**: distinguish null CRS (unknown) from omitted CRS (default) ([#471](https://github.com/geoparquet/geoparquet-io/issues/471))
- **ci**: repair recurring slow-tests failures on main ([#472](https://github.com/geoparquet/geoparquet-io/issues/472))
- **add**: admin-divisions OOM fix — restore plain spatial join, retire in-join dedup ([#461](https://github.com/geoparquet/geoparquet-io/issues/461))
- **add**: restore bbox pre-filter in spatial join ON clause ([#460](https://github.com/geoparquet/geoparquet-io/issues/460))
- **add**: remove bbox pre-filter from spatial join ON clauses ([#457](https://github.com/geoparquet/geoparquet-io/issues/457))
- **ci**: separate blocking security checks from proactive CVE alerts ([#454](https://github.com/geoparquet/geoparquet-io/issues/454))
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
- **sort**: address adversarial review findings for PR [#446](https://github.com/geoparquet/geoparquet-io/issues/446)
- **sort**: handle empty/null geometries in Hilbert ordering ([#446](https://github.com/geoparquet/geoparquet-io/issues/446))
- **test**: skip DuckDB tests that crash xdist workers
- **wfs**: handle empty/null properties in GeoJSON features ([#445](https://github.com/geoparquet/geoparquet-io/issues/445))
- **write**: handle remote URLs and clean up dead code in no-geometry path ([#444](https://github.com/geoparquet/geoparquet-io/issues/444))
- **write**: handle geometry_column=None in all write strategies ([#440](https://github.com/geoparquet/geoparquet-io/issues/440))
- **add**: address CodeRabbit review comments
- **add**: preserve ALL metadata in bbox-metadata operation ([#439](https://github.com/geoparquet/geoparquet-io/issues/439))
- **add**: preserve bloom filters and GEOMETRY type in bbox-metadata ([#433](https://github.com/geoparquet/geoparquet-io/issues/433))
- **convert**: support GeoArrow native-encoded GeoParquet as input ([#435](https://github.com/geoparquet/geoparquet-io/issues/435))
- **validate**: guard against None logical_type in schema lookups ([#438](https://github.com/geoparquet/geoparquet-io/issues/438))
- **arcgis**: use CRS84 for metadata when extracting via f=geojson ([#427](https://github.com/geoparquet/geoparquet-io/issues/427))
- **cli**: reconfigure stdout/stderr to UTF-8 at CLI entry ([#432](https://github.com/geoparquet/geoparquet-io/issues/432))
- **tests**: mark flaky WFS test as xfail
- **s3**: connection leak in admin_datasets + add CLI docs
- **deps**: update vulnerable dependencies
- **s3**: address adversarial review findings
- **deps**: upgrade pip to 26.1 for CVE-2026-6357
- **wfs**: add retry logic for transient network errors
- **wfs**: address adversarial review findings ([#431](https://github.com/geoparquet/geoparquet-io/issues/431))
- **wfs**: adaptive pagination and reliability fixes for auto-tile
- **wfs**: detect server startIndex limits and improve error messages
- **wfs**: auto-paginate large datasets and fix parallel worker crash
- **pmtiles**: address adversarial review of [#422](https://github.com/geoparquet/geoparquet-io/issues/422)
- **pmtiles**: handle BrokenPipe and surface upstream stderr ([#421](https://github.com/geoparquet/geoparquet-io/issues/421))

### Refactor

- **wfs**: extract property probe and query builder helpers
- **geoarrow**: move coord-expr helpers to duckdb_utils, remove circular import
- **geo_metadata**: deduplicate _get_query_column_type by importing from duckdb_utils
- **admin**: use s3_config_scope() instead of configure_s3(con)
- **cli**: hide per-command S3 and profile flags (now global)
- **wfs**: unify fetch to always use httpx + local parsing ([#425](https://github.com/geoparquet/geoparquet-io/issues/425), [#426](https://github.com/geoparquet/geoparquet-io/issues/426), [#429](https://github.com/geoparquet/geoparquet-io/issues/429))

## v1.1.1 (2026-04-29)

### Fix

- **windows**: release file handles before replace in bbox_metadata ([#420](https://github.com/geoparquet/geoparquet-io/issues/420))

## v1.1.0 (2026-04-29)

### Feat

- integrate gpio-pmtiles into core ([#417](https://github.com/geoparquet/geoparquet-io/issues/417))
- **api**: expose axis_order and strict_crs in Python API
- **wfs**: add WFS 2.0 support, axis order fix, and CRS validation ([#312](https://github.com/geoparquet/geoparquet-io/issues/312), [#397](https://github.com/geoparquet/geoparquet-io/issues/397), [#398](https://github.com/geoparquet/geoparquet-io/issues/398))

### Fix

- **wfs**: address adversarial review findings for --output-crs
- **wfs**: honor --output-crs by reprojecting when server returns different CRS ([#407](https://github.com/geoparquet/geoparquet-io/issues/407))
- skip non-GeoParquet files early in check --pmtiles ([#408](https://github.com/geoparquet/geoparquet-io/issues/408))
- address PR [#417](https://github.com/geoparquet/geoparquet-io/issues/417) adversarial review findings
- PMTiles pipeline bugs found in adversarial review
- extend bbox fixes to streaming code paths ([#415](https://github.com/geoparquet/geoparquet-io/issues/415))
- GeoParquet 2.0 bbox handling for reproject, check, and add commands ([#409](https://github.com/geoparquet/geoparquet-io/issues/409), [#410](https://github.com/geoparquet/geoparquet-io/issues/410), [#412](https://github.com/geoparquet/geoparquet-io/issues/412))
- **tests**: use tuple comparison for pip version check
- **tests**: resolve CI failures for pip-audit CVE and wfs import ([#414](https://github.com/geoparquet/geoparquet-io/issues/414))
- **inspect**: scope fixtures to module, add markers, fix multi-geometry stats
- **inspect**: default --geo-stats to 1 row group with "... and N more" hint
- **inspect**: adapt geo_bbox precision to fit large projected coordinates
- **inspect**: use 2 decimal places for geo_bbox display and remove truncation
- **inspect**: show geo_bbox stats in row group tables and --geo-stats ([#413](https://github.com/geoparquet/geoparquet-io/issues/413))
- **wfs**: add version guard for srsName and improve test coverage ([#406](https://github.com/geoparquet/geoparquet-io/issues/406))
- **wfs**: include srsName parameter without bbox filter ([#406](https://github.com/geoparquet/geoparquet-io/issues/406))
- **test**: correct import path for list_layers module
- **convert**: apply gc.collect() on all platforms for multi-layer reads ([#403](https://github.com/geoparquet/geoparquet-io/issues/403))
- **wfs**: move type inference after concat, fix resource leak ([#400](https://github.com/geoparquet/geoparquet-io/issues/400))
- **wfs**: infer column types from string values ([#402](https://github.com/geoparquet/geoparquet-io/issues/402))
- **wfs**: address review findings
- **deps**: bump lxml>=6.1.0 for CVE-2026-41066 ([#395](https://github.com/geoparquet/geoparquet-io/issues/395))
- **deps**: remove pyarrow version cap ([#394](https://github.com/geoparquet/geoparquet-io/issues/394))
- **ci**: rename release.yml back to publish.yml for PyPI trusted publisher

## v1.0.1 (2026-04-20)

### Fix

- **arcgis**: address code review feedback for adaptive batch
- **arcgis**: add adaptive batch size and --batch-size flag ([#382](https://github.com/geoparquet/geoparquet-io/issues/382))
- **reproject**: use PROJJSON for ST_Transform when CRS lacks authority id ([#383](https://github.com/geoparquet/geoparquet-io/issues/383))
- **inspect**: eliminate false-positive CRS mismatch warnings for GeoParquet 2.0 ([#384](https://github.com/geoparquet/geoparquet-io/issues/384))
- **deps**: bump pytest to 9.0.3 for CVE-2025-71176 ([#389](https://github.com/geoparquet/geoparquet-io/issues/389))

### Refactor

- **release**: adopt portolan-cli release workflow ([#390](https://github.com/geoparquet/geoparquet-io/issues/390))

## v1.0.0 (2026-04-13)

### BREAKING CHANGE

- `geography_as_geometry` parameter no longer accepted.
BREAKING CHANGE: `gt` CLI alias removed, use `gpio` instead.

### Feat

- **core**: add framework-agnostic exception classes and CLI handler
- **api**: add partition_by_a5() and fix documentation drift ([#361](https://github.com/geoparquet/geoparquet-io/issues/361))
- **convert**: support multiple geometry columns in GeoParquet files ([#357](https://github.com/geoparquet/geoparquet-io/issues/357))
- **skills**: Package skill with gpio for all LLM users
- **docs**: Add menard documentation anti-drift system ([#332](https://github.com/geoparquet/geoparquet-io/issues/332))

### Fix

- **release**: use PyPI menard instead of git dependency ([#381](https://github.com/geoparquet/geoparquet-io/issues/381))
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
- **convert**: preserve original geometry column names and fix multi-geometry for all write strategies ([#357](https://github.com/geoparquet/geoparquet-io/issues/357))
- **core**: address CodeRabbit findings and centralize geometry detection
- **bigquery**: Address PR review findings
- **arcgis**: handle schema mismatch between paginated batches ([#355](https://github.com/geoparquet/geoparquet-io/issues/355))
- **benchmark**: use shared DuckDB connection helper and add test markers ([#353](https://github.com/geoparquet/geoparquet-io/issues/353))
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
- **core**: extract modules from common.py monolith ([#364](https://github.com/geoparquet/geoparquet-io/issues/364))
- **core**: extract remote, geometry_detection, file_utils from common.py ([#363](https://github.com/geoparquet/geoparquet-io/issues/363))
- **docs**: Compress contributing.md and add auto-sync
- **docs**: Compress CLAUDE.md and add deterministic enforcement
- **scripts**: Consolidate doc generators and pre-commit hooks
- **wfs**: remove ~900 lines of dead code, fix SQL injection, add parallel pagination

## v1.0.0b2 (2026-03-06)

### Feat

- allow specifying --profile web for web specific parquet structure checks ([#252](https://github.com/geoparquet/geoparquet-io/issues/252))

### Fix

- Address code review issues for FileGDB CRS detection
- Add FileGDB CRS detection workaround ([#250](https://github.com/geoparquet/geoparquet-io/issues/250))

## v1.0-beta (2026-02-10)

### Feat

- add directory input support with --min-size to partition s2 and quadkey commands
- add directory input support with --min-size to partition h3 command
- add sub_partition_directory function for batch sub-partitioning
- add find_large_files function for directory scanning
- add --min-size and --in-place options to partition_options decorator

### Fix

- add min_size and in_place params to all partition function signatures
- Fix CRS export for GDAL formats — projected CRS now roundtrips through FlatGeobuf and GeoPackage (fixes [#189](https://github.com/geoparquet/geoparquet-io/issues/189), [#190](https://github.com/geoparquet/geoparquet-io/issues/190))
- Fix crash on non-numeric CRS codes like IGNF:LAMB93 ([#193](https://github.com/geoparquet/geoparquet-io/issues/193))
- Fix inspect metadata performance regression ([#232](https://github.com/geoparquet/geoparquet-io/issues/232))
- Improved error messages for common user mistakes — invalid Parquet files now show helpful hints ([#140](https://github.com/geoparquet/geoparquet-io/issues/140))

## v0.9.0 (2026-01-17)

## v0.8.0 (2026-01-04)

## v0.7.0 (2025-12-28)

## v0.6.1 (2025-12-11)

## v0.6.0 (2025-12-06)

### Feat

- enhance inspect command with geometry types and WKT preview ([#93](https://github.com/geoparquet/geoparquet-io/issues/93))

## v0.5.1 (2025-12-04)

## v0.5.0 (2025-12-02)

### Feat

- **io**: add remote-to-remote support with consolidated write infrastructure ([#74](https://github.com/geoparquet/geoparquet-io/issues/74))
- **auth**: add automatic AWS credential discovery for S3
- **io**: add remote-to-remote operations and automatic AWS auth
- **remote**: handle edge cases for remote file operations                                                                                                                             │ │                                                                                                                                                                                                         │ │   Edge case improvements:                                                                                                                                                                               │ │   - fix(convert): support remote parquet files via read_parquet() instead of ST_Read()                                                                                                                  │ │   - fix(convert): validate only local files, allow remote URLs to pass through                                                                                                                          │ │   - feat(stac): block remote files with clear error message and TODO for future                                                                                                                         │ │   - feat(common): add progress indicator for remote operations (shows protocol)                                                                                                                         │ │   - feat(common): add get_remote_error_hint() for better error messages                                                                                                                                 │ │   - docs: update limitations section with what works vs doesn't work                                                                                                                                    │ │                                                                                                                                                                                                         │ │   Closes edge cases for remote reads before tests/docs phase.
- **upload**: add upload command to remote buckets using obstore - Upload command with obstore, supporting parallelism and progress tracking - Single file and directory uploads - Support for s3, GCS, Azure, HTTP - Pattern filtering - Dry run mode ([#68](https://github.com/geoparquet/geoparquet-io/issues/68))
- **check**: add --fix flag to automatically correct issues detected by check command - add --fix, --fix-output, and --no-backup flags to all check commands - refactor check functions to return structured results enabling fixes via new core/check_fixes.py module. ([#63](https://github.com/geoparquet/geoparquet-io/issues/63))
- **cli**: add benchmark command for conversion performance testing ([#65](https://github.com/geoparquet/geoparquet-io/issues/65))

### Fix

- **check**: remove format command group - remove format command group; consolidate under check - move add-bbox-metadata to add command group - simplify add-bbox command - update + reorg tests

## v0.4.0 (2025-11-17)

### Feat

- **cli**: add ability to pass custom basename for partition output file, e.g., fields_NL.parquet instead of NL.parquet
- **stac**: add STAC Item and Collection generation ([#57](https://github.com/geoparquet/geoparquet-io/issues/57))
- **cli**: Add convert command for optimized GeoParquet conversion ([#56](https://github.com/geoparquet/geoparquet-io/issues/56))

### Fix

- **tests**: correct issue with failing test on windows

## v0.3.0 (2025-11-06)

## v0.2.0 (2025-10-24)

### Refactor

- **cli**: consolidate repetitive option decorators ([#36](https://github.com/geoparquet/geoparquet-io/issues/36))

## v0.1.0 (2025-10-24)

### Feat

- **cli**: add inspect command for fast file examination ([#31](https://github.com/geoparquet/geoparquet-io/issues/31))
- **partition**: add KD-tree spatial partitioning ([#30](https://github.com/geoparquet/geoparquet-io/issues/30))
- Add intelligent partition analysis with recommendations and H3 column exclusion
- **partition**: add H3 partitioning with auto-column creation
- **add**: add H3 support with computed column abstraction ([#23](https://github.com/geoparquet/geoparquet-io/issues/23))

### Fix

- Add Windows compatibility for hive partition tests
- Resolve Windows file locking issues in tests
- **tests**: Ensure DuckDB connections are closed before file cleanup
- Update test_partition_format.py import to use geoparquet_io
