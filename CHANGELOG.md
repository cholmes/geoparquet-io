# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## v1.4.0 (2026-08-30)

This release adds a pipeline for visualizing large data at low zoom.
`gpio process aggregate` rolls features into A5, H3 or admin buckets with `count`,
`--metric` rollups and `--breakdown` pivots; `gpio process overview` derives coarser
levels from that output; and `gpio pmtiles pyramid` bakes the whole stack into one
zoom-banded archive. `gpio sort str` adds Sort-Tile-Recursive ordering beside Hilbert,
`gpio convert --geoparquet-version 1.1-geoarrow` produces native GeoArrow from any
input, and WFS extraction now auto-tiles large layers instead of silently truncating
them at a capped server.

Most of the work went into correctness. A write-contract characterization suite, a
CLI-to-Python-API parity harness, a CLI-surface snapshot, spatial-index golden values,
ten end-to-end journeys, and every fenced example in `docs/guide/*.md` running as a
test together surfaced a large share of the fixes below: stale bbox metadata after a
reproject or a filtered extract, `--row-group-size` ignored by the disk-rewrite write
strategy, secondary geometry columns written with the wrong physical carrier, sidecar
key/value metadata dropped from tables with no geometry column, and unquoted column
identifiers at several raw-SQL sites.

Two changes break existing behavior. Python API defaults that had silently diverged
from their CLI twins are now aligned, and a parity test fails on any new mismatch
([#661](https://github.com/geoparquet/geoparquet-io/issues/661)). The dependency floor
rises to `duckdb>=1.5.2`, which drops a segfault in geometry repair that could kill a
long extraction with no output and no error. The cost is that `gpio add s2` and
`gpio partition s2` are unavailable in this release: the `geography` community
extension is not published for that DuckDB, so both stop with an explanation before
reading input, and both start working again with no gpio change once the extension is
republished upstream. `gpio add a5` is the closest substitute
([#778](https://github.com/geoparquet/geoparquet-io/issues/778)).

Three people contributed to gpio for the first time in this release, and we are
grateful to all of them. [@cayetanobv](https://github.com/cayetanobv) sent ten pull
requests, and between them they made gpio survive the geometry the real world
actually contains: curved WKB is linearized on read and streamed rather than
materialized, empty geometries sort last instead of failing `ST_Hilbert`, CSV rows
with NULL geometry are kept, repair no longer segfaults on NULL geometry rows,
conversion names the types it cannot handle, `convert geojson` reprojects before it
repairs, and `Table.convert()` stops losing the source CRS on the way to
`Table.write()` — most of them bugs nobody else had hit yet.
[@oakhill87](https://github.com/oakhill87) sent three: Sort-Tile-Recursive ordering,
the second spatial sort gpio offers; the catch that `sort hilbert` dropped
`--write-memory` before it reached the write engine; and the `geoparquet_version`
documentation on `Table.write` and `Table.upload`.
[@Sanjays2402](https://github.com/Sanjays2402) sent three as well, making two silent
failures speak up — DuckDB extension install errors, and the misleading "Batch size 0
too large" on non-paged ArcGIS requests — and adding regression coverage for
empty-geometry bbox exclusion. If any of this sounds like work you would enjoy, the
issue tracker is open and the guides now run as tests, so a fix is easy to prove.

### Breaking

- fix(api)!: align CLI and Python API defaults (admin dataset, hive, WFS page size) by [@cholmes](https://github.com/cholmes) in [#661](https://github.com/geoparquet/geoparquet-io/pull/661)
- fix(deps)!: require duckdb>=1.5.2 and withdraw S2 until the geography extension returns by [@cholmes](https://github.com/cholmes) in [#778](https://github.com/geoparquet/geoparquet-io/pull/778)

### Added

- feat(pmtiles): expose tippecanoe tile-size/drop-densest flags ([#492](https://github.com/geoparquet/geoparquet-io/issues/492)) by [@nlebovits](https://github.com/nlebovits) in [#493](https://github.com/geoparquet/geoparquet-io/pull/493)
- feat(wfs): auto-tile large datasets and optimize extraction performance by [@nlebovits](https://github.com/nlebovits) in [#502](https://github.com/geoparquet/geoparquet-io/pull/502)
- feat: default geometry repair with ST_MakeValid (issue [#506](https://github.com/geoparquet/geoparquet-io/issues/506)) by [@nlebovits](https://github.com/nlebovits) in [#507](https://github.com/geoparquet/geoparquet-io/pull/507)
- feat(carto): extract non-geo (tabular) tables to plain Parquet (issue [#508](https://github.com/geoparquet/geoparquet-io/issues/508)) by [@nlebovits](https://github.com/nlebovits) in [#509](https://github.com/geoparquet/geoparquet-io/pull/509)
- feat(arcgis): make the HTTP timeout configurable for large polygon layers by [@nlebovits](https://github.com/nlebovits) in [#533](https://github.com/geoparquet/geoparquet-io/pull/533)
- feat(inspect): fit head/tail previews to terminal width by [@cholmes](https://github.com/cholmes) in [#520](https://github.com/geoparquet/geoparquet-io/pull/520)
- feat(geoarrow): produce native GeoArrow encoding for 1.1-geoarrow from any input by [@cholmes](https://github.com/cholmes) in [#519](https://github.com/geoparquet/geoparquet-io/pull/519)
- feat(process): add aggregate (a5, h3, admin) for low-zoom visualization by [@cholmes](https://github.com/cholmes) in [#529](https://github.com/geoparquet/geoparquet-io/pull/529)
- feat(ci): harden CI and code-trust machinery — deterministic gates, green main, bots open PRs by [@nlebovits](https://github.com/nlebovits) in [#552](https://github.com/geoparquet/geoparquet-io/pull/552)
- feat(validate): add corpus-surfaced missing checks ([#586](https://github.com/geoparquet/geoparquet-io/issues/586)) by [@cholmes](https://github.com/cholmes) in [#597](https://github.com/geoparquet/geoparquet-io/pull/597)
- feat(process): add --where row filter to process aggregate ([#568](https://github.com/geoparquet/geoparquet-io/issues/568)) by [@cholmes](https://github.com/cholmes) in [#612](https://github.com/geoparquet/geoparquet-io/pull/612)
- feat(process): add --metric-nodata sentinel handling to process aggregate ([#566](https://github.com/geoparquet/geoparquet-io/issues/566)) by [@cholmes](https://github.com/cholmes) in [#613](https://github.com/geoparquet/geoparquet-io/pull/613)
- feat(process): add --bucket-point bbox keying to process aggregate ([#567](https://github.com/geoparquet/geoparquet-io/issues/567)) by [@cholmes](https://github.com/cholmes) in [#614](https://github.com/geoparquet/geoparquet-io/pull/614)
- feat: process overview + pmtiles pyramid ([#570](https://github.com/geoparquet/geoparquet-io/issues/570)) by [@cholmes](https://github.com/cholmes) in [#615](https://github.com/geoparquet/geoparquet-io/pull/615)
- feat(convert): linearize curved WKB geometries on read instead of failing by [@cayetanobv](https://github.com/cayetanobv) in [#647](https://github.com/geoparquet/geoparquet-io/pull/647)
- feat(sort): add Sort-Tile-Recursive ordering by [@oakhill87](https://github.com/oakhill87) in [#766](https://github.com/geoparquet/geoparquet-io/pull/766)

### Changed

- refactor(common): decompose _promote_numeric_type (xenon F→A) by [@nlebovits](https://github.com/nlebovits) in [#494](https://github.com/geoparquet/geoparquet-io/pull/494)
- perf(add): lean on DuckDB SPATIAL_JOIN for admin-divisions/country-codes; fix misleading native-geometry message; re-evaluate bbox pre-filter (supersedes [#462](https://github.com/geoparquet/geoparquet-io/issues/462)) by [@nlebovits](https://github.com/nlebovits) in [#540](https://github.com/geoparquet/geoparquet-io/pull/540)
- perf(convert): stream the linearized curved-geometry read by [@cayetanobv](https://github.com/cayetanobv) in [#650](https://github.com/geoparquet/geoparquet-io/pull/650)

### Fixed

- fix(wfs): add sortBy parameter for stable pagination on PK-less layers by [@nlebovits](https://github.com/nlebovits) in [#489](https://github.com/geoparquet/geoparquet-io/pull/489)
- fix(wfs): trust server-declared CRS from GeoJSON response ([#499](https://github.com/geoparquet/geoparquet-io/issues/499)) by [@nlebovits](https://github.com/nlebovits) in [#500](https://github.com/geoparquet/geoparquet-io/pull/500)
- fix(wfs): auto-tiling fails with --workers > 1 by [@nlebovits](https://github.com/nlebovits) in [#504](https://github.com/geoparquet/geoparquet-io/pull/504)
- fix(arcgis): catch the int32 to timestamp cast error during extraction by [@nlebovits](https://github.com/nlebovits) in [#535](https://github.com/geoparquet/geoparquet-io/pull/535)
- fix(partition): make --auto resolution extent-aware ([#524](https://github.com/geoparquet/geoparquet-io/issues/524)) by [@cholmes](https://github.com/cholmes) in [#526](https://github.com/geoparquet/geoparquet-io/pull/526)
- fix(arcgis): catch duckdb.IOException so the batch-size fallback ladder fires by [@nlebovits](https://github.com/nlebovits) in [#536](https://github.com/geoparquet/geoparquet-io/pull/536)
- fix(spatial): make admin joins and grid keying CRS-aware for non-CRS84 input by [@nlebovits](https://github.com/nlebovits) in [#537](https://github.com/geoparquet/geoparquet-io/pull/537)
- fix(pmtiles): stop the geoarrow push_batch crash on large GeoParquet by [@nlebovits](https://github.com/nlebovits) in [#541](https://github.com/geoparquet/geoparquet-io/pull/541)
- fix(spatial): make add/partition CRS-aware for non-CRS84 input ([#525](https://github.com/geoparquet/geoparquet-io/issues/525)) by [@cholmes](https://github.com/cholmes) in [#530](https://github.com/geoparquet/geoparquet-io/pull/530)
- fix(geometry): summarize geo metadata in verbose output instead of full dump by [@cholmes](https://github.com/cholmes) in [#542](https://github.com/geoparquet/geoparquet-io/pull/542)
- fix(partition): stop leaking the internal __gpio_part alias column into output by [@nlebovits](https://github.com/nlebovits) in [#539](https://github.com/geoparquet/geoparquet-io/pull/539)
- fix(convert): honor --row-group-size-mb on the duckdb-kv write path ([#547](https://github.com/geoparquet/geoparquet-io/issues/547)) by [@nlebovits](https://github.com/nlebovits) in [#549](https://github.com/geoparquet/geoparquet-io/pull/549)
- fix(mutmut): keep test suite CWD-stable so mutation stats phase passes by [@nlebovits](https://github.com/nlebovits) in [#565](https://github.com/geoparquet/geoparquet-io/pull/565)
- fix(ci): rename Codecov 'file' input to 'files' for v7 action by [@nlebovits](https://github.com/nlebovits) in [#575](https://github.com/geoparquet/geoparquet-io/pull/575)
- fix(ci): drop self-approval from Dependabot auto-merge by [@nlebovits](https://github.com/nlebovits) in [#577](https://github.com/geoparquet/geoparquet-io/pull/577)
- fix(ci): handle missing duckdb 'geography' extension for duckdb 1.5.4 by [@nlebovits](https://github.com/nlebovits) in [#598](https://github.com/geoparquet/geoparquet-io/pull/598)
- fix: adversarial review findings for the corpus audit stack ([#591](https://github.com/geoparquet/geoparquet-io/issues/591)–[#597](https://github.com/geoparquet/geoparquet-io/issues/597)) by [@cholmes](https://github.com/cholmes) in [#602](https://github.com/geoparquet/geoparquet-io/pull/602)
- fix(validate): corpus-surfaced validation bugs ([#581](https://github.com/geoparquet/geoparquet-io/issues/581), [#582](https://github.com/geoparquet/geoparquet-io/issues/582), [#584](https://github.com/geoparquet/geoparquet-io/issues/584), [#585](https://github.com/geoparquet/geoparquet-io/issues/585)) by [@cholmes](https://github.com/cholmes) in [#592](https://github.com/geoparquet/geoparquet-io/pull/592)
- fix(validate): dimension-aware Z/M geometry_types handling ([#583](https://github.com/geoparquet/geoparquet-io/issues/583)) by [@cholmes](https://github.com/cholmes) in [#593](https://github.com/geoparquet/geoparquet-io/pull/593)
- fix(convert): preserve input GeoParquet version in auto mode ([#587](https://github.com/geoparquet/geoparquet-io/issues/587)) by [@cholmes](https://github.com/cholmes) in [#594](https://github.com/geoparquet/geoparquet-io/pull/594)
- fix(write): guarantee geo metadata on 2.0 writes of M/ZM data ([#589](https://github.com/geoparquet/geoparquet-io/issues/589)) by [@cholmes](https://github.com/cholmes) in [#595](https://github.com/geoparquet/geoparquet-io/pull/595)
- fix(convert): preserve non-planar edges when rewriting geography data ([#588](https://github.com/geoparquet/geoparquet-io/issues/588)) by [@cholmes](https://github.com/cholmes) in [#596](https://github.com/geoparquet/geoparquet-io/pull/596)
- fix(arcgis): stop reporting 'Batch size 0 too large' on non-paged requests by [@Sanjays2402](https://github.com/Sanjays2402) in [#606](https://github.com/geoparquet/geoparquet-io/pull/606)
- fix(deps): upgrade vulnerable dependencies (automated) by [@nlebovits](https://github.com/nlebovits) in [#623](https://github.com/geoparquet/geoparquet-io/pull/623)
- fix(process): clear errors for missing --metric/--breakdown columns by [@cholmes](https://github.com/cholmes) in [#617](https://github.com/geoparquet/geoparquet-io/pull/617)
- fix(sort): forward --write-memory from sort hilbert to the write engine by [@oakhill87](https://github.com/oakhill87) in [#627](https://github.com/geoparquet/geoparquet-io/pull/627)
- fix(geometry): stop the SIGSEGV in repair on tables with NULL geometry rows by [@cayetanobv](https://github.com/cayetanobv) in [#645](https://github.com/geoparquet/geoparquet-io/pull/645)
- fix(convert): name the offending types when non-linear WKB fails conversion by [@cayetanobv](https://github.com/cayetanobv) in [#646](https://github.com/geoparquet/geoparquet-io/pull/646)
- fix(api): keep the source CRS between Table.convert() and Table.write() by [@cayetanobv](https://github.com/cayetanobv) in [#644](https://github.com/geoparquet/geoparquet-io/pull/644)
- fix(convert): order empty geometries last instead of failing on ST_Hilbert by [@cayetanobv](https://github.com/cayetanobv) in [#651](https://github.com/geoparquet/geoparquet-io/pull/651)
- fix(convert): reproject before repairing geometry in convert geojson by [@cayetanobv](https://github.com/cayetanobv) in [#653](https://github.com/geoparquet/geoparquet-io/pull/653)
- fix(convert): keep CSV rows whose geometry is NULL by [@cayetanobv](https://github.com/cayetanobv) in [#656](https://github.com/geoparquet/geoparquet-io/pull/656)
- fix(duckdb): route bare duckdb.connect() sites through the connection factory by [@cholmes](https://github.com/cholmes) in [#659](https://github.com/geoparquet/geoparquet-io/pull/659)
- fix(core): make library writes free of global state leaks by [@cholmes](https://github.com/cholmes) in [#660](https://github.com/geoparquet/geoparquet-io/pull/660)
- fix(add,sort): forward --write-memory to the write engine by [@cholmes](https://github.com/cholmes) in [#663](https://github.com/geoparquet/geoparquet-io/pull/663)
- fix(metadata): drop stale geo bbox after reproject and filtered extract by [@cholmes](https://github.com/cholmes) in [#658](https://github.com/geoparquet/geoparquet-io/pull/658)
- fix(sql): quote geometry-column identifiers at raw SQL interpolation by [@cholmes](https://github.com/cholmes) in [#662](https://github.com/geoparquet/geoparquet-io/pull/662)
- fix(extract): validate --where on bigquery and carto extractors by [@cholmes](https://github.com/cholmes) in [#657](https://github.com/geoparquet/geoparquet-io/pull/657)
- fix(tests): complete the WFS mock so six tests stop hitting the network by [@cholmes](https://github.com/cholmes) in [#676](https://github.com/geoparquet/geoparquet-io/pull/676)
- fix(add): quote quadkey column identifiers in the file-based path by [@cholmes](https://github.com/cholmes) in [#695](https://github.com/geoparquet/geoparquet-io/pull/695)
- fix(validate): accept the GeoArrow encodings GeoParquet 1.1 permits by [@cholmes](https://github.com/cholmes) in [#715](https://github.com/geoparquet/geoparquet-io/pull/715)
- fix(wfs): surface failed feature-count probes instead of swallowing them by [@cholmes](https://github.com/cholmes) in [#696](https://github.com/geoparquet/geoparquet-io/pull/696)
- fix(write): honor row-group sizing in the disk-rewrite strategy by [@cholmes](https://github.com/cholmes) in [#698](https://github.com/geoparquet/geoparquet-io/pull/698)
- fix(convert): omit the 1.1-only covering key from GeoParquet 1.0 output by [@cholmes](https://github.com/cholmes) in [#714](https://github.com/geoparquet/geoparquet-io/pull/714)
- fix(common): honor explicit parquet-geo-only in write_geoparquet_table by [@cholmes](https://github.com/cholmes) in [#702](https://github.com/geoparquet/geoparquet-io/pull/702)
- fix(write): make arrow-streaming output independent of geoarrow import state by [@cholmes](https://github.com/cholmes) in [#707](https://github.com/geoparquet/geoparquet-io/pull/707)
- fix(metadata): preserve input non-geo KV metadata in convert and Table.write by [@cholmes](https://github.com/cholmes) in [#710](https://github.com/geoparquet/geoparquet-io/pull/710)
- fix(duckdb): surface extension install errors when the extension fails to load by [@Sanjays2402](https://github.com/Sanjays2402) in [#605](https://github.com/geoparquet/geoparquet-io/pull/605)
- fix(metadata): keep the bbox covering on GeoParquet 2.0 output by [@cholmes](https://github.com/cholmes) in [#744](https://github.com/geoparquet/geoparquet-io/pull/744)
- fix(geojson): stop --write-bbox and --id-field truncating every Feature by [@cholmes](https://github.com/cholmes) in [#745](https://github.com/geoparquet/geoparquet-io/pull/745)
- fix(geojson): read stdin into a named GeoJSON output file by [@cholmes](https://github.com/cholmes) in [#746](https://github.com/geoparquet/geoparquet-io/pull/746)
- fix(add): build the country-codes summary from the columns actually written ([#672](https://github.com/geoparquet/geoparquet-io/issues/672)) by [@cholmes](https://github.com/cholmes) in [#750](https://github.com/geoparquet/geoparquet-io/pull/750)
- fix(write): let the disk-rewrite metadata pass merge row groups, not only split ([#697](https://github.com/geoparquet/geoparquet-io/issues/697)) by [@cholmes](https://github.com/cholmes) in [#757](https://github.com/geoparquet/geoparquet-io/pull/757)
- fix(convert): explain that csv/fgb/gpkg/shp cannot read stdin ([#749](https://github.com/geoparquet/geoparquet-io/issues/749)) by [@cholmes](https://github.com/cholmes) in [#752](https://github.com/geoparquet/geoparquet-io/pull/752)
- fix(metadata): honor parquet-geo-only when the geometry column is absent ([#701](https://github.com/geoparquet/geoparquet-io/issues/701)) by [@cholmes](https://github.com/cholmes) in [#753](https://github.com/geoparquet/geoparquet-io/pull/753)
- fix(add): keep bbox-metadata metadata-only and refuse plain Parquet ([#712](https://github.com/geoparquet/geoparquet-io/issues/712), [#713](https://github.com/geoparquet/geoparquet-io/issues/713)) by [@cholmes](https://github.com/cholmes) in [#754](https://github.com/geoparquet/geoparquet-io/pull/754)
- fix(write): give secondary geometry columns the target version's carrier ([#706](https://github.com/geoparquet/geoparquet-io/issues/706)) by [@cholmes](https://github.com/cholmes) in [#765](https://github.com/geoparquet/geoparquet-io/pull/765)
- fix(metadata): carry sidecar KV keys without geometry and without a rewrite ([#708](https://github.com/geoparquet/geoparquet-io/issues/708), [#709](https://github.com/geoparquet/geoparquet-io/issues/709)) by [@cholmes](https://github.com/cholmes) in [#756](https://github.com/geoparquet/geoparquet-io/pull/756)
- fix(metadata): judge geometries against the whole file's geo statistics ([#721](https://github.com/geoparquet/geoparquet-io/issues/721)) by [@cholmes](https://github.com/cholmes) in [#770](https://github.com/geoparquet/geoparquet-io/pull/770)

### Documentation

- docs(api): document geoparquet_version on Table.write/upload; drop write_memory by [@oakhill87](https://github.com/oakhill87) in [#510](https://github.com/geoparquet/geoparquet-io/pull/510)
- docs(spatial-join): correct pre-filter rationale to memory-safety ([#545](https://github.com/geoparquet/geoparquet-io/issues/545) Fix A) by [@nlebovits](https://github.com/nlebovits) in [#551](https://github.com/geoparquet/geoparquet-io/pull/551)
- docs(deps): record why the duckdb pin also holds the TRY() workaround by [@cayetanobv](https://github.com/cayetanobv) in [#654](https://github.com/geoparquet/geoparquet-io/pull/654)
- docs: correct guide examples that use nonexistent CLI flags by [@cholmes](https://github.com/cholmes) in [#735](https://github.com/geoparquet/geoparquet-io/pull/735)
- docs(changelog): compress unreleased entries and link every issue reference by [@cholmes](https://github.com/cholmes) in [#743](https://github.com/geoparquet/geoparquet-io/pull/743)
- docs(write-strategies): say what --write-memory does on extract bigquery ([#673](https://github.com/geoparquet/geoparquet-io/issues/673)) by [@cholmes](https://github.com/cholmes) in [#763](https://github.com/geoparquet/geoparquet-io/pull/763)

### Internal

- chore: deterministic guardrails for recurring fix/review cycles by [@nlebovits](https://github.com/nlebovits) in [#495](https://github.com/geoparquet/geoparquet-io/pull/495)
- test(arcgis): shrink live [#334](https://github.com/geoparquet/geoparquet-io/issues/334) regression fetch to curb worker crashes by [@nlebovits](https://github.com/nlebovits) in [#544](https://github.com/geoparquet/geoparquet-io/pull/544)
- chore(ci): surface non-blocking network-test failures in run summary + tracking issue by [@nlebovits](https://github.com/nlebovits) in [#599](https://github.com/geoparquet/geoparquet-io/pull/599)
- test(corpus): add official geoparquet-testing conformance suite (GeoParquet 2.0 audit) by [@cholmes](https://github.com/cholmes) in [#591](https://github.com/geoparquet/geoparquet-io/pull/591)
- chore(ci): retire the "main: review required" ruleset by [@nlebovits](https://github.com/nlebovits) in [#635](https://github.com/geoparquet/geoparquet-io/pull/635)
- test(benchmark): assert setup ordering instead of racing a stopwatch by [@cholmes](https://github.com/cholmes) in [#675](https://github.com/geoparquet/geoparquet-io/pull/675)
- ci(coverage): report coverage in one job, not ten ([#665](https://github.com/geoparquet/geoparquet-io/issues/665)) by [@cholmes](https://github.com/cholmes) in [#677](https://github.com/geoparquet/geoparquet-io/pull/677)
- test(metadata): pin agreement across the geo-metadata readers and CRS helpers ([#664](https://github.com/geoparquet/geoparquet-io/issues/664)) by [@cholmes](https://github.com/cholmes) in [#681](https://github.com/geoparquet/geoparquet-io/pull/681)
- test(cli): pin the CLI surface with a structural snapshot ([#664](https://github.com/geoparquet/geoparquet-io/issues/664)) by [@cholmes](https://github.com/cholmes) in [#679](https://github.com/geoparquet/geoparquet-io/pull/679)
- test(ci): move repo tooling checks into a meta lane ([#665](https://github.com/geoparquet/geoparquet-io/issues/665)) by [@cholmes](https://github.com/cholmes) in [#684](https://github.com/geoparquet/geoparquet-io/pull/684)
- test(spatial-index): pin golden cell values across add, partition, and sort ([#664](https://github.com/geoparquet/geoparquet-io/issues/664)) by [@cholmes](https://github.com/cholmes) in [#682](https://github.com/geoparquet/geoparquet-io/pull/682)
- test(api): add CLI/ops/Table parity harness at the core boundary ([#664](https://github.com/geoparquet/geoparquet-io/issues/664)) by [@cholmes](https://github.com/cholmes) in [#685](https://github.com/geoparquet/geoparquet-io/pull/685)
- test(write): add the write-contract characterization suite ([#664](https://github.com/geoparquet/geoparquet-io/issues/664)) by [@cholmes](https://github.com/cholmes) in [#693](https://github.com/geoparquet/geoparquet-io/pull/693)
- test(validate): regression coverage for empty-geometry bbox exclusion by [@Sanjays2402](https://github.com/Sanjays2402) in [#590](https://github.com/geoparquet/geoparquet-io/pull/590)
- test(notebooks): stop the inert cloud notebook from reporting a false-positive pass ([#667](https://github.com/geoparquet/geoparquet-io/issues/667)) by [@cholmes](https://github.com/cholmes) in [#720](https://github.com/geoparquet/geoparquet-io/pull/720)
- test(slow): re-align two slow-lane assertions with current behavior by [@cholmes](https://github.com/cholmes) in [#741](https://github.com/geoparquet/geoparquet-io/pull/741)
- test(data): add canonical sample dataset for docs and e2e journeys ([#667](https://github.com/geoparquet/geoparquet-io/issues/667)) by [@cholmes](https://github.com/cholmes) in [#719](https://github.com/geoparquet/geoparquet-io/pull/719)
- test(e2e): add the ten-journey end-to-end matrix ([#667](https://github.com/geoparquet/geoparquet-io/issues/667)) by [@cholmes](https://github.com/cholmes) in [#725](https://github.com/geoparquet/geoparquet-io/pull/725)
- test(docs): run the guides' examples as tests ([#667](https://github.com/geoparquet/geoparquet-io/issues/667)) by [@cholmes](https://github.com/cholmes) in [#732](https://github.com/geoparquet/geoparquet-io/pull/732)
- test(geo-stats): record that Windows geo-stat zeros are DuckDB's read, not pyarrow's write by [@cholmes](https://github.com/cholmes) in [#748](https://github.com/geoparquet/geoparquet-io/pull/748)
- test(covering): pin the spatial-index covering against the bbox covering ([#694](https://github.com/geoparquet/geoparquet-io/issues/694)) by [@cholmes](https://github.com/cholmes) in [#751](https://github.com/geoparquet/geoparquet-io/pull/751)
- chore(release): generate the changelog from merged PRs, and prep v1.4.0 by [@cholmes](https://github.com/cholmes) in [#780](https://github.com/geoparquet/geoparquet-io/pull/780)

### Dependencies

- build(deps-dev): bump pytest from 9.0.3 to 9.1.0 in the development-dependencies group by [@dependabot[bot]](https://github.com/dependabot[bot]) in [#496](https://github.com/geoparquet/geoparquet-io/pull/496)
- build(deps): upgrade dependencies flagged by the security audit by [@nlebovits](https://github.com/nlebovits) in [#532](https://github.com/geoparquet/geoparquet-io/pull/532)
- build(deps-dev): bump the development-dependencies group across 1 directory with 2 updates by [@dependabot[bot]](https://github.com/dependabot[bot]) in [#527](https://github.com/geoparquet/geoparquet-io/pull/527)
- build(deps): bump actions/checkout from 6 to 7 by [@dependabot[bot]](https://github.com/dependabot[bot]) in [#513](https://github.com/geoparquet/geoparquet-io/pull/513)
- build(deps): relax the duckdb pin by [@nlebovits](https://github.com/nlebovits) in [#534](https://github.com/geoparquet/geoparquet-io/pull/534)
- build(deps): bump the production-dependencies group across 1 directory with 10 updates by [@dependabot[bot]](https://github.com/dependabot[bot]) in [#528](https://github.com/geoparquet/geoparquet-io/pull/528)
- build(deps): bump pymdown-extensions from 10.21.3 to 11.0.1 by [@dependabot[bot]](https://github.com/dependabot[bot]) in [#556](https://github.com/geoparquet/geoparquet-io/pull/556)
- build(deps-dev): bump the development-dependencies group across 1 directory with 2 updates by [@dependabot[bot]](https://github.com/dependabot[bot]) in [#554](https://github.com/geoparquet/geoparquet-io/pull/554)
- ci: bump astral-sh/setup-uv from 7.6.0 to 8.2.0 by [@dependabot[bot]](https://github.com/dependabot[bot]) in [#558](https://github.com/geoparquet/geoparquet-io/pull/558)
- ci: bump actions/deploy-pages from 4.0.5 to 5.0.0 by [@dependabot[bot]](https://github.com/dependabot[bot]) in [#559](https://github.com/geoparquet/geoparquet-io/pull/559)
- ci: bump actions/upload-artifact from 6.0.0 to 7.0.1 by [@dependabot[bot]](https://github.com/dependabot[bot]) in [#561](https://github.com/geoparquet/geoparquet-io/pull/561)
- ci: bump peter-evans/create-pull-request from 7.0.11 to 8.1.1 by [@dependabot[bot]](https://github.com/dependabot[bot]) in [#562](https://github.com/geoparquet/geoparquet-io/pull/562)
- ci: bump astral-sh/setup-uv from 8.2.0 to 8.3.2 by [@dependabot[bot]](https://github.com/dependabot[bot]) in [#571](https://github.com/geoparquet/geoparquet-io/pull/571)
- build(deps): bump the production-dependencies group across 1 directory with 3 updates by [@dependabot[bot]](https://github.com/dependabot[bot]) in [#576](https://github.com/geoparquet/geoparquet-io/pull/576)
- build(deps): bump ruff from 0.15.20 to 0.15.21 in the production-dependencies group by [@dependabot[bot]](https://github.com/dependabot[bot]) in [#579](https://github.com/geoparquet/geoparquet-io/pull/579)
- ci: bump astral-sh/setup-uv from 8.3.2 to 9.0.0 by [@dependabot[bot]](https://github.com/dependabot[bot]) in [#620](https://github.com/geoparquet/geoparquet-io/pull/620)
- ci: bump actions/setup-python from 6.3.0 to 7.0.0 by [@dependabot[bot]](https://github.com/dependabot[bot]) in [#607](https://github.com/geoparquet/geoparquet-io/pull/607)
- ci: bump actions/checkout from 7.0.0 to 7.0.1 by [@dependabot[bot]](https://github.com/dependabot[bot]) in [#608](https://github.com/geoparquet/geoparquet-io/pull/608)
- build(deps): raise the pip floor to 26.2 for PYSEC-2026-3721 by [@cayetanobv](https://github.com/cayetanobv) in [#669](https://github.com/geoparquet/geoparquet-io/pull/669)
- ci: bump astral-sh/setup-uv from 9.0.0 to 10.0.1 by [@dependabot[bot]](https://github.com/dependabot[bot]) in [#716](https://github.com/geoparquet/geoparquet-io/pull/716)
- build(deps): bump the production-dependencies group across 1 directory with 16 updates by [@dependabot[bot]](https://github.com/dependabot[bot]) in [#742](https://github.com/geoparquet/geoparquet-io/pull/742)
- build(deps-dev): bump pygments from 2.20.0 to 2.21.0 in the development-dependencies group by [@dependabot[bot]](https://github.com/dependabot[bot]) in [#767](https://github.com/geoparquet/geoparquet-io/pull/767)
- build(deps): bump pyarrow from 24.0.0 to 25.0.1 by [@dependabot[bot]](https://github.com/dependabot[bot]) in [#769](https://github.com/geoparquet/geoparquet-io/pull/769)
- build(deps): bump obstore and pymdown-extensions by [@dependabot[bot]](https://github.com/dependabot[bot]) in [#768](https://github.com/geoparquet/geoparquet-io/pull/768)

### New Contributors

- @oakhill87 made their first contribution in https://github.com/geoparquet/geoparquet-io/pull/510
- @Sanjays2402 made their first contribution in https://github.com/geoparquet/geoparquet-io/pull/606
- @cayetanobv made their first contribution in https://github.com/geoparquet/geoparquet-io/pull/645

**Full Changelog**: https://github.com/geoparquet/geoparquet-io/compare/v1.3.0...v1.4.0

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
