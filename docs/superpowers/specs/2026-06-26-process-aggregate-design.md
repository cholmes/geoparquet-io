# Design: `gpio process aggregate`

**Date:** 2026-06-26
**Status:** Approved design, ready for implementation planning

## Problem

Visualizing very large GeoParquet datasets (e.g. ~1.5 billion field boundaries) at
low zoom is broken. Tiling the raw features either thins them so aggressively that
nothing is visible until you zoom way in, or returns so many features that the
frontend chokes. At low zoom the user does not want individual features — they
want a *sense of where the data is* and a few summary statistics they can color,
filter, or animate.

The fix is to pre-aggregate the data into a much smaller GeoParquet whose rows are
spatial buckets (grid cells or admin regions), each carrying summary statistics.
That aggregated file can be visualized directly, and later packed into a PMTiles
archive as the low-zoom layer (the PMTiles packaging is a separate, deferred
feature).

## Goals

- Reduce an arbitrarily large GeoParquet (single file, glob, or directory/Hive
  partition of GeoParquets) into a small GeoParquet of spatial buckets with
  per-bucket statistics.
- Support two bucketing schemes in v1:
  - **a5** — equal-area grid cells (statistically comparable cell-to-cell).
  - **admin** — administrative regions (country or one level below).
- Let the user choose which statistics each bucket carries: a count, numeric
  rollups (sum/avg/min/max of a column), and a category breakdown (count pivoted
  per category value — enabling class filtering and time sliders).
- Output a valid GeoParquet that can be visualized directly.

## Non-Goals (deferred)

- **h3** scheme — a trivial follow-up that reuses the a5 engine almost verbatim
  (`h3_latlng_to_cell_string` + `h3_cell_to_boundary_wkt`).
- **PMTiles multi-layer packaging** — the "aggregation at low zoom + full data at
  high zoom" archive is a separate later feature. This command only produces the
  aggregated GeoParquet that feeds it.
- **Per-category metric cross-tabs** — in v1 `--breakdown` pivots *count* only, not
  sum/avg per category. (e.g. no `sum_area_wheat`.)
- Other `gpio process` operations (simplify, dedupe, …) — `process` is introduced
  as a group with room to grow, but only `aggregate` ships here.

## Command Structure

A new top-level `process` command group, for operations that transform/reduce data
(distinct from `add` which enriches, `convert` which reformats). `aggregate` is the
first operation, sub-grouped by scheme to mirror `gpio partition`:

```
gpio process aggregate a5    <input> <output> [options]
gpio process aggregate admin <input> <output> [options]
```

Click supports group-under-group nesting; `aggregate` is a `Group` registered on
the `process` group, with `a5` and `admin` as commands on it.

## Architecture

### Shared pipeline (one DuckDB pass)

Every scheme is the same three steps:

1. **Assign a bucket key** to each input feature.
   - `a5`: `a5_lonlat_to_cell(<centroid lon>, <centroid lat>, <resolution>)`.
   - `admin`: point-in-polygon join of each feature's centroid against the
     simplified Overture admin cache (reusing `core/admin_datasets.py`), keyed by
     region id at `--level country|region`.
2. **`GROUP BY` the key**, computing the aggregation columns (see below). Only
   buckets that contain data are emitted (plus the admin `unassigned` bucket when
   relevant).
3. **Attach the bucket geometry** and write valid GeoParquet via the standard
   writer (bbox covering column + GeoParquet metadata).

Input resolution (single file / glob / directory / Hive partition) reuses the
existing `partition_input_options` and glob/dir handling so large multi-file
datasets work unchanged.

### Module layout

```
geoparquet_io/
  core/process/
    __init__.py
    aggregate/
      __init__.py
      common.py     # shared GROUP BY engine; metric/breakdown SQL builders;
                    # output column naming + collision handling; geometry attach
      by_a5.py      # key = a5 cell; geometry = a5_cell_to_boundary
      by_admin.py   # key = admin region (spatial join); geometry = region polygon
  cli/main.py       # `process` group + `aggregate` subgroup + a5/admin commands
  api/table.py      # Table.aggregate_a5(...) / Table.aggregate_admin(...)
  api/ops.py        # functional equivalents
```

Core may not import Click; API may not import CLI (enforced by import-linter).

## Aggregation Columns

Specified via flags; all computed in a single `GROUP BY`.

- **`count`** — always present. Number of input features in the bucket.
- **`--metric "sum:area_ha,avg:area_ha"`** — comma-separated `<func>:<column>`
  pairs. Functions: `sum`, `avg`, `min`, `max`. Each yields one cell-global column
  named `<func>_<column>` (e.g. `sum_area_ha`, `avg_area_ha`). NULLs are skipped
  per standard SQL aggregate semantics (documented in help/docs).
  - **Bare-column shorthand:** a metric entry with no `func:` prefix (e.g.
    `--metric area_ha`) is treated as `sum:area_ha`. `sum` is the default because
    a total (e.g. total hectares per cell) is the most common visualization intent.
    Bare and `func:` forms can be mixed: `--metric "area_ha,avg:yield"`.
- **`--breakdown <column>`** — pivots count by the distinct values of a categorical
  or time-bucket column into `count_<value>` columns. Enables class filtering
  (5 crop classes → 5 toggleable columns) and time animation (one column per year).
  - Capped at `--breakdown-limit` (default **20**) most-common values by frequency.
  - Values beyond the cap roll into a single **`count_other`** column.
  - In v1, breakdown pivots **count only**.

### Output column naming & collisions

`count_<value>` columns derive from data values, which may contain characters
unsuitable for clean column names. Sanitize values to safe column names, but
**disambiguate on collision** — keep an explicit value→column map and suffix
colliding names (e.g. `count_foo`, `count_foo_2`) rather than silently merging two
categories into one column. (Past `sanitize_filename` collisions caused silent row
loss in partitioning; we must not repeat that pattern here.)

## Output Geometry

`--out-geometry polygon|centroid|both` (default **`polygon`**):

- **`polygon`** — a5 cell boundary (`a5_cell_to_boundary`) / admin region polygon.
  True choropleth: fill each bucket by count / metric / share.
- **`centroid`** — representative point: `a5_cell_to_lonlat` / admin region
  centroid. For proportional-symbol (bubble) maps.
- **`both`** — polygon as the primary `geometry` column plus an extra `centroid`
  point column (e.g. for label placement).

## Scheme-Specific Details

### a5

- `--resolution <int>` — explicit a5 resolution.
- `--auto` — mutually exclusive with `--resolution`; auto-picks the resolution by
  reusing/adapting `core/partition/auto_resolution.py`. The aggregation target
  differs from partitioning: rather than "rows per partition file," it aims for a
  **sensible number of output cells for visualization** — enough cells to reveal
  spatial pattern without overwhelming a frontend. The exact heuristic (target cell
  count / range) is finalized during implementation; `--resolution` always
  overrides.
- Geometry source: `a5_cell_to_boundary` (polygon), `a5_cell_to_lonlat` (centroid).

### admin

- `--level country|region` — administrative level (mirrors admin partition levels).
- Bucketing is a centroid point-in-polygon join against the simplified Overture
  admin cache via `core/admin_datasets.py`.
- **Unassigned bucket:** features whose centroid falls outside every admin region
  go into a single `unassigned` bucket. `count`, metrics, and breakdown are still
  computed for it; its geometry is `NULL` (valid in GeoParquet). The count of
  unassigned features is logged. No data silently disappears.
- Geometry source: the admin region polygon (and region centroid for `centroid`/
  `both`).

## Error Handling

- Use `ClickException` for user-facing errors (CLI), domain exceptions in core.
- `--resolution` and `--auto` are mutually exclusive → clear error if both given.
- `--metric` parse errors (bad function, missing column) → clear, early error
  before the heavy pass, listing valid functions.
- Empty input / no features → clear message, no crash.
- Reuse existing DuckDB tuning patterns for large joins (temp_directory, spill
  settings) consistent with the partition code.

## Testing (TDD — write tests first)

- **Unit:** metric SQL builder, breakdown SQL builder, output-column naming +
  collision disambiguation, `--metric` parsing/validation, `--resolution`/`--auto`
  mutual exclusion.
- **a5 end-to-end:** small fixture with known points → deterministic cell counts;
  verify `count`, `sum`/`avg`, breakdown columns, `count_other` cap behavior, and
  each `--out-geometry` variant produces valid GeoParquet with the expected
  geometry type.
- **admin end-to-end:** a handful of features over a known region (and at least one
  point outside all regions) → verify region buckets, the `unassigned` bucket, and
  logged unassigned count.
- **Output validity:** outputs pass GeoParquet validation (bbox covering +
  metadata present).
- Markers: heavier cases tagged `slow`; anything needing remote admin data tagged
  `network`.

## Documentation

- `docs/guide/` page for `gpio process aggregate` with CLI + Python API examples in
  the tabbed format.
- Update `docs/api/python-api.md` for the new Table methods / ops functions.
- Update root `CHANGELOG.md` (doc-sync copies to `docs/CHANGELOG.md`).

## Future Work

- `gpio process aggregate h3` (reuses a5 engine).
- Per-category metric cross-tabs (`sum_<col>_<category>`).
- PMTiles multi-layer packaging using this command's output as the low-zoom layer.
- Additional `gpio process` operations.
