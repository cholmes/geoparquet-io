# Canonical sample dataset

One small, spec-clean dataset for the documentation examples, the end-to-end
journey tests, and the `examples/` notebooks to share. Those consumers land in
their own changes; this is the data they are being written against. Doc examples
name their inputs with placeholders (`input.parquet`, `data.parquet`,
`places.parquet`, `buildings.parquet`, `input.geojson`), and the docs-as-tests
harness will seed a temp directory from these files under those names, so an
example can be executed verbatim without editing the doc.

Everything here is a derived artifact. Nothing is hand-made, and nothing should
be edited in place — change the generator and rerun it.

## Files

| File | Rows | Geometry | Derived from |
|------|------|----------|--------------|
| `places.parquet` | 766 | POINT | `tests/data/places_test.parquet` |
| `buildings.parquet` | 42 | POLYGON | `tests/data/buildings_test.parquet` |
| `places.geojson` | 766 | Point | `places.parquet` |
| `places.csv` | 766 | `lon`/`lat` columns | `places.parquet` |

About 405 KB in total, mirrored byte-for-byte into `examples/data/`.

**`places.parquet`** — a Foursquare places extract over northern Ghana and Togo
(lon -0.989…1.481, lat 9.799…12.391). Columns: `fsq_place_id`, `name`,
`address`, `placemaker_url`, `geometry`, `bbox`. These are real attributes with
the awkwardness real data has: 189 of the 766 names contain non-ASCII
characters, 12 names are duplicates, and 513 addresses are null — useful for
`--where`, `sort column`, and `partition string` examples.

**`buildings.parquet`** — 42 building footprints from a small area near the
German–Belgian border (lon 6.124…6.151, lat 50.121…50.138). Columns: `id`,
`geometry`, `bbox`. Same features as the pre-existing
`examples/data/sample.parquet`, upgraded to 1.1 with a bbox covering.

**`places.geojson`** — RFC 7946 `FeatureCollection`. The `bbox` covering is a
GeoParquet construct and is dropped, so each feature carries the four
attributes only. Coordinates are written at 7 decimal places (~1 cm).

**`places.csv`** — attributes plus `lon` and `lat` at full double precision,
which is the shape `gpio convert geoparquet` auto-detects for point CSVs.

Both parquet files are GeoParquet **1.1** with a `bbox` covering, Hilbert-sorted,
ZSTD-compressed, single row group. `gpio check all` reports no warnings on
either, and `validate_geoparquet` passes with zero failures — which is the point:
the tool's own sample data should model what the tool recommends. 1.1 rather than
2.0 keeps the files readable by the widest range of clients that a doc example
might be run against.

## Regenerating

```bash
uv run python tests/data/canonical/generate_canonical.py
```

The parquet files and the GeoJSON are produced by the real `gpio` CLI
(`sort hilbert --add-bbox`, `convert geojson`), so regenerating also smoke-tests
the tool. Only the lon/lat CSV goes straight to DuckDB, because
`gpio convert csv` emits WKT plus the bbox struct rather than coordinate columns.

Output is byte-reproducible: the CLI is deterministic for these inputs and the
DuckDB query runs single-threaded with insertion order preserved. Rerunning on
unchanged sources leaves `git status` clean. The script also refreshes the
`examples/data/` mirror; it never touches `examples/data/sample.parquet`, which
the notebooks still reference.

`--output-dir DIR` writes the dataset somewhere else and skips the mirror, so a
regeneration can be inspected without touching the repository:

```bash
uv run python tests/data/canonical/generate_canonical.py --output-dir /tmp/check
```

`tests/test_canonical_dataset.py` pins the shape of all of this — row counts,
exact column lists, geo metadata, clean validation, the mirror, and the size
budget — so the dataset cannot rot silently. If a change here is deliberate,
update those constants in the same commit.

One test there is slow-marked: it regenerates into a temp directory via
`--output-dir` and compares SHA-256 against the committed files. That is the
guard against the failure mode the rest of the suite cannot see — a change in
gpio's own output leaving these files stale and the reproducibility claim above
quietly false. When it fails, rerun the generator and commit the result.

## Known gaps

Doc examples reference plenty of attribute columns that this data does not have:
`population` (~26 mentions in `docs/guide/`), `region` (~34), `category` (~24),
`area_ha` (~15), `height` (~14), `crop_type` (~10). Those columns were not
invented here — synthetic attributes bolted onto real geometry would make the
sample data lie about what it is. Blocks that need them are the docs-harness's
problem to skip or edit.

`country` and `region` are a partial exception: `gpio add admin-divisions`
generates them, so an example that runs that command first does have them.

Round-tripping `places.geojson` back through `gpio convert geoparquet` yields
GDAL's column naming — an extra `OGC_FID` and a geometry column named `geom`
rather than `geometry`. Journey tests comparing a GeoJSON round-trip against
`places.parquet` need to account for that.
