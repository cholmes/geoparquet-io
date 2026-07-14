# Web-Optimized GeoParquet

`--optimize-for web` produces a GeoParquet 2.0 file tuned for browser map viewers
(hyparquet + deck.gl and similar stacks). It combines native Parquet
`GeospatialStatistics`, a covering `bbox` column, byte-targeted fetch-sized row
groups, a Parquet page index, and ZSTD compression, all written through the
memory-safe streaming writer, so inputs larger than RAM convert fine (DuckDB
spills the Hilbert sort to disk).

## When to use it

Use the web profile when the output file will be read directly by a browser
over HTTP range requests, for example a deck.gl or MapLibre viewer reading
GeoParquet with hyparquet. It is not needed for files that will only be
queried with DuckDB, a data warehouse, or another server-side engine, those
readers already get pushdown from the full Parquet footer regardless of row
group size.

## CLI usage

```bash
gpio convert geoparquet buildings.parquet buildings_web.parquet --optimize-for web
```

Tune the fetch unit, the compressed size of one row group, and therefore of
one HTTP range request, with `--row-group-size-mb`:

```bash
gpio convert geoparquet buildings.parquet buildings_web.parquet \
  --optimize-for web --row-group-size-mb 4
```

`--compression SNAPPY` is also supported for a lighter decoder bundle in the
browser (see Reader Contract below).

```bash
gpio convert geoparquet buildings.parquet buildings_web.parquet \
  --optimize-for web --compression SNAPPY
```

## Python usage

One-shot file to file, mirroring the CLI command:

```python
from geoparquet_io import convert_geoparquet

convert_geoparquet("buildings.gpkg", "buildings_web.parquet", optimize_for="web")
```

`convert_geoparquet` is also importable from `geoparquet_io.api`.

Fluent, in-memory chain:

```python
import geoparquet_io as gpio

gpio.convert("buildings.gpkg").sort_hilbert().write(
    "buildings_web.parquet", optimize_for="web"
)
```

When `optimize_for="web"` is passed to `Table.write(...)`, gpio forces
GeoParquet 2.0, routes the write through the streaming strategy, sizes row
groups by target bytes instead of a fixed row count, writes a Parquet page
index, and auto-adds a covering `bbox` column if the table does not already
have one.

## How row groups are sized

Each row group is one HTTP range request for a viewport. The profile sizes
row groups by target compressed bytes so you avoid both extremes, many tiny
requests that waste per-request overhead and bloat the footer, and few
oversized requests that over-fetch data outside the viewport.

The fetch-unit equation is:

```
rows_per_group = clamp(target_bytes / bytes_per_row, MIN, MAX)
```

with a footer guard that caps the total row group count so the initial footer
download stays small. Concretely:

- Default target is 8 MiB of compressed data per row group.
- Row count per group is clamped between 10,000 and 200,000 rows.
- If the clamped row count would produce more than about 1,000 row groups for
  the dataset, the row count is increased so the total stays under that cap.
- An explicit `--row-group-rows` (CLI) or `row_group_rows` (Python) overrides
  the equation outright.

Lower `--row-group-size-mb` (2 to 4) for dense interactive panning where you
want finer-grained range requests. Raise it for datasets viewed mostly at low
zoom, where fewer, larger requests reduce request overhead.

## Reader contract

The web profile targets a specific reading pattern. This section documents
the contract the file satisfies so a browser-side reader can rely on it.

**Footer-only row-group bbox index.** A stock hyparquet reader parses native
`GeospatialStatistics` for free from the footer alone, no page index needed.
The viewer reads each row group's bbox from
`metadata.row_groups[i].columns[j].meta_data.geospatial_statistics.bbox`
(native order is `{xmin, xmax, ymin, ymax}`, reorder to
`[xmin, ymin, xmax, ymax]` for use). When native geo statistics are absent,
for example because the reader does not yet support them, the viewer falls
back to the covering `bbox` column's per-row-group min and max statistics,
which gpio always writes alongside the native stats. `num_rows` is a BigInt in
the footer, and `rowStart` for a given row group is the cumulative sum of the
`num_rows` of every prior row group. The footer download itself is small
(tens of kilobytes for a typical file) and does not include the page index.

**Viewport to range-fetch.** hyparquet does not do min/max pushdown on its
own, so the viewer computes which row groups intersect the current viewport
itself, then issues one `parquetRead({rowStart, rowEnd, compressors})` call
per intersecting row group. Because row groups are byte-targeted (8 MiB by
default), each range request carries a useful payload without over-fetching
far outside the viewport. The covering `bbox` column also lets a viewer prune
candidate rows without decoding WKB geometry.

**Page index is optional.** `write_page_index` is on by default, and the page
index is written outside the footer, so page-level-pruning tools can opt in
with `useOffsetIndex: true` when reading. A plain row-group-level viewer
ignores the page index entirely and pays no cost for it. It lives after the
row groups it describes.

**Compression.** ZSTD is the default and decodes via `hyparquet-compressors`
in the browser. SNAPPY is available with `--optimize-for web --compression
SNAPPY` for viewers that want a smaller decompression bundle at the cost of a
larger file.

## See Also

- [Converting Formats](convert.md)
- [Write Strategies](write-strategies.md)
- [Checking Best Practices](check.md)
- [Python API Reference](../api/python-api.md)
