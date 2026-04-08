# geoparquet-io

[![Tests](https://github.com/geoparquet/geoparquet-io/actions/workflows/tests.yml/badge.svg)](https://github.com/geoparquet/geoparquet-io/actions/workflows/tests.yml)
[![codecov](https://codecov.io/gh/geoparquet/geoparquet-io/branch/main/graph/badge.svg)](https://codecov.io/gh/geoparquet/geoparquet-io)
[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![PyPI version](https://badge.fury.io/py/geoparquet-io.svg)](https://badge.fury.io/py/geoparquet-io)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/geoparquet/geoparquet-io/blob/main/LICENSE)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

**[Documentation](https://geoparquet.io/)** · **[Quick Start](https://geoparquet.io/getting-started/quickstart/)** · **[Python API](https://geoparquet.io/api/python-api/)** · **[Contributing](https://geoparquet.io/contributing/)**

`geoparquet-io` offers a CLI and a fluent Python API to help you create, validate, and optimize GeoParquet files.

`geoparquet-io` (or, `gpio`) is written in Python and uses DuckDB (with GDAL embedded for legacy format support), PyArrow, and `obstore` for fast operations on larger-than-memory datasets. By default, `gpio` enforces best practices: bbox columns, Hilbert ordering, ZSTD compression, and smart row group sizes.

Additional features include:
- **Unix pipes** with Arrow IPC streaming
- Read and write to **object storage** (S3, GCS, Azure, HTTPS, etc.)
- Easily add **spatial indices** (bbox, H3, quadkey, S2, A5, KD-tree)
- Support for **GeoParquet 1.1 and 2.0**
- **bbox-based subsetting** of datasets for spatial filtering and extraction
- **Service extraction** from ArcGIS Feature Services, BigQuery tables, and WFS → GeoParquet
- **Easy inspection** of metadata, row previews, and statistics
- **PMTiles** generation via the `gpio-pmtiles` plugin
- A **Claude Code skill** for AI-assisted spatial data workflows

## Installation

```bash
uv tool install geoparquet-io    # CLI tool (recommended)
uv add geoparquet-io             # Python library
```
See [Installation Guide](https://geoparquet.io/getting-started/installation/) for more details.

## Quick Examples

With `gpio convert`, you can seamlessly convert from (and to) legacy formats like Shapefiles, GeoJSON, and GeoPackages:
```bash
# One command: converts, adds bbox, Hilbert-sorts, compresses
gpio convert buildings.shp buildings.parquet
```

One of `gpio`’s strengths is composability. On the CLI, commands chain together with Unix pipes using Arrow IPC streaming—no intermediate files:

```bash
# Extract Senegal from global admin boundaries, Hilbert-sort
gpio extract --bbox "-18,14,-11,18" \
  https://data.fieldmaps.io/edge-matched/humanitarian/intl/adm2_polygons.parquet | \
  gpio sort hilbert - senegal_adm2.parquet
```

```bash
# Chain enrichment steps together
gpio add bbox input.parquet | \
  gpio add h3 --resolution 9 - | \
  gpio sort hilbert - enriched.parquet
```

The Python API mirrors this with a fluent interface:

```python
import geoparquet_io as gpio

gpio.read('buildings.parquet') \
    .add_bbox() \
    .add_h3(resolution=9) \
    .sort_hilbert() \
    .write('s3://bucket/optimized.parquet')
```

Cloud I/O is handled automatically via DuckDB and `obstore`, so commands like this just work:
```bash
# Convert shapefile → auto-partition by H3 → write directly to S3
gpio convert large_roads.shp | \
  gpio partition h3 - s3://bucket/roads/ --auto --hive --profile prod
```
See the [User Guide](https://geoparquet.io/guide/inspect/) for spatial indexing, partitioning, cloud storage, and more.

## For Development

```bash
git clone https://github.com/geoparquet/geoparquet-io.git
cd geoparquet-io
uv sync --all-extras
uv run geoparquet-io --help
```

See [Contributing Guide](https://geoparquet.io/contributing/) for full development setup.

## License

[Apache 2.0](LICENSE)
