# ADR: WFS Extractor Design

**Date:** 2026-03-23
**Status:** Proposed
**Deciders:** Nissim Lebovits, Claude

---

## Context

geoparquet-io currently supports extraction from ArcGIS REST Feature Services and BigQuery tables. Users have requested support for WFS (Web Feature Service), a widely-used OGC standard for serving vector geospatial data over HTTP. Many government agencies, municipalities, and organizations publish data via WFS, making it a valuable data source alongside existing extractors.

**Target Use Case:** Enable users to extract data from public WFS servers (e.g., USGS, state GIS portals, municipal data services) with automatic pagination, intelligent spatial filtering, and memory-efficient streaming to GeoParquet format.

---

## Decision

Implement a WFS extractor following these design principles:

### 1. **WFS Version Support**
Support WFS 1.0.0 and 1.1.0 (defer WFS 2.0 to future work).

**Rationale:**
- WFS 1.0/1.1 covers the vast majority of existing public WFS servers
- Simpler GetFeature API without complex filter encoding requirements
- YAGNI principle: add WFS 2.0 features only when needed

### 2. **Authentication**
Support public WFS servers only in v1 (no authentication).

**Rationale:**
- Most valuable public data sources don't require auth (USGS, state portals, OSM-based services)
- Authentication requires access to private WFS servers for testing
- Can be added in follow-up issue after core functionality is proven

**Deferred:** Basic HTTP Auth, API keys, token-based auth

### 3. **Bbox Filtering Strategy**
Reuse BigQuery's three-mode bbox filtering: `auto`, `server`, `local`.

**Rationale:**
- Proven pattern that balances performance and flexibility
- Users understand the tradeoffs from BigQuery extractor
- Consistent UX across extractors

**Implementation:**
- `auto` mode defaults to server-side for WFS (conservative for remote services)
- `server` mode pushes bbox as WFS request parameter
- `local` mode applies DuckDB `ST_Intersects` filter after download

### 4. **Attribute Filtering**
Support bbox filtering only (defer CQL/FES WHERE clause translation).

**Rationale:**
- Bbox is the most common spatial filtering need
- CQL/FES translation is complex (SQL → OGC Filter Encoding XML)
- Users can filter locally after download: `gpio extract output.parquet filtered.parquet --where "..."`
- Piping workflow already works well for multi-stage filtering

**Deferred:** SQL WHERE → CQL_FILTER translation using OWSLib's `fes` module

### 5. **Pagination**
Auto-detect server capabilities and page automatically with progress tracking.

**Rationale:**
- Follows proven ArcGIS pattern (transparent, "just works")
- Different WFS servers have different max feature limits
- Progress tracking keeps users informed on large datasets

**Implementation:**
- Use `resultType=hits` to get total count (WFS 1.1+)
- Page using `startIndex` + `maxFeatures` parameters
- Generator pattern yields pages for memory efficiency

### 6. **Geometry Format Negotiation**
Auto-detect and prefer best available format: GeoJSON > GML3 > GML2.

**Rationale:**
- GeoJSON is fastest to parse (simple JSON vs complex XML)
- Not all servers support GeoJSON, so fallback to GML ensures broad compatibility
- Automatic negotiation provides best performance without user configuration

**Implementation:**
- Check `GetCapabilities` for supported output formats
- Request preferred format via `outputFormat` parameter
- Parse both GeoJSON and GML via DuckDB's `ST_Read()` + GDAL

### 7. **CRS Handling**
Auto-negotiate CRS (try EPSG:4326, accept server default) with optional `--output-crs`.

**Rationale:**
- EPSG:4326 (WGS84) is most universal for GeoParquet
- Many WFS servers default to native projection (e.g., national grids)
- Explicit `--output-crs` provides control for advanced users

**Implementation:**
- Try EPSG:4326 variants: `EPSG:4326`, `urn:ogc:def:crs:EPSG::4326`, `http://www.opengis.net/def/crs/EPSG/0/4326`
- Fall back to server's default CRS with metadata preservation
- Optional local reprojection via `--output-crs` (using DuckDB/PyGeoArrow)

### 8. **Layer/Typename Selection**
Single typename argument with namespace auto-resolution and layer listing.

**Rationale:**
- WFS typenames are the natural layer identifier (like BigQuery table names)
- Namespace prefixes vary (`topp:states` vs `states`), auto-resolution improves UX
- Listing layers (when typename omitted) aids discovery

**Implementation:**
- Match typename with/without namespace: `states` → `topp:states`
- List available layers via `gpio extract wfs <url>` (no typename)
- Clear error messages with suggestions when typename not found

---

## Architecture

### Two-Pass Streaming Pattern (from ArcGIS)

```
┌─────────────┐
│ WFS Server  │
└──────┬──────┘
       │ GetFeature (page 1)
       ▼
┌─────────────────────────┐
│ Parse GeoJSON/GML       │
│ → PyArrow Table         │
└──────┬──────────────────┘
       │ Stream write
       ▼
┌─────────────────────────┐
│ Temp Parquet File       │ ◄─── Memory-efficient:
│ (constant RAM usage)    │      handles 50GB on 4GB RAM
└──────┬──────────────────┘
       │ Read back
       ▼
┌─────────────────────────┐
│ PyArrow Table           │
│ + Post-processing       │
│   • Local bbox filter   │
│   • Hilbert ordering    │
│   • Bbox column         │
└──────┬──────────────────┘
       │
       ▼
┌─────────────────────────┐
│ Final GeoParquet File   │
└─────────────────────────┘
```

**Why this pattern:**
- Constant memory regardless of dataset size
- Produces complete Arrow tables for post-processing
- Proven reliable with ArcGIS extractor (handles millions of features)

### Code Reuse Strategy

| Component | Reuse From | Justification |
|-----------|------------|---------------|
| HTTP client + retries | `arcgis.py` | Proven connection pooling, retry logic, error handling |
| Bbox filtering logic | `extract_bigquery.py` | Three-mode strategy (auto/server/local) already understood by users |
| Bbox string parsing | `extract.py` | Standard `parse_bbox()` function |
| Streaming to Parquet | `arcgis.py` | Fixed schema pattern prevents type mismatches between pages |
| GeoJSON → Arrow | `arcgis.py` | DuckDB `ST_Read()` + `ST_AsWKB()` conversion |
| CRS metadata | `common.py` | `parse_crs_string_to_projjson()` for PROJJSON conversion |

---

## Implementation Plan

### Key Files

1. **`geoparquet_io/core/wfs.py`** (~1000-1200 lines)
   - Data structures: `WFSLayerInfo`, `WFSCapabilities`
   - HTTP client (reuse ArcGIS pattern)
   - Capability parsing with OWSLib
   - Bbox filter construction
   - Pagination with progress tracking
   - Multi-format geometry parsing (GeoJSON + GML)
   - CRS negotiation
   - Streaming to Parquet
   - High-level functions: `wfs_to_table()`, `convert_wfs_to_geoparquet()`

2. **CLI Command** (in `cli/main.py`)
   - `@extract.command(name="wfs")`
   - Standard decorators: `@compression_options`, `@row_group_options`, `@verbose_option`
   - Special: typename optional (lists layers if omitted)

3. **Python API** (in `api/`)
   - `ops.from_wfs()` - Functional API
   - `Table.from_wfs()` - Chainable API

4. **Tests** (`tests/test_wfs.py` ~800-1000 lines)
   - Unit tests (bbox strategy, filter construction, format detection, CRS negotiation)
   - Mock-based tests (capability parsing, pagination, error handling)
   - Integration tests against real WFS (marked `@pytest.mark.network`)

5. **Dependencies**
   - Add `owslib>=0.29.0` (OSGeo-maintained OGC services library)

### CLI Examples

```bash
# Extract entire layer
gpio extract wfs https://geo.example.com/wfs cities output.parquet

# With bbox filter (server-side)
gpio extract wfs https://geo.example.com/wfs roads output.parquet \
    --bbox -122.5,37.5,-122.0,38.0

# Limit features and specify CRS
gpio extract wfs https://geo.example.com/wfs parcels output.parquet \
    --limit 10000 --output-crs EPSG:3857

# List available layers
gpio extract wfs https://geo.example.com/wfs
```

### Python API Example

```python
from geoparquet_io.api import Table

# Chainable API
Table.from_wfs('https://geo.example.com/wfs', 'cities') \
    .add_bbox() \
    .sort_hilbert() \
    .write('cities.parquet')

# With filtering
Table.from_wfs(
    'https://geo.example.com/wfs',
    'buildings',
    bbox=(-122.5, 37.5, -122.0, 38.0),
    limit=1000
).write('buildings.parquet')
```

---

## Consequences

### Positive

- **Consistency**: Follows proven ArcGIS and BigQuery patterns, users already understand these workflows
- **Reliability**: Two-pass streaming handles larger-than-memory datasets
- **Compatibility**: Multi-format support (GeoJSON + GML) works with diverse WFS implementations
- **Performance**: Auto-negotiation provides best performance without user configuration
- **Maintainability**: Aggressive code reuse minimizes new code surface area

### Negative

- **No authentication**: Public WFS only in v1 (deferred to follow-up)
- **No attribute filtering**: Bbox only, no CQL/FES WHERE clause translation (deferred)
- **WFS 1.x only**: No WFS 2.0 specific features (deferred)

### Neutral

- **New dependency**: Adds `owslib` (but it's OSGeo-maintained and widely used)
- **Code volume**: ~2100-2600 lines total (core + tests + docs)

---

## Deferred to Future Issues

These features are explicitly deferred to keep v1 focused:

### 1. WFS Authentication Support
- Basic HTTP Auth (username/password)
- API key support (query param or header)
- Token-based auth (like ArcGIS)
- **Rationale**: Requires private WFS servers for testing

### 2. Server-Side Attribute Filtering
- Translate SQL WHERE clauses to CQL_FILTER
- Use OWSLib's `fes` module for OGC Filter Encoding
- Support basic operators: `=`, `>`, `<`, `LIKE`, `IN`, `AND`, `OR`
- **Rationale**: Complex feature requiring SQL→CQL parser, can be added incrementally

### 3. WFS 2.0 Support
- Enhanced pagination (improved `startIndex`/`count` handling)
- Filter Encoding 2.0
- Additional capabilities
- **Rationale**: WFS 1.x covers vast majority of existing services

---

## References

- **WFS Specification**: https://www.ogc.org/standards/wfs
- **OWSLib Documentation**: https://geopython.github.io/OWSLib/
- **Existing Patterns**:
  - ArcGIS extractor: `geoparquet_io/core/arcgis.py`
  - BigQuery extractor: `geoparquet_io/core/extract_bigquery.py`
  - Bbox parsing: `geoparquet_io/core/extract.py`

---

## Implementation Checklist

### Core Module
- [ ] Create `geoparquet_io/core/wfs.py`
- [ ] Data structures: `WFSLayerInfo`, `WFSCapabilities`
- [ ] HTTP client setup (reuse ArcGIS pattern)
- [ ] Capability parsing with OWSLib
- [ ] Layer info and DescribeFeatureType parsing
- [ ] Bbox filter construction (server + local)
- [ ] Pagination logic with progress tracking
- [ ] Format detection and negotiation
- [ ] GeoJSON→Arrow conversion (via DuckDB)
- [ ] GML→Arrow conversion (via DuckDB)
- [ ] CRS negotiation logic
- [ ] Streaming to Parquet function
- [ ] `wfs_to_table()` main function
- [ ] `convert_wfs_to_geoparquet()` CLI wrapper
- [ ] `list_available_layers()` helper

### CLI Command
- [ ] Add `@extract.command(name="wfs")` to `cli/main.py`
- [ ] All option decorators
- [ ] Docstring with examples
- [ ] List layers mode (typename optional)
- [ ] Error handling and user-friendly messages

### Python API
- [ ] Add `ops.from_wfs()` to `api/ops.py`
- [ ] Add `Table.from_wfs()` to `api/table.py`
- [ ] Docstrings with examples

### Tests
- [ ] Create `tests/test_wfs.py`
- [ ] Unit tests: bbox strategy, filter construction, format detection, CRS negotiation
- [ ] Mock-based tests: capability parsing, pagination, error handling
- [ ] Integration tests (marked `@pytest.mark.network`)
- [ ] Edge case coverage: empty results, missing geometry, namespace resolution

### Documentation
- [ ] Update `docs/guide/extract.md` with WFS section
- [ ] Update `docs/api/python-api.md` with WFS API
- [ ] Update `CLAUDE.md` CLI command table
- [ ] Add examples for common public WFS services

### Dependencies
- [ ] Add `owslib>=0.29.0` to `pyproject.toml`
- [ ] Test installation: `uv pip install -e .`

### Verification
- [ ] Extract from public WFS (USGS)
- [ ] Test bbox filtering (server + local modes)
- [ ] Test list layers mode
- [ ] Test Python API
- [ ] Run all tests (unit + integration)
- [ ] Check test coverage (>80% for new code)
- [ ] Verify complexity grade A (`xenon --max-absolute=A geoparquet_io/core/wfs.py`)

---

## Estimates

- **Lines of Code**: ~2100-2600 total
  - Core module: ~1000-1200 lines
  - Tests: ~800-1000 lines
  - CLI/API: ~100 lines
  - Docs: ~200 lines

- **Test Coverage Target**: >80% for new code (following project standard)

- **Complexity Target**: Grade A (maintained through modular design)

- **Development Time**: ~15-22 hours
  - Core module: 8-10 hours
  - Tests: 4-6 hours
  - CLI/API: 1-2 hours
  - Documentation: 2-4 hours
