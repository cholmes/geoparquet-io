# Normalizing Schema Layout

The `normalize-schema` command rewrites a GeoParquet file's **schema** (not its
data, geometry encoding, or CRS) into a deterministic, portable layout. This is
the layout external table engines and downstream tools expect when the same
files must serve multiple access paths.

## What it does

=== "CLI"

    ```bash
    gpio normalize-schema input.parquet output.parquet
    ```

=== "Python"

    ```python
    import geoparquet_io as gpio

    gpio.read('input.parquet').normalize_schema().write('output.parquet')
    ```

Applied transformations:

1. **Lowercase column names** — uppercase names break some external readers.
   Use `--no-lowercase` to keep original casing.
2. **Deterministic order** — attributes first (original relative order), then the
   geometry column, then bbox covering columns **last**. This keeps attributes +
   geometry in a contiguous field-id block with no gaps.
3. **Contiguous `PARQUET:field_id`** — every top-level column gets a stable
   field-id (`1..N`), written into the real Parquet schema.
4. **Per-column descriptions** — optional, via `--descriptions`.

The GeoParquet `geo` metadata (`primary_column` and the bbox `covering`
references) is updated to track any renamed columns.

!!! note "Geometry encoding is not changed"
    Normalization only touches the schema (names, order, field-ids, descriptions).
    Geometry encoding and CRS are governed separately by
    [`--geoparquet-version`](convert.md) and `convert reproject`.

## Descriptions

Provide a JSON file mapping (final, lowercased) column names to descriptions:

```json
{
  "name": "Official feature name",
  "pop": "Population (2020 census)"
}
```

```bash
gpio normalize-schema input.parquet output.parquet --descriptions cols.json
```

Descriptions are written to each column's Parquet field metadata (`description`),
where external engines surface them as column comments.

## See Also

- [add command](add.md) — add a bbox covering before normalizing
- [Reducing Precision](reduce-precision.md) — shrink geometry before publishing
