"""
Tests for schema unification utilities in core/common.py.

These utilities handle type mismatches when merging paginated results,
specifically the int64 vs decimal128 issue (GitHub #475).
"""

import pyarrow as pa

from geoparquet_io.core.common import (
    _cast_table_to_schema,
    _compute_unified_schema,
    _promote_numeric_type,
)


class TestPromoteNumericType:
    """Tests for _promote_numeric_type()."""

    def test_same_types_unchanged(self):
        assert _promote_numeric_type(pa.int64(), pa.int64()) == pa.int64()
        assert _promote_numeric_type(pa.float64(), pa.float64()) == pa.float64()
        assert _promote_numeric_type(pa.string(), pa.string()) == pa.string()

    def test_null_yields_other_type(self):
        assert _promote_numeric_type(pa.null(), pa.int64()) == pa.int64()
        assert _promote_numeric_type(pa.float64(), pa.null()) == pa.float64()
        assert _promote_numeric_type(pa.null(), pa.null()) == pa.null()

    def test_int_int_promotes_to_int64(self):
        assert _promote_numeric_type(pa.int16(), pa.int32()) == pa.int64()
        assert _promote_numeric_type(pa.int32(), pa.int64()) == pa.int64()
        assert _promote_numeric_type(pa.int8(), pa.int64()) == pa.int64()

    def test_float_float_promotes_to_float64(self):
        assert _promote_numeric_type(pa.float32(), pa.float64()) == pa.float64()
        assert _promote_numeric_type(pa.float64(), pa.float32()) == pa.float64()

    def test_int_float_promotes_to_float64(self):
        assert _promote_numeric_type(pa.int64(), pa.float64()) == pa.float64()
        assert _promote_numeric_type(pa.float32(), pa.int32()) == pa.float64()

    def test_int_decimal_promotes_to_float64(self):
        """This is the key case from issue #475."""
        assert _promote_numeric_type(pa.int64(), pa.decimal128(38, 0)) == pa.float64()
        assert _promote_numeric_type(pa.decimal128(38, 0), pa.int64()) == pa.float64()

    def test_decimal_float_promotes_to_float64(self):
        assert _promote_numeric_type(pa.decimal128(38, 0), pa.float64()) == pa.float64()
        assert _promote_numeric_type(pa.float32(), pa.decimal128(18, 2)) == pa.float64()

    def test_decimal_decimal_different_precision_promotes_to_float64(self):
        assert _promote_numeric_type(pa.decimal128(38, 0), pa.decimal128(18, 2)) == pa.float64()

    def test_string_wins_over_numeric(self):
        assert _promote_numeric_type(pa.string(), pa.int64()) == pa.string()
        assert _promote_numeric_type(pa.float64(), pa.string()) == pa.string()
        assert _promote_numeric_type(pa.decimal128(38, 0), pa.string()) == pa.string()

    def test_large_string_treated_as_string(self):
        assert _promote_numeric_type(pa.large_string(), pa.int64()) == pa.string()


class TestComputeUnifiedSchema:
    """Tests for _compute_unified_schema()."""

    def test_empty_list_returns_empty_schema(self):
        result = _compute_unified_schema([])
        assert result == pa.schema([])

    def test_single_schema_unchanged(self):
        schema = pa.schema([("a", pa.int64()), ("b", pa.string())])
        result = _compute_unified_schema([schema])
        assert result == schema

    def test_identical_schemas_unchanged(self):
        schema = pa.schema([("a", pa.int64()), ("b", pa.string())])
        result = _compute_unified_schema([schema, schema, schema])
        assert result == schema

    def test_int_decimal_unified_to_float64(self):
        """Core test case for issue #475."""
        schema1 = pa.schema([("fun", pa.int64()), ("name", pa.string())])
        schema2 = pa.schema([("fun", pa.decimal128(38, 0)), ("name", pa.string())])

        result = _compute_unified_schema([schema1, schema2])

        assert result.field("fun").type == pa.float64()
        assert result.field("name").type == pa.string()

    def test_preserves_field_order_from_first_schema(self):
        schema1 = pa.schema([("a", pa.int64()), ("b", pa.string()), ("c", pa.float32())])
        schema2 = pa.schema([("a", pa.int32()), ("b", pa.string()), ("c", pa.float64())])

        result = _compute_unified_schema([schema1, schema2])

        assert [f.name for f in result] == ["a", "b", "c"]
        assert result.field("a").type == pa.int64()
        assert result.field("c").type == pa.float64()

    def test_handles_null_type_in_some_schemas(self):
        schema1 = pa.schema([("x", pa.null())])
        schema2 = pa.schema([("x", pa.int64())])

        result = _compute_unified_schema([schema1, schema2])

        assert result.field("x").type == pa.int64()

    def test_multiple_schemas_progressive_promotion(self):
        schema1 = pa.schema([("val", pa.int32())])
        schema2 = pa.schema([("val", pa.int64())])
        schema3 = pa.schema([("val", pa.float64())])

        result = _compute_unified_schema([schema1, schema2, schema3])

        assert result.field("val").type == pa.float64()


class TestCastTableToSchema:
    """Tests for _cast_table_to_schema()."""

    def test_identical_schema_returns_same_table(self):
        table = pa.table({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        result = _cast_table_to_schema(table, table.schema)
        assert result.equals(table)

    def test_casts_int64_to_float64(self):
        table = pa.table({"x": pa.array([1, 2, 3], type=pa.int64())})
        target = pa.schema([("x", pa.float64())])

        result = _cast_table_to_schema(table, target)

        assert result.schema.field("x").type == pa.float64()
        assert result.column("x").to_pylist() == [1.0, 2.0, 3.0]

    def test_casts_decimal_to_float64(self):
        table = pa.table({"x": pa.array([1, 2, 3], type=pa.decimal128(38, 0))})
        target = pa.schema([("x", pa.float64())])

        result = _cast_table_to_schema(table, target)

        assert result.schema.field("x").type == pa.float64()
        assert result.column("x").to_pylist() == [1.0, 2.0, 3.0]

    def test_adds_missing_columns_as_nulls(self):
        table = pa.table({"a": [1, 2, 3]})
        target = pa.schema([("a", pa.int64()), ("b", pa.string())])

        result = _cast_table_to_schema(table, target)

        assert result.num_columns == 2
        assert result.column("b").to_pylist() == [None, None, None]

    def test_preserves_column_order_from_target(self):
        table = pa.table({"b": ["x", "y"], "a": [1, 2]})
        target = pa.schema([("a", pa.int64()), ("b", pa.string())])

        result = _cast_table_to_schema(table, target)

        assert result.column_names == ["a", "b"]


class TestSchemaUnificationIntegration:
    """Integration tests for the full unification workflow."""

    def test_concat_after_unification_succeeds(self):
        """Simulates the WFS pagination scenario from issue #475."""
        # Page 1: column 'fun' inferred as int64
        table1 = pa.table(
            {
                "geometry": pa.array([b"\x01\x02", b"\x03\x04"], type=pa.binary()),
                "fun": pa.array([100, 200], type=pa.int64()),
                "name": pa.array(["a", "b"], type=pa.string()),
            }
        )

        # Page 2: column 'fun' inferred as decimal128 (large integers)
        table2 = pa.table(
            {
                "geometry": pa.array([b"\x05\x06", b"\x07\x08"], type=pa.binary()),
                "fun": pa.array([300, 400], type=pa.decimal128(38, 0)),
                "name": pa.array(["c", "d"], type=pa.string()),
            }
        )

        # Without unification, this would fail:
        # pa.concat_tables([table1, table2], promote=True)
        # -> "Unable to merge: Field fun has incompatible types"

        # With unification:
        schemas = [table1.schema, table2.schema]
        unified = _compute_unified_schema(schemas)

        cast1 = _cast_table_to_schema(table1, unified)
        cast2 = _cast_table_to_schema(table2, unified)

        combined = pa.concat_tables([cast1, cast2])

        assert combined.num_rows == 4
        assert combined.schema.field("fun").type == pa.float64()
        assert combined.column("fun").to_pylist() == [100.0, 200.0, 300.0, 400.0]

    def test_multiple_pages_with_varying_types(self):
        """Tests unification across many pages with different type patterns."""
        tables = [
            pa.table({"x": pa.array([1], type=pa.int32())}),
            pa.table({"x": pa.array([2], type=pa.int64())}),
            pa.table({"x": pa.array([3], type=pa.decimal128(38, 0))}),
            pa.table({"x": pa.array([4], type=pa.float32())}),
        ]

        schemas = [t.schema for t in tables]
        unified = _compute_unified_schema(schemas)
        cast_tables = [_cast_table_to_schema(t, unified) for t in tables]

        combined = pa.concat_tables(cast_tables)

        assert combined.num_rows == 4
        assert combined.schema.field("x").type == pa.float64()
