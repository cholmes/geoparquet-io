"""Tests for CLI exception handler."""

import click
import pytest

from geoparquet_io.cli.exception_handler import (
    core_exception_handler,
    handle_core_exception,
)
from geoparquet_io.core.exceptions import (
    FileNotFoundGeoParquetError,
    GeoParquetError,
    InvalidParameterError,
    PartitionError,
    RemoteAccessError,
    ValidationError,
)


class TestExceptionHandler:
    """Test CLI exception handling."""

    def test_converts_file_not_found_to_click_exception(self):
        exc = FileNotFoundGeoParquetError("test.parquet")
        click_exc = handle_core_exception(exc)
        assert isinstance(click_exc, click.ClickException)
        assert "test.parquet" in click_exc.message

    def test_converts_invalid_parameter_to_bad_parameter(self):
        exc = InvalidParameterError("resolution", "must be 1-15")
        click_exc = handle_core_exception(exc)
        assert isinstance(click_exc, click.BadParameter)
        assert "resolution" in str(click_exc.message)

    def test_converts_remote_access_error_to_click_exception(self):
        exc = RemoteAccessError("s3://bucket/file", "Access denied")
        click_exc = handle_core_exception(exc)
        assert isinstance(click_exc, click.ClickException)

    def test_converts_partition_error_to_click_exception(self):
        exc = PartitionError("Failed to partition")
        click_exc = handle_core_exception(exc)
        assert isinstance(click_exc, click.ClickException)
        assert "Failed to partition" in click_exc.message

    def test_converts_validation_error_to_click_exception(self):
        exc = ValidationError("Invalid GeoParquet")
        click_exc = handle_core_exception(exc)
        assert isinstance(click_exc, click.ClickException)

    def test_converts_base_geoparquet_error(self):
        exc = GeoParquetError("Generic error")
        click_exc = handle_core_exception(exc)
        assert isinstance(click_exc, click.ClickException)
        assert "Generic error" in click_exc.message

    def test_decorator_catches_and_converts_exceptions(self):
        @core_exception_handler
        def failing_function():
            raise FileNotFoundGeoParquetError("missing.parquet")

        with pytest.raises(click.ClickException) as exc_info:
            failing_function()
        assert "missing.parquet" in str(exc_info.value)

    def test_decorator_passes_through_non_geoparquet_exceptions(self):
        @core_exception_handler
        def raises_value_error():
            raise ValueError("not a GeoParquet error")

        with pytest.raises(ValueError):
            raises_value_error()

    def test_decorator_preserves_return_value(self):
        @core_exception_handler
        def returns_value():
            return "success"

        assert returns_value() == "success"

    def test_decorator_preserves_function_metadata(self):
        @core_exception_handler
        def documented_function():
            """This is the docstring."""
            pass

        assert documented_function.__name__ == "documented_function"
        assert documented_function.__doc__ == """This is the docstring."""
