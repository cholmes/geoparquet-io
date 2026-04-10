"""Tests for core exception classes."""

import pytest

from geoparquet_io.core.exceptions import (
    FileNotFoundGeoParquetError,
    GeometryError,
    GeoParquetError,
    InvalidParameterError,
    PartitionError,
    RemoteAccessError,
    ValidationError,
)


class TestCoreExceptions:
    """Test that core exceptions are framework-agnostic."""

    def test_base_exception_inherits_from_exception(self):
        """GeoParquetError should inherit from Exception, not click exceptions."""
        assert issubclass(GeoParquetError, Exception)
        # Should NOT inherit from click
        import click

        assert not issubclass(GeoParquetError, click.ClickException)

    def test_file_not_found_error(self):
        with pytest.raises(FileNotFoundGeoParquetError) as exc_info:
            raise FileNotFoundGeoParquetError("test.parquet")
        assert "test.parquet" in str(exc_info.value)

    def test_file_not_found_error_with_detail(self):
        with pytest.raises(FileNotFoundGeoParquetError) as exc_info:
            raise FileNotFoundGeoParquetError("test.parquet", "no read permission")
        assert "test.parquet" in str(exc_info.value)
        assert "no read permission" in str(exc_info.value)

    def test_invalid_parameter_error(self):
        with pytest.raises(InvalidParameterError) as exc_info:
            raise InvalidParameterError("resolution", "must be between 1-15")
        assert "resolution" in str(exc_info.value)
        assert "must be between 1-15" in str(exc_info.value)

    def test_remote_access_error(self):
        with pytest.raises(RemoteAccessError):
            raise RemoteAccessError("s3://bucket/file", "Access denied")

    def test_remote_access_error_sanitizes_url(self):
        """URLs with credentials should be sanitized."""
        exc = RemoteAccessError(
            "s3://bucket/path/to/file.parquet?AWSAccessKeyId=SECRET", "Access denied"
        )
        # Query string should be stripped
        assert "AWSAccessKeyId" not in exc.url
        assert "SECRET" not in exc.url
        # URL is truncated after 4 parts: s3://bucket/path/...
        assert exc.url.endswith("...")
        assert "bucket" in exc.url

    def test_geometry_error(self):
        with pytest.raises(GeometryError):
            raise GeometryError("No geometry column found")

    def test_partition_error(self):
        with pytest.raises(PartitionError):
            raise PartitionError("Invalid partition scheme")

    def test_validation_error(self):
        with pytest.raises(ValidationError):
            raise ValidationError("File does not conform to GeoParquet spec")

    def test_exception_hierarchy(self):
        """All specific exceptions should inherit from GeoParquetError."""
        assert issubclass(FileNotFoundGeoParquetError, GeoParquetError)
        assert issubclass(InvalidParameterError, GeoParquetError)
        assert issubclass(RemoteAccessError, GeoParquetError)
        assert issubclass(GeometryError, GeoParquetError)
        assert issubclass(PartitionError, GeoParquetError)
        assert issubclass(ValidationError, GeoParquetError)

    def test_exception_message_attribute(self):
        """All exceptions should have a message attribute."""
        exc = GeoParquetError("test message")
        assert exc.message == "test message"

        exc2 = InvalidParameterError("param", "reason")
        assert "param" in exc2.message
        assert exc2.param_name == "param"
        assert exc2.reason == "reason"
