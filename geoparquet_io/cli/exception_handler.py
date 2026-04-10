"""
Convert core exceptions to click exceptions at CLI boundary.

This module bridges the gap between framework-agnostic core exceptions
and click-specific exceptions needed for proper CLI error display.
"""

from __future__ import annotations

import functools
from collections.abc import Callable
from typing import TypeVar

import click

from geoparquet_io.core.exceptions import (
    FileNotFoundGeoParquetError,
    GeometryError,
    GeoParquetError,
    InvalidParameterError,
    PartitionError,
    RemoteAccessError,
    ValidationError,
)

F = TypeVar("F", bound=Callable)


def handle_core_exception(exc: GeoParquetError) -> click.ClickException:
    """Convert a core exception to the appropriate click exception."""
    if isinstance(exc, InvalidParameterError):
        return click.BadParameter(exc.message, param_hint=exc.param_name)
    elif isinstance(exc, FileNotFoundGeoParquetError):
        return click.ClickException(exc.message)
    elif isinstance(exc, RemoteAccessError):
        return click.ClickException(exc.message)
    elif isinstance(exc, GeometryError):
        return click.ClickException(exc.message)
    elif isinstance(exc, PartitionError):
        return click.ClickException(exc.message)
    elif isinstance(exc, ValidationError):
        return click.ClickException(exc.message)
    else:
        # Generic fallback for any GeoParquetError subclass
        return click.ClickException(exc.message)


def core_exception_handler(func: F) -> F:
    """
    Decorator to catch core exceptions and convert to click exceptions.

    Use this decorator on CLI command functions to automatically convert
    framework-agnostic core exceptions to click exceptions.

    Note:
        This decorator is provided for external integrations and custom Click
        commands that use geoparquet-io core functions. The main CLI commands
        handle exceptions through Click's built-in error handling, but this
        decorator enables consistent error display for third-party extensions.

    Example:
        @click.command()
        @core_exception_handler
        def my_command():
            # If this raises InvalidParameterError, it becomes click.BadParameter
            do_something_that_might_fail()
    """

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except GeoParquetError as e:
            raise handle_core_exception(e) from e

    return wrapper  # type: ignore
