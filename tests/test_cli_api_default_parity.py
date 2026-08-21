"""CLI and Python API must agree on default values for shared parameters.

Three defaults had silently diverged between the CLI and the Python API,
producing different behavior for the "same" operation depending on which
interface was used:

- ``gpio add admin-divisions --dataset`` (CLI, default "gaul") vs.
  ``ops.add_admin_divisions`` / ``Table.add_admin_divisions`` (default
  "overture") -- different boundary datasets, materially different joins.
- ``gpio partition <scheme> --hive`` (CLI, off by default) vs.
  ``Table.partition_by_*`` (``hive=True`` by default) -- different output
  directory layout.
- ``gpio extract wfs --page-size`` (CLI, 100000) vs. ``Table.from_wfs``
  (``page_size=10000``) -- a silent 10x divergence between the two Python
  API wrappers of the same core function.

This module asserts the CLI option default and the API function/method
default are identical for each of these parameters. No network access is
needed -- these are pure introspection checks on Click command definitions
and Python function signatures.
"""

from __future__ import annotations

import inspect

from geoparquet_io.api import ops
from geoparquet_io.api.table import Table
from geoparquet_io.cli.main import cli
from geoparquet_io.core.wfs import DEFAULT_WFS_PAGE_SIZE


def _cli_option_default(command, opt_name):
    """Return the Click default for an option like '--dataset' on a command."""
    for param in command.params:
        if opt_name in param.opts:
            return param.default
    raise AssertionError(f"{opt_name!r} not found on command {command.name!r}")


def _param_default(func, param_name):
    """Return the default value of a keyword parameter via inspect.signature."""
    sig = inspect.signature(func)
    param = sig.parameters[param_name]
    assert param.default is not inspect.Parameter.empty, (
        f"{func!r} has no default for {param_name!r}"
    )
    return param.default


class TestAdminDivisionsDatasetParity:
    """gpio add admin-divisions --dataset vs. API add_admin_divisions(dataset=...)."""

    def test_cli_default_is_gaul(self):
        cmd = cli.commands["add"].commands["admin-divisions"]
        assert _cli_option_default(cmd, "--dataset") == "gaul"

    def test_ops_add_admin_divisions_matches_cli(self):
        cli_default = _cli_option_default(
            cli.commands["add"].commands["admin-divisions"], "--dataset"
        )
        assert _param_default(ops.add_admin_divisions, "dataset") == cli_default

    def test_table_add_admin_divisions_matches_cli(self):
        cli_default = _cli_option_default(
            cli.commands["add"].commands["admin-divisions"], "--dataset"
        )
        assert _param_default(Table.add_admin_divisions, "dataset") == cli_default


class TestPartitionHiveParity:
    """gpio partition <scheme> --hive vs. API Table.partition_by_*(hive=...)."""

    SCHEME_TO_METHOD = {
        "h3": "partition_by_h3",
        "quadkey": "partition_by_quadkey",
        "s2": "partition_by_s2",
        "a5": "partition_by_a5",
        "kdtree": "partition_by_kdtree",
        "string": "partition_by_string",
        "admin": "partition_by_admin",
    }

    def test_cli_hive_defaults_false_for_all_schemes(self):
        partition_group = cli.commands["partition"]
        for scheme in self.SCHEME_TO_METHOD:
            cmd = partition_group.commands[scheme]
            assert _cli_option_default(cmd, "--hive") is False, (
                f"partition {scheme} --hive default changed from False"
            )

    def test_table_partition_methods_match_cli_hive_default(self):
        partition_group = cli.commands["partition"]
        for scheme, method_name in self.SCHEME_TO_METHOD.items():
            cli_default = _cli_option_default(partition_group.commands[scheme], "--hive")
            method = getattr(Table, method_name)
            assert _param_default(method, "hive") == cli_default, (
                f"Table.{method_name}(hive=...) default diverges from "
                f"`gpio partition {scheme} --hive` default"
            )


class TestWfsPageSizeParity:
    """gpio extract wfs --page-size vs. ops.from_wfs / Table.from_wfs page_size=..."""

    def test_cli_default_matches_core_default(self):
        cmd = cli.commands["extract"].commands["wfs"]
        assert _cli_option_default(cmd, "--page-size") == DEFAULT_WFS_PAGE_SIZE

    def test_ops_from_wfs_matches_core_default(self):
        assert _param_default(ops.from_wfs, "page_size") == DEFAULT_WFS_PAGE_SIZE

    def test_table_from_wfs_matches_core_default(self):
        assert _param_default(Table.from_wfs, "page_size") == DEFAULT_WFS_PAGE_SIZE

    def test_ops_and_table_from_wfs_agree(self):
        assert _param_default(ops.from_wfs, "page_size") == _param_default(
            Table.from_wfs, "page_size"
        )
