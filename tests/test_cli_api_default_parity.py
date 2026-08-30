"""Drift guard: the CLI and the Python API must not grow new default mismatches.

`gpio <group> <cmd>` and its `geoparquet_io.api` twin (`ops.<fn>` /
`Table.<method>`) are advertised as the same operation through two front doors.
When a Click option default and the matching Python parameter default disagree,
the "same" call silently does two different things -- different boundary
datasets, different row limits, different output schemas.

Rather than pinning a handful of parameters by hand (which only ever proves
that the parameters somebody already fixed are still fixed), this module walks
*every* Click command, resolves its API twin, and diffs the two default sets.
`collect_divergences()` returns the current mismatches; the test asserts they
are a subset of `KNOWN_DIVERGENCES`, an explicit allowlist where every entry
carries a justification. Any newly introduced mismatch fails the suite.

A command with no API twin at all is a different failure of the same rule, so
`TestEveryCommandHasAnApiTwin` pins the twin-less commands to the `NO_API_TWIN`
allowlist -- the `check-api-for-cli` pre-commit hook only prints a reminder.

This module compares *declared* defaults. Its sibling,
`tests/test_cli_api_call_parity_scaffold.py`, compares the values each front end
actually hands to core for a sample of commands, and catches what introspection
cannot (a CLI deriving one option from another, or a parameter the API omits
entirely so the core default silently applies). Several entries in
`KNOWN_DIVERGENCES` have a counterpart in that module's `KNOWN_PARITY_GAPS`;
closing a gap means deleting from both.

No network access is required -- this is pure introspection over Click command
definitions and Python function signatures.
"""

from __future__ import annotations

import inspect

import click
import pytest

from geoparquet_io.api import ops
from geoparquet_io.api.table import Table
from geoparquet_io.cli.main import cli
from geoparquet_io.core.wfs import DEFAULT_WFS_PAGE_SIZE

# click >= 8.2 marks "no default given" with a sentinel object; the shim that
# keeps this working on click 8.1 lives in conftest so both introspection
# suites share one copy.
from tests.conftest import UNSET
from tests.conftest import walk_cli_commands as _walk

# --------------------------------------------------------------------------
# CLI command -> API twin resolution
# --------------------------------------------------------------------------

# Hand-written mappings for twins whose names cannot be derived mechanically.
NAME_OVERRIDES: dict[tuple[str, ...], list[str]] = {
    ("extract", "wfs"): ["from_wfs", "from_wfs_layers"],
    ("extract", "arcgis"): ["from_arcgis"],
    ("extract", "carto"): ["from_carto"],
    ("extract", "bigquery"): ["read_bigquery", "from_bigquery"],
    ("extract", "geoparquet"): ["extract"],
    ("pmtiles", "create"): ["create_pmtiles"],
    ("pmtiles", "pyramid"): ["create_pmtiles_pyramid"],
    ("process", "overview"): ["create_overviews", "overview"],
    ("publish", "upload"): ["upload"],
    ("inspect", "meta"): ["metadata"],
    ("benchmark", "explain"): ["explain_analyze"],
    ("convert", "reproject"): ["reproject"],
    ("check", "all"): ["check"],
    ("check", "spec"): ["validate"],
    ("check", "row-group"): ["check_row_groups"],
    ("check", "optimization"): ["check_optimization"],
    ("inspect", "summary"): ["info"],
    ("convert", "geoparquet"): ["write"],
}

# CLI-only plumbing (I/O paths, output formatting) with no API analogue.
IGNORE_PARAMS = frozenset(
    {
        "help",
        "verbose",
        "quiet",
        "output",
        "input_file",
        "output_file",
        "dry_run",
        "show_sql",
        "json",
        "json_output",
        "yes",
        "input",
        "url",
        "no_color",
    }
)


def _candidate_names(path: tuple[str, ...]) -> list[str]:
    """Plausible API attribute names for a CLI command path."""
    parts = [p.replace("-", "_") for p in path]
    group = parts[0] if len(parts) > 1 else ""
    name = parts[-1]
    out = [
        *NAME_OVERRIDES.get(path, []),
        "_".join(parts),
        name,
        f"{group}_by_{name}" if group else "",
        f"convert_to_{name}",
        f"to_{name}",
        f"{name}_{group}" if group else "",
        f"check_{name}",
        "_".join(parts[1:]),
        "_".join(parts[-2:]),
    ]
    return [c for c in out if c]


def _api_twins(path: tuple[str, ...]) -> list[tuple[str, object]]:
    """Return ``[(label, callable)]`` for the ops function / Table method twins."""
    found: list[tuple[str, object]] = []
    seen: set[tuple[str, str]] = set()
    for cand in _candidate_names(path):
        for container_name, container in (("ops", ops), ("Table", Table)):
            key = (container_name, cand)
            if key in seen:
                continue
            fn = getattr(container, cand, None)
            if callable(fn):
                seen.add(key)
                found.append((f"{container_name}.{cand}", fn))
    return found


def _normalize(value):
    """Collapse the many spellings of "not specified" to a single value.

    click >= 8.4 stores ``UNSET`` for options declared without ``default=``; at
    runtime those resolve to ``None`` (or ``()`` for ``multiple=True``). The API
    spells the same thing as ``None``. Treat them all as ``"<unset>"`` so a
    representation difference is never reported as a behaviour difference.
    """
    if value is UNSET or value is None:
        return "<unset>"
    if isinstance(value, (tuple, list)) and not value:
        return "<unset>"
    return value


def _same_default(cli_value, api_value) -> bool:
    """True when two normalized defaults mean the same thing at runtime.

    Equality alone is too loose (``False == 0``) and identity of type too
    strict (the CLI declares ``--timeout`` as an int while the API annotates
    ``float``). So: equal values, and a bool on one side must be a bool on the
    other.
    """
    if isinstance(cli_value, bool) != isinstance(api_value, bool):
        return False
    return cli_value == api_value


def _cli_defaults(cmd) -> dict:
    return {
        p.name: p.default
        for p in cmd.params
        if not isinstance(p, click.Argument) and p.name not in IGNORE_PARAMS
    }


def _api_defaults(fn) -> dict:
    try:
        sig = inspect.signature(fn)
    except (ValueError, TypeError):  # pragma: no cover - builtins
        return {}
    return {
        name: param.default
        for name, param in sig.parameters.items()
        if name not in ("self", "cls") and param.default is not inspect.Parameter.empty
    }


def collect_divergences() -> list[tuple[str, str, str, str, str]]:
    """Diff every Click option default against its Python API twin's default.

    Returns:
        Sorted list of ``(command, api, param, cli_default, api_default)``
        tuples, one per mismatch. Defaults are rendered with ``repr()`` after
        normalization so the tuples are stable, comparable and readable in an
        assertion message.
    """
    divergences = []
    for path, cmd in _walk(cli):
        twins = _api_twins(path)
        if not twins:
            # Enforced separately by `TestEveryCommandHasAnApiTwin`; a command with
            # no twin has nothing to diff, but it is not silently acceptable.
            continue
        cli_defaults = _cli_defaults(cmd)
        for label, fn in twins:
            api_defaults = _api_defaults(fn)
            for pname, cli_value in cli_defaults.items():
                if pname not in api_defaults:
                    continue
                cli_norm = _normalize(cli_value)
                api_norm = _normalize(api_defaults[pname])
                if _same_default(cli_norm, api_norm):
                    continue
                divergences.append((" ".join(path), label, pname, repr(cli_norm), repr(api_norm)))
    return sorted(divergences)


# --------------------------------------------------------------------------
# Allowlist -- every entry needs a reason
# --------------------------------------------------------------------------

_AUTO_RESOLUTION = (
    "CLI unset = auto-calculate the resolution from the file on disk; the API is handed an "
    "in-memory table and falls back to the documented default"
)
_KEEP_TRISTATE = (
    "CLI flag False and API None both mean 'follow hive'; the API adds an explicit-drop option "
    "the CLI flag cannot express"
)

KNOWN_DIVERGENCES: dict[tuple[str, str, str, str, str], str] = {
    ("partition a5", "Table.partition_by_a5", "resolution", "'<unset>'", "15"): _AUTO_RESOLUTION,
    ("partition h3", "Table.partition_by_h3", "resolution", "'<unset>'", "9"): _AUTO_RESOLUTION,
    (
        "partition quadkey",
        "Table.partition_by_quadkey",
        "partition_resolution",
        "'<unset>'",
        "6",
    ): _AUTO_RESOLUTION,
    (
        "partition quadkey",
        "Table.partition_by_quadkey",
        "resolution",
        "'<unset>'",
        "13",
    ): _AUTO_RESOLUTION,
    ("partition s2", "Table.partition_by_s2", "level", "'<unset>'", "13"): _AUTO_RESOLUTION,
    (
        "sort hilbert",
        "ops.sort_hilbert",
        "geometry_column",
        "'geometry'",
        "'<unset>'",
    ): (
        "CLI reads a file and names the conventional column; the API holds a Table that already "
        "tracks its geometry column, so None means 'use that one'"
    ),
    (
        "sort str",
        "ops.sort_str",
        "geometry_column",
        "'geometry'",
        "'<unset>'",
    ): (
        "CLI reads a file and names the conventional column; the API holds a Table that already "
        "tracks its geometry column, so None means 'use that one'"
    ),
    (
        "partition a5",
        "Table.partition_by_a5",
        "keep_a5_column",
        "False",
        "'<unset>'",
    ): _KEEP_TRISTATE,
    (
        "partition h3",
        "Table.partition_by_h3",
        "keep_h3_column",
        "False",
        "'<unset>'",
    ): _KEEP_TRISTATE,
    (
        "partition kdtree",
        "Table.partition_by_kdtree",
        "keep_kdtree_column",
        "False",
        "'<unset>'",
    ): _KEEP_TRISTATE,
    (
        "partition quadkey",
        "Table.partition_by_quadkey",
        "keep_quadkey_column",
        "False",
        "'<unset>'",
    ): _KEEP_TRISTATE,
    (
        "partition s2",
        "Table.partition_by_s2",
        "keep_s2_column",
        "False",
        "'<unset>'",
    ): _KEEP_TRISTATE,
}


# --------------------------------------------------------------------------
# "Every CLI command needs a Python API" -- allowlisted exceptions
# --------------------------------------------------------------------------

# CLAUDE.md states the rule; the `check-api-for-cli` pre-commit hook only prints a
# reminder and never fails. These are the commands that ship without an API twin
# today. Adding a CLI command without an API now fails here until someone either
# writes the API or consciously adds a line to this dict.
NO_API_TWIN: dict[str, str] = {
    "benchmark compare": (
        "Benchmarking is a CLI reporting workflow, not a data operation: `compare` "
        "diffs two benchmark runs and renders a table for a human. There is no "
        "GeoParquet input/output to hang a Table method or ops function off."
    ),
    "benchmark report": (
        "Renders previously collected benchmark results as a human-readable report. "
        "Same reason as `benchmark compare` -- presentation, not a table operation."
    ),
    "benchmark suite": (
        "Orchestrates a multi-command benchmark run and prints timings. An API caller "
        "would compose the individual operations directly instead."
    ),
    "check stac": (
        "Validates a STAC catalog/item document rather than a GeoParquet table, so it "
        "has no `Table` receiver. Worth an `ops.check_stac` eventually."
    ),
    "publish stac": (
        "Writes STAC metadata for a dataset or directory of datasets; the unit of work "
        "is a collection on disk, not the in-memory table a `Table` method operates on. "
        "Worth an `ops.publish_stac` eventually."
    ),
    "inspect layers": (
        "Lists the layers of a multi-layer source (GeoPackage, FlatGeobuf) *before* a "
        "single-layer Table can exist, so it cannot be a Table method. Worth an "
        "`ops.list_layers` eventually."
    ),
    "skills": (
        "Lists and prints the bundled LLM skill documents. A CLI affordance with no "
        "data operation behind it."
    ),
}


def commands_without_api_twin() -> set[str]:
    """Every CLI command path that resolves to no `ops` function and no `Table` method."""
    return {" ".join(path) for path, _cmd in _walk(cli) if not _api_twins(path)}


class TestEveryCommandHasAnApiTwin:
    """The project rule is "every CLI command needs a Python API"; enforce it."""

    def test_twin_less_commands_are_exactly_the_allowlist(self):
        actual = commands_without_api_twin()
        missing = actual - set(NO_API_TWIN)
        stale = set(NO_API_TWIN) - actual
        assert not missing, (
            "CLI command(s) with no `ops` function and no `Table` method. CLAUDE.md "
            "requires a Python API for every CLI command -- add one, or add an entry "
            "to NO_API_TWIN with a justification:\n" + "\n".join(f"  {c}" for c in sorted(missing))
        )
        assert not stale, (
            "NO_API_TWIN lists command(s) that now have a Python API (or no longer "
            "exist); delete the entries:\n" + "\n".join(f"  {c}" for c in sorted(stale))
        )

    def test_every_allowlist_entry_has_a_reason(self):
        for command, reason in NO_API_TWIN.items():
            assert reason and reason.strip(), f"No justification recorded for {command!r}"


class TestNoNewDefaultDrift:
    """The set of CLI/API default mismatches must never grow."""

    def test_no_unlisted_divergences(self):
        unexpected = set(collect_divergences()) - set(KNOWN_DIVERGENCES)
        assert not unexpected, (
            "New CLI/Python API default mismatch(es) introduced.\n"
            "Either align the defaults, or add an entry to KNOWN_DIVERGENCES "
            "with a one-line justification:\n" + "\n".join(f"  {d}" for d in sorted(unexpected))
        )

    def test_allowlist_has_no_stale_entries(self):
        stale = set(KNOWN_DIVERGENCES) - set(collect_divergences())
        assert not stale, (
            "KNOWN_DIVERGENCES entries no longer describe a real mismatch; delete them:\n"
            + "\n".join(f"  {d}" for d in sorted(stale))
        )

    def test_every_allowlist_entry_has_a_reason(self):
        for entry, reason in KNOWN_DIVERGENCES.items():
            assert reason and reason.strip(), f"No justification recorded for {entry}"


def _cli_option_default(command, opt_name):
    """Return the normalized Click default for an option like '--dataset'."""
    for param in command.params:
        if opt_name in param.opts:
            return _normalize(param.default)
    raise AssertionError(f"{opt_name!r} not found on command {command.name!r}")


def _param_default(func, param_name):
    """Return the normalized default of a keyword parameter."""
    param = inspect.signature(func).parameters[param_name]
    assert param.default is not inspect.Parameter.empty, (
        f"{func!r} has no default for {param_name!r}"
    )
    return _normalize(param.default)


class TestAdminDivisionsParity:
    """gpio add admin-divisions vs. add_admin_divisions()."""

    def test_cli_dataset_default_is_gaul(self):
        cmd = cli.commands["add"].commands["admin-divisions"]
        assert _cli_option_default(cmd, "--dataset") == "gaul"

    @pytest.mark.parametrize("fn", [ops.add_admin_divisions, Table.add_admin_divisions])
    def test_dataset_default_matches_cli(self, fn):
        cli_default = _cli_option_default(
            cli.commands["add"].commands["admin-divisions"], "--dataset"
        )
        assert _param_default(fn, "dataset") == cli_default

    def test_levels_helper_returns_every_level_of_the_dataset(self):
        """The CLI with no --levels adds all levels; the API must do the same."""
        from geoparquet_io.core.admin_datasets import default_admin_levels

        assert default_admin_levels("gaul") == ["continent", "country", "department"]
        assert default_admin_levels("overture") == ["country", "region"]

    @pytest.mark.parametrize("fn", [ops.add_admin_divisions, Table.add_admin_divisions])
    def test_prefix_is_exposed_so_column_names_can_be_pinned(self, fn):
        """The CLI has --prefix; without it the API cannot pin output column names."""
        assert "prefix" in inspect.signature(fn).parameters


class TestPartitionHiveParity:
    """gpio partition <scheme> --hive vs. Table.partition_by_*(hive=...)."""

    SCHEME_TO_METHOD = {
        "h3": "partition_by_h3",
        "quadkey": "partition_by_quadkey",
        "s2": "partition_by_s2",
        "a5": "partition_by_a5",
        "kdtree": "partition_by_kdtree",
        "string": "partition_by_string",
        "admin": "partition_by_admin",
    }

    # Schemes whose partition key is a column gpio generated itself, and which
    # therefore drop that column from the output when hive is off.
    SCHEME_TO_KEEP_PARAM = {
        "h3": "keep_h3_column",
        "quadkey": "keep_quadkey_column",
        "s2": "keep_s2_column",
        "a5": "keep_a5_column",
        "kdtree": "keep_kdtree_column",
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

    @pytest.mark.parametrize("scheme,keep_param", sorted(SCHEME_TO_KEEP_PARAM.items()))
    def test_api_exposes_the_keep_column_escape_hatch(self, scheme, keep_param):
        """hive=False drops the index column; the API needs the CLI's escape hatch."""
        method = getattr(Table, self.SCHEME_TO_METHOD[scheme])
        params = inspect.signature(method).parameters
        assert keep_param in params, (
            f"Table.{self.SCHEME_TO_METHOD[scheme]} has no {keep_param} parameter, so an "
            f"API caller cannot keep the {scheme} column when hive=False "
            f"(the CLI can, via --{keep_param.replace('_', '-')})"
        )
        assert params[keep_param].default is None, (
            f"{keep_param} must default to None ('follow hive'), not {params[keep_param].default!r}"
        )

    @pytest.mark.parametrize(
        "method_name",
        ["partition_by_string", "partition_by_kdtree", "partition_by_admin"],
    )
    def test_compression_level_is_codec_neutral(self, method_name):
        """A hardcoded 15 is out of range for GZIP/BROTLI and raises."""
        method = getattr(Table, method_name)
        assert inspect.signature(method).parameters["compression_level"].default is None, (
            f"Table.{method_name} pins compression_level, which breaks every "
            f"codec whose valid range excludes that value (e.g. GZIP is 1-9)"
        )


class TestCheckSpatialParity:
    """gpio check spatial --limit-rows vs. Table.check_spatial(limit_rows=...)."""

    def test_limit_rows_matches_cli(self):
        cmd = cli.commands["check"].commands["spatial"]
        assert _param_default(Table.check_spatial, "limit_rows") == _cli_option_default(
            cmd, "--limit-rows"
        )

    def test_check_all_and_check_spatial_agree_on_limit_rows(self):
        assert _cli_option_default(
            cli.commands["check"].commands["all"], "--limit-rows"
        ) == _cli_option_default(cli.commands["check"].commands["spatial"], "--limit-rows")


class TestWfsParity:
    """gpio extract wfs vs. ops.from_wfs / ops.from_wfs_layers / Table.from_wfs."""

    WFS_API_ENTRY_POINTS = [ops.from_wfs, ops.from_wfs_layers, Table.from_wfs]
    # The CLI calls convert_wfs_to_geoparquet / convert_wfs_layers_to_directory,
    # NOT wfs_to_table, so all three need the shared defaults.
    WFS_CORE_ENTRY_POINTS = [
        "wfs_to_table",
        "convert_wfs_to_geoparquet",
        "convert_wfs_layers_to_directory",
    ]
    WFS_CORE_PAGE_SIZE_HELPERS = ["fetch_all_features_duckdb", "_fetch_with_spatial_tiles"]

    def test_cli_page_size_default_matches_constant(self):
        cmd = cli.commands["extract"].commands["wfs"]
        assert _cli_option_default(cmd, "--page-size") == DEFAULT_WFS_PAGE_SIZE

    def test_cli_page_size_range_admits_the_constant(self):
        """The IntRange constrains what DEFAULT_WFS_PAGE_SIZE may become."""
        cmd = cli.commands["extract"].commands["wfs"]
        param = next(p for p in cmd.params if "--page-size" in p.opts)
        assert param.type.min <= DEFAULT_WFS_PAGE_SIZE <= param.type.max

    @pytest.mark.parametrize("fn", WFS_API_ENTRY_POINTS)
    def test_api_page_size_default_matches_constant(self, fn):
        assert _param_default(fn, "page_size") == DEFAULT_WFS_PAGE_SIZE

    @pytest.mark.parametrize("name", WFS_CORE_ENTRY_POINTS + WFS_CORE_PAGE_SIZE_HELPERS)
    def test_core_wfs_entry_points_use_the_constant(self, name):
        """Every core WFS entry point must reference the shared constant.

        The CLI calls ``convert_wfs_to_geoparquet``, not ``wfs_to_table``; a
        literal left behind in any of them re-opens the drift the constant was
        added to close.
        """
        from geoparquet_io.core import wfs as wfs_module

        assert _param_default(getattr(wfs_module, name), "page_size") == DEFAULT_WFS_PAGE_SIZE, (
            f"core.wfs.{name} does not use DEFAULT_WFS_PAGE_SIZE"
        )

    @pytest.mark.parametrize("fn", WFS_API_ENTRY_POINTS)
    def test_auto_tile_default_matches_cli(self, fn):
        """auto_tile=False silently truncates at a server's feature cap."""
        cmd = cli.commands["extract"].commands["wfs"]
        assert _param_default(fn, "auto_tile") == _cli_option_default(cmd, "--auto-tile"), (
            "auto_tile off by default means a capped server returns partial data "
            "and reports success"
        )

    @pytest.mark.parametrize("name", WFS_CORE_ENTRY_POINTS)
    def test_core_wfs_entry_points_default_to_auto_tile(self, name):
        from geoparquet_io.core import wfs as wfs_module

        assert _param_default(getattr(wfs_module, name), "auto_tile") is True, (
            f"core.wfs.{name} defaults auto_tile=False and will silently truncate"
        )
