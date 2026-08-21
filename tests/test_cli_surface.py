"""Characterization snapshot of the ``gpio`` CLI surface.

This suite freezes the *structure* of the Click command tree - groups,
commands, parameter names, types, defaults and flags - not the help prose.
Help wording may be reflowed freely; adding, removing or re-typing an option
is a user-visible change and must be acknowledged explicitly.

To accept an intentional change::

    GPIO_UPDATE_SNAPSHOT=1 uv run pytest tests/test_cli_surface.py

then review the diff of ``tests/data/cli_surface.json`` before committing.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import click
import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli

SNAPSHOT_PATH = Path(__file__).parent / "data" / "cli_surface.json"
UPDATE_ENV_VAR = "GPIO_UPDATE_SNAPSHOT"

# Commands contributed by third-party plugins (loaded through the
# ``gpio.plugins`` entry point group) are excluded from the snapshot so an
# installed plugin cannot break this test.
BUILTIN_PACKAGE = "geoparquet_io"


def _origin_module(command: click.Command) -> str:
    """Return the module a command was defined in (for plugin filtering)."""
    callback = getattr(command, "callback", None)
    if callback is not None:
        return getattr(callback, "__module__", "") or ""
    return type(command).__module__ or ""


def _is_builtin(command: click.Command) -> bool:
    module = _origin_module(command)
    return module == BUILTIN_PACKAGE or module.startswith(f"{BUILTIN_PACKAGE}.")


def _type_repr(param_type: click.ParamType) -> str:
    """Render a param type as a stable, machine-independent string.

    ``str(click.Path(...))`` embeds the object's memory address, so types are
    described structurally instead of by their default repr.
    """
    if isinstance(param_type, click.Choice):
        return "choice[" + "|".join(str(choice) for choice in param_type.choices) + "]"
    if isinstance(param_type, click.Path):
        return (
            "path["
            f"exists={param_type.exists},"
            f"file_okay={param_type.file_okay},"
            f"dir_okay={param_type.dir_okay},"
            f"writable={param_type.writable},"
            f"readable={param_type.readable},"
            f"resolve_path={param_type.resolve_path}"
            "]"
        )
    if isinstance(param_type, (click.IntRange, click.FloatRange)):
        return (
            f"{param_type.name}["
            f"min={param_type.min},"
            f"max={param_type.max},"
            f"min_open={param_type.min_open},"
            f"max_open={param_type.max_open},"
            f"clamp={param_type.clamp}"
            "]"
        )
    if isinstance(param_type, click.Tuple):
        return "tuple[" + ",".join(_type_repr(inner) for inner in param_type.types) + "]"
    return param_type.name


def _default_repr(param: click.Parameter) -> str:
    """Return ``repr`` of a parameter default, invoking callable defaults."""
    default = param.default
    if callable(default):
        default = default()
    return repr(default)


def describe_param(param: click.Parameter) -> dict:
    """Describe one Click parameter as a JSON-serializable dict."""
    return {
        "name": param.name,
        "param_type": type(param).__name__,
        "opts": sorted(param.opts) + sorted(param.secondary_opts),
        "type": _type_repr(param.type),
        "required": bool(param.required),
        "default": _default_repr(param),
        "is_flag": bool(getattr(param, "is_flag", False)),
        "multiple": bool(getattr(param, "multiple", False)),
        "nargs": param.nargs,
    }


def describe_command(command: click.Command) -> dict:
    """Describe a command (or group, recursively) as a JSON-serializable dict."""
    described: dict = {
        "kind": "group" if isinstance(command, click.Group) else "command",
        "class": type(command).__name__,
        "hidden": bool(getattr(command, "hidden", False)),
        "params": sorted(
            (describe_param(param) for param in command.params),
            key=lambda entry: entry["name"] or "",
        ),
    }
    if isinstance(command, click.Group):
        described["commands"] = {
            name: describe_command(sub)
            for name, sub in sorted(command.commands.items())
            if _is_builtin(sub)
        }
    return described


def build_surface() -> dict:
    """Walk the built-in ``gpio`` command tree into a JSON-serializable dict."""
    return describe_command(cli)


def iter_leaf_paths(described: dict, prefix: tuple[str, ...] = ()) -> list[tuple[str, ...]]:
    """Yield the argv path of every non-group command in a described tree."""
    if described["kind"] != "group":
        return [prefix]
    leaves: list[tuple[str, ...]] = []
    for name, sub in described["commands"].items():
        leaves.extend(iter_leaf_paths(sub, prefix + (name,)))
    return leaves


def _serialize(surface: dict) -> str:
    return json.dumps(surface, indent=1, sort_keys=True) + "\n"


def _diff_paths(expected, actual, prefix: str = "") -> list[str]:
    """Return human-readable descriptions of every difference between two trees."""
    where = prefix or "<root>"
    if type(expected) is not type(actual):
        return [f"{where}: type changed {type(expected).__name__} -> {type(actual).__name__}"]
    if isinstance(expected, dict):
        diffs = []
        for key in sorted(set(expected) - set(actual)):
            diffs.append(f"{prefix}/{key}: removed (was {expected[key]!r})")
        for key in sorted(set(actual) - set(expected)):
            diffs.append(f"{prefix}/{key}: added ({actual[key]!r})")
        for key in sorted(set(expected) & set(actual)):
            diffs.extend(_diff_paths(expected[key], actual[key], f"{prefix}/{key}"))
        return diffs
    if isinstance(expected, list):
        # Params are keyed by name so a reorder is not reported as a change.
        if all(isinstance(item, dict) and "name" in item for item in expected + actual):
            keyed_expected = {item["name"]: item for item in expected}
            keyed_actual = {item["name"]: item for item in actual}
            return _diff_paths(keyed_expected, keyed_actual, prefix)
        if expected != actual:
            return [f"{where}: {expected!r} != {actual!r}"]
        return []
    if expected != actual:
        return [f"{where}: expected {expected!r}, got {actual!r}"]
    return []


def test_cli_surface_matches_snapshot():
    """The Click command tree matches the committed structural snapshot."""
    surface = build_surface()

    if os.environ.get(UPDATE_ENV_VAR):
        SNAPSHOT_PATH.write_text(_serialize(surface), encoding="utf-8")
        pytest.skip(f"{UPDATE_ENV_VAR} set: refreshed {SNAPSHOT_PATH.name}")

    assert SNAPSHOT_PATH.exists(), (
        f"Missing CLI surface snapshot {SNAPSHOT_PATH}. "
        f"Create it with {UPDATE_ENV_VAR}=1 uv run pytest {Path(__file__).name}"
    )
    expected = json.loads(SNAPSHOT_PATH.read_text(encoding="utf-8"))

    diffs = _diff_paths(expected, surface)
    assert not diffs, (
        "CLI surface drifted from tests/data/cli_surface.json:\n"
        + "\n".join(f"  - {line}" for line in diffs[:40])
        + (f"\n  ... and {len(diffs) - 40} more" if len(diffs) > 40 else "")
        + f"\n\nIf the change is intentional, re-record the snapshot with:\n"
        f"  {UPDATE_ENV_VAR}=1 uv run pytest tests/{Path(__file__).name}\n"
        "and review the resulting diff before committing."
    )


LEAF_PATHS = iter_leaf_paths(build_surface())


@pytest.mark.parametrize(
    "argv",
    LEAF_PATHS,
    ids=["/".join(path) for path in LEAF_PATHS],
)
def test_leaf_command_help_renders(argv):
    """``--help`` renders successfully for every built-in leaf command."""
    result = CliRunner().invoke(cli, [*argv, "--help"])
    assert result.exit_code == 0, (
        f"gpio {' '.join(argv)} --help exited {result.exit_code}\n{result.output}"
    )
    assert "Usage:" in result.output


def test_leaf_command_inventory_is_non_trivial():
    """Guard against the walker silently collecting nothing."""
    assert len(LEAF_PATHS) > 40
