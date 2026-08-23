"""Keep the `integration` marker meaningful in CI (issue #667, deliverable 4).

The issue asks to "wire ``-m integration`` into CI so the marker is meaningful".
Investigation of the current workflow shows the marker is ALREADY selected: the
fast job runs ``-m "not slow and not network and not meta"``, which admits
integration-marked tests (that is how ``tests/test_geoparquet_corpus.py``
already runs on every PR). What was missing was not a lane but a guarantee --
nothing stopped a future selection from excluding it, and nothing tied each
journey to a lane.

So the smallest change that gives the marker real semantics is this guard plus
a sharper marker description in ``pyproject.toml``, NOT a new ``-m integration``
job: a separate job would repeat checkout + ``uv sync --all-extras`` + the
DuckDB extension install + tippecanoe (several minutes of runner time) only to
re-run tests the fast job already runs, and the brief says not to restructure
CI. These tests fail loudly if that reasoning stops holding.

Pattern follows ``tests/test_coverage_job.py``, which guards the coverage leg of
the same workflow the same way. Deliberately NOT marked ``meta``: it parses a
file and imports a module (no subprocesses), and it must fail on the PR that
breaks the wiring, not in the nightly lane.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from tests.e2e import test_journeys

PROJECT_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = PROJECT_ROOT / ".github" / "workflows" / "tests.yml"
PYPROJECT = PROJECT_ROOT / "pyproject.toml"

LANE_MARKERS = {"slow", "network", "meta", "integration", "corpus"}

# `-m "a and not b"` or a bare `-m network`.
_SELECTION = re.compile(r"-m\s+(?:\"([^\"]+)\"|(?!-)(\S+))")


def _selections() -> list[str]:
    """Distinct pytest marker selections used by tests.yml.

    Deduplicated: each job repeats its selection once per OS branch (xdist
    flags differ), and those repeats are the same lane.
    """
    text = WORKFLOW.read_text(encoding="utf-8")
    found = list(dict.fromkeys(quoted or bare for quoted, bare in _SELECTION.findall(text)))
    assert found, "tests.yml has no `-m` marker selections at all"
    return found


def selects(expression: str, marks: set[str]) -> bool:
    """Evaluate a pytest ``-m`` expression against a set of marker names.

    ``eval`` is safe here: the expression comes from this repo's own workflow
    file and every name is bound to a bool in an empty builtins namespace.
    """
    names = set(re.findall(r"[A-Za-z_][A-Za-z_0-9]*", expression)) - {"and", "or", "not"}
    return bool(eval(expression, {"__builtins__": {}}, {name: name in marks for name in names}))


def _marks_of(func) -> set[str]:
    marks = {mark.name for mark in getattr(test_journeys, "pytestmark", [])}
    marks |= {mark.name for mark in getattr(func, "pytestmark", [])}
    return marks & LANE_MARKERS


def _journeys() -> dict[str, set[str]]:
    return {
        name: _marks_of(getattr(test_journeys, name))
        for name in dir(test_journeys)
        if name.startswith("test_")
    }


def test_selection_evaluator_is_not_vacuous():
    """The evaluator this module reasons with must actually discriminate."""
    assert selects("not slow and not network and not meta", {"integration"})
    assert not selects("not slow and not network and not meta", {"integration", "slow"})
    assert selects("(slow or meta) and not network", {"slow"})
    assert not selects("(slow or meta) and not network", {"slow", "network"})
    assert selects("network", {"network"})


def test_every_journey_carries_the_integration_marker():
    journeys = _journeys()
    assert len(journeys) >= 10, f"expected the ten journeys plus guards, got {journeys}"
    for name, marks in journeys.items():
        assert "integration" in marks, f"{name} is not integration-marked"


def test_no_ci_selection_excludes_integration_tests():
    """No job may deselect the marker; otherwise the journeys run nowhere."""
    for expression in _selections():
        assert "integration" not in expression, (
            f"tests.yml selection {expression!r} mentions `integration`. The journeys are "
            "meant to ride the existing lanes; naming the marker here (especially as "
            "`not integration`) would silently strand them."
        )


def test_each_journey_lands_in_exactly_one_ci_job():
    """Every journey runs in exactly one of the fast / slow / network jobs."""
    selections = _selections()
    for name, marks in _journeys().items():
        matched = [expression for expression in selections if selects(expression, marks)]
        assert len(matched) == 1, (
            f"{name} (marks={sorted(marks)}) is selected by {len(matched)} CI jobs: {matched}"
        )


def test_fast_lane_runs_the_fast_journeys():
    """Journeys 1-7 must run on every PR, i.e. under the fast-suite selection."""
    fast = [e for e in _selections() if selects(e, {"integration"})]
    assert fast, "no CI selection admits plain integration-marked tests"
    fast_journeys = {name for name, marks in _journeys().items() if not marks & {"slow", "network"}}
    assert len(fast_journeys) >= 7, f"expected journeys 1-7 in the fast lane, got {fast_journeys}"


def test_marker_description_documents_the_lane():
    """`--strict-markers` is on; the description is where the lane is documented."""
    description = None
    for line in PYPROJECT.read_text(encoding="utf-8").splitlines():
        if line.strip().startswith('"integration:'):
            description = line
            break
    assert description is not None, "pyproject.toml lost its `integration` marker"
    assert "fast suite" in description, (
        "the `integration` marker description must say which lane it rides, "
        f"got: {description.strip()}"
    )


@pytest.mark.parametrize("path", [WORKFLOW, PYPROJECT])
def test_guarded_files_exist(path: Path):
    assert path.exists(), f"{path} is missing; this guard would silently pass"
