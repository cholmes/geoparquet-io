"""Guard the honesty of the notebook CI signal.

The ``notebooks`` job in ``.github/workflows/tests.yml`` runs every
``examples/*.ipynb`` under nbmake and reports a green check. That check is only
meaningful if the notebooks actually execute something. Before this guard,
``05_cloud_workflows.ipynb`` had ten code cells of which ten were entirely
commented out -- nbmake executed zero statements and reported success anyway
(see issue #667).

Two invariants keep that from recurring silently:

1. A notebook that nbmake *runs* must execute at least one real call into the
   ``gpio`` API. A notebook with nothing to execute must instead be listed in
   ``examples/conftest.py::ILLUSTRATIVE_NOTEBOOKS`` so nbmake skips it and no
   green check is claimed for it.
2. A notebook whose code cells are mostly commented-out ("uncomment to run")
   stubs must say so in its own prose, via an HTML-comment directive that is
   invisible in the rendered notebook. Weak signal is allowed -- silent weak
   signal is not.

Directive convention (mirrors the docs-example directives in #667)::

    <!-- nbsignal: illustrative reason="..." -->

This is a static check: it parses notebook JSON and never executes a cell, so
it belongs in the fast suite where it guards every PR, rather than in the
``meta`` lane which only runs nightly.
"""

from __future__ import annotations

import ast
import json
import re
import subprocess
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).parent.parent
EXAMPLES_DIR = PROJECT_ROOT / "examples"

#: Fraction of code cells that may be inert before a notebook must declare itself
#: illustrative. Notebooks 01-03 sit near zero; 04 is deliberately above it.
MOSTLY_INERT_THRESHOLD = 0.5

#: The directive a notebook uses to declare that its examples are illustrative.
#: ``reason="..."`` is mandatory so the waiver explains itself to the next reader.
DIRECTIVE_RE = re.compile(
    r"<!--\s*nbsignal:\s*illustrative\s+reason=\"([^\"]+)\"\s*-->",
)

#: A cell counts as exercising the library if it calls into the ``gpio`` API.
GPIO_CALL_RE = re.compile(r"\bgpio\s*\.\s*\w+\s*\(|\bread\s*\(|\bops\s*\.\s*\w+\s*\(")


def _notebook_paths() -> list[Path]:
    return sorted(EXAMPLES_DIR.glob("*.ipynb"))


def _load_excluded() -> dict[str, str]:
    """Read the nbmake exclusion map that ``examples/conftest.py`` declares.

    Parsed with ``ast`` rather than imported: importing a conftest outside of
    pytest's own collection machinery is fragile, and a static read keeps this
    test honest about what the file literally says.
    """
    conftest = EXAMPLES_DIR / "conftest.py"
    assert conftest.exists(), (
        f"Expected {conftest} to declare which notebooks nbmake must skip. "
        "Without it, an inert notebook produces a false-positive green check."
    )
    tree = ast.parse(conftest.read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if not isinstance(node, ast.Assign):
            continue
        names = {t.id for t in node.targets if isinstance(t, ast.Name)}
        if "ILLUSTRATIVE_NOTEBOOKS" in names:
            return dict(ast.literal_eval(node.value))
    pytest.fail("examples/conftest.py does not define ILLUSTRATIVE_NOTEBOOKS")


def _cells(path: Path, kind: str) -> list[str]:
    nb = json.loads(path.read_text(encoding="utf-8"))
    return ["".join(cell["source"]) for cell in nb["cells"] if cell["cell_type"] == kind]


def _is_inert(source: str) -> bool:
    """True when a code cell has nothing for the kernel to run.

    Blank cells and cells that are entirely ``#`` comments (the "uncomment to
    run" pattern) execute no statements, so nbmake passing them proves nothing.
    """
    lines = [line for line in source.splitlines() if line.strip()]
    return not lines or all(line.lstrip().startswith("#") for line in lines)


def _directive_reason(path: Path) -> str | None:
    for source in _cells(path, "markdown"):
        match = DIRECTIVE_RE.search(source)
        if match:
            return match.group(1)
    return None


def test_notebooks_exist() -> None:
    """Sanity: the guard is pointed at real files."""
    assert _notebook_paths(), f"No notebooks found under {EXAMPLES_DIR}"


def test_exclusion_list_has_no_stale_entries() -> None:
    """Every excluded name must match a notebook that actually exists."""
    names = {path.name for path in _notebook_paths()}
    stale = set(_load_excluded()) - names
    assert not stale, (
        f"examples/conftest.py excludes notebooks that no longer exist: {sorted(stale)}"
    )


def test_every_exclusion_states_a_reason() -> None:
    """The skip reason is what a CI reader sees, so it has to be a real sentence."""
    for name, reason in _load_excluded().items():
        assert len(reason.strip()) >= 20, (
            f"{name}: skip reason is too terse to explain itself: {reason!r}"
        )


def test_conftest_skips_via_a_hook_not_collect_ignore() -> None:
    """The exclusion must survive the file-list invocation CI actually uses.

    ``collect_ignore`` is only consulted when pytest walks a directory. The CI
    job passes an expanded ``examples/*.ipynb`` file list, against which
    ``collect_ignore`` is silently bypassed and every notebook runs anyway --
    exactly the false positive this module exists to prevent. Requiring the
    ``pytest_collection_modifyitems`` hook keeps the mechanism invocation-proof.
    """
    source = (EXAMPLES_DIR / "conftest.py").read_text(encoding="utf-8")
    tree = ast.parse(source)
    hooks = {node.name for node in ast.walk(tree) if isinstance(node, ast.FunctionDef)}
    assert "pytest_collection_modifyitems" in hooks, (
        "examples/conftest.py must skip illustrative notebooks from a "
        "pytest_collection_modifyitems hook"
    )
    # Check for a real assignment, not the word appearing in the module docstring.
    assigned = {
        target.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign)
        for target in node.targets
        if isinstance(target, ast.Name)
    }
    assert "collect_ignore" not in assigned, (
        "collect_ignore does not work for the file-list invocation the CI "
        "notebooks job uses; it would give a false sense of exclusion"
    )


@pytest.mark.parametrize(
    "path", _notebook_paths(), ids=lambda p: p.name if isinstance(p, Path) else str(p)
)
def test_executed_notebook_runs_real_code(path: Path) -> None:
    """A notebook nbmake executes must actually call the library.

    This is the check that ``05_cloud_workflows.ipynb`` failed: nbmake reported
    it green while executing nothing at all.
    """
    if path.name in _load_excluded():
        pytest.skip(f"{path.name} is excluded from nbmake; covered by other tests here")

    live = [src for src in _cells(path, "code") if not _is_inert(src)]
    assert live, (
        f"{path.name} has no executable code cells, so its nbmake green check "
        "would be a false positive. Give it runnable content, or add it to "
        "ILLUSTRATIVE_NOTEBOOKS in examples/conftest.py."
    )
    assert any(GPIO_CALL_RE.search(src) for src in live), (
        f"{path.name} executes code but never calls the gpio API, so nbmake "
        "proves nothing about this project."
    )


@pytest.mark.meta
def test_illustrative_notebook_is_actually_skipped_by_nbmake() -> None:
    """Prove the skip fires under the real nbmake invocation, not just in theory.

    The static checks above assert the *shape* of the mechanism; this one runs
    it. Marked ``meta`` because it spawns a nested pytest, matching how the
    other subprocess-based tooling checks in this repo are laned.
    """
    excluded = sorted(_load_excluded())
    assert excluded, "nothing to prove: no notebooks are excluded"
    target = EXAMPLES_DIR / excluded[0]

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "--nbmake",
            str(target),
            "--no-cov",
            "-q",
            "-p",
            "no:randomly",
        ],
        capture_output=True,
        text=True,
        cwd=str(PROJECT_ROOT),
    )
    assert result.returncode == 0, f"nbmake run failed:\n{result.stdout}\n{result.stderr}"
    assert "1 skipped" in result.stdout, (
        f"{target.name} was executed by nbmake instead of skipped. Its green "
        f"check would be a false positive.\n{result.stdout}"
    )


@pytest.mark.parametrize(
    "path", _notebook_paths(), ids=lambda p: p.name if isinstance(p, Path) else str(p)
)
def test_mostly_inert_notebook_declares_itself(path: Path) -> None:
    """Weak or absent execution signal has to be visible in the notebook's prose.

    Applies to excluded notebooks (which run nothing) and to notebooks kept in
    the nbmake job whose cells are mostly commented-out stubs.
    """
    code = _cells(path, "code")
    inert = sum(1 for src in code if _is_inert(src))
    excluded = path.name in _load_excluded()
    mostly_inert = bool(code) and inert / len(code) >= MOSTLY_INERT_THRESHOLD

    reason = _directive_reason(path)
    if excluded or mostly_inert:
        assert reason, (
            f"{path.name} has {inert}/{len(code)} inert code cells"
            f"{' and is excluded from nbmake' if excluded else ''}, so it must "
            'carry a markdown directive: <!-- nbsignal: illustrative reason="..." -->'
        )
        assert len(reason.strip()) >= 20, (
            f"{path.name}: nbsignal reason is too terse to be useful: {reason!r}"
        )
    else:
        assert not reason, (
            f"{path.name} carries an nbsignal directive but only {inert}/{len(code)} "
            "of its code cells are inert. Drop the stale directive."
        )
