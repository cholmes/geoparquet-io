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
import importlib.util
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

#: The repo's convention for an install cell, which ships commented out on
#: purpose so nbmake does not pip-install anything. It is idiomatic rather than
#: dead code, so it does not count towards a notebook's inert cells.
INSTALL_CELL_RE = re.compile(r"^\s*#?\s*[!%]\s*pip\s+install\b", re.MULTILINE)


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


def _is_install_cell(source: str) -> bool:
    """True for the commented-out ``!pip install`` cell notebooks conventionally open with.

    The exemption is anchored to the whole-cell shape (a couple of comment
    lines at most), so a long commented-out stub cell cannot dodge the inert
    count by merely citing a pip-install line.
    """
    lines = [line for line in source.splitlines() if line.strip()]
    return len(lines) <= 3 and bool(INSTALL_CELL_RE.search(source))


def _safe_parse(source: str) -> ast.Module | None:
    """Parse a notebook code cell, tolerating IPython magics and partial snippets."""
    stripped = "\n".join(
        "" if line.lstrip().startswith(("!", "%")) else line for line in source.splitlines()
    )
    try:
        return ast.parse(stripped)
    except SyntaxError:
        return None


def _gpio_bound_names(sources: list[str]) -> set[str]:
    """Names in this notebook that are bound to ``geoparquet_io``.

    Covers both ``import geoparquet_io as gpio`` and
    ``from geoparquet_io.api import Table, ops, pipe, read``.
    """
    bound: set[str] = set()
    for source in sources:
        tree = _safe_parse(source)
        if tree is None:
            continue
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    if alias.name == "geoparquet_io" or alias.name.startswith("geoparquet_io."):
                        bound.add(alias.asname or alias.name.split(".")[0])
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                if module == "geoparquet_io" or module.startswith("geoparquet_io."):
                    for alias in node.names:
                        bound.add(alias.asname or alias.name)
    return bound


def _root_name(node: ast.expr) -> str | None:
    """The leftmost identifier of a (possibly dotted) call target."""
    while isinstance(node, ast.Attribute):
        node = node.value
    return node.id if isinstance(node, ast.Name) else None


def _calls_gpio(source: str, bound: set[str]) -> bool:
    """True when this cell calls something bound to ``geoparquet_io``.

    Resolved through the AST against the notebook's own imports rather than by
    regex: a bare ``\\bread\\s*\\(`` pattern also matches ``f.read()``, which
    would let a notebook with zero gpio usage satisfy the invariant.
    """
    tree = _safe_parse(source)
    if tree is None:
        return False
    return any(
        isinstance(node, ast.Call) and _root_name(node.func) in bound for node in ast.walk(tree)
    )


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

    code = _cells(path, "code")
    live = [src for src in code if not _is_inert(src)]
    assert live, (
        f"{path.name} has no executable code cells, so its nbmake green check "
        "would be a false positive. Give it runnable content, or add it to "
        "ILLUSTRATIVE_NOTEBOOKS in examples/conftest.py."
    )
    bound = _gpio_bound_names(live)
    assert bound, (
        f"{path.name} never imports geoparquet_io, so nbmake proves nothing about this project."
    )
    assert any(_calls_gpio(src, bound) for src in live), (
        f"{path.name} imports geoparquet_io but never calls it, so nbmake "
        "proves nothing about this project."
    )


def test_bare_read_call_does_not_count_as_gpio_usage() -> None:
    """``f.read()`` must not satisfy the "calls the library" invariant.

    The first version of this guard matched a bare ``\\bread\\s*\\(``, which any
    file handle satisfies -- a notebook with zero gpio usage would have passed.
    """
    cells = ["with open('x.txt') as f:\n    data = f.read()\n"]
    assert _gpio_bound_names(cells) == set()
    assert not _calls_gpio(cells[0], {"gpio"})


def test_gpio_usage_is_resolved_through_the_notebooks_own_imports() -> None:
    """Both import spellings the example notebooks actually use are recognised."""
    aliased = "import geoparquet_io as gpio"
    from_import = "from geoparquet_io.api import Table, ops, pipe, read"

    bound = _gpio_bound_names([aliased])
    assert bound == {"gpio"}
    assert _calls_gpio("t = gpio.read('a.parquet').add_bbox()", bound)

    bound = _gpio_bound_names([from_import])
    assert {"Table", "ops", "pipe", "read"} <= bound
    # A bare `read(...)` counts only because this notebook imported it from us.
    assert _calls_gpio("result = read('a.parquet')", bound)
    assert _calls_gpio("arrow = ops.add_bbox(arrow)", bound)


def test_commented_install_cell_is_not_counted_as_inert() -> None:
    """A short notebook must not be forced to declare itself over the install cell.

    The convention ships commented out on purpose so nbmake never pip-installs.
    Counting it as inert made a 2-cell notebook (install + one real cell) read as
    50% inert and demand an `nbsignal` directive it does not deserve.
    """
    install = "# Uncomment to install\n# !pip install geoparquet-io"
    assert _is_inert(install), "still inert: nothing executes"
    assert _is_install_cell(install), "but exempt from the inert count"

    # The exemption is narrow: an ordinary commented-out cell still counts.
    stub = "# gpio.read('x.parquet').upload('s3://bucket/x.parquet')"
    assert _is_inert(stub)
    assert not _is_install_cell(stub)


class _StubItem:
    """Minimal stand-in for a collected pytest item, for driving the hook."""

    def __init__(self, name: str) -> None:
        self.path = Path("/examples") / name
        self.markers: list[object] = []

    def add_marker(self, marker: object) -> None:
        self.markers.append(marker)


def _load_examples_conftest():
    """Import ``examples/conftest.py`` by path, the way test_doc_sync.py does."""
    path = EXAMPLES_DIR / "conftest.py"
    spec = importlib.util.spec_from_file_location("examples_conftest", str(path))
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_skip_hook_actually_applies_the_skip_marker() -> None:
    """Drive the real hook body and assert it marks the illustrative notebook.

    The nbmake end-to-end proof below is ``meta``-marked (post-merge slow lane),
    so on its own it leaves a window in which a neutered hook body -- one that
    matches the notebook but never calls ``add_marker`` -- passes every PR lane.
    This test closes that window for a few microseconds, by importing the hook
    and running it against stub items rather than asserting on its source shape.
    """
    conftest = _load_examples_conftest()
    excluded = sorted(conftest.ILLUSTRATIVE_NOTEBOOKS)
    assert excluded, "nothing to prove: no notebooks are excluded"

    target = _StubItem(excluded[0])
    healthy = _StubItem("01_getting_started.ipynb")
    assert healthy.path.name not in conftest.ILLUSTRATIVE_NOTEBOOKS

    conftest.pytest_collection_modifyitems([target, healthy])

    assert target.markers, (
        f"the hook matched {target.path.name} but never applied a marker, so "
        "nbmake would execute it and report a false-positive pass"
    )
    marker = target.markers[0]
    assert marker.name == "skip", f"expected a skip marker, got {marker.name!r}"
    assert len(marker.kwargs.get("reason", "").strip()) >= 20, (
        f"skip reason must explain itself in the CI log: {marker.kwargs!r}"
    )
    assert not healthy.markers, (
        f"the hook skipped {healthy.path.name}, which is not illustrative; "
        "that would silently drop real coverage"
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
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
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
    # The commented-out install cell is a deliberate convention, not dead code,
    # so it must not push a short but otherwise healthy notebook over the bar.
    inert = sum(1 for src in code if _is_inert(src) and not _is_install_cell(src))
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
