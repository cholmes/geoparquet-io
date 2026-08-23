"""Guard the docs-as-tests harness against silent opt-outs.

The point of running the guides' examples is that a broken example turns CI red.
That only holds if every example is *reachable* by the harness. There are three
ways a block can slip out of reach, and one test here for each:

1. It is fenced with a language the harness does not execute (```sh, ```py,
   ```console) — collected by nobody, noticed by nobody.
2. It carries a directive the parser cannot read, or opts out with no stated
   reason.
3. It is fenced as prose (```text) while actually containing commands.

Plus a ratchet: the number of opted-out blocks may not grow unnoticed.

These are cheap string checks, so they live in the fast suite where they gate
every pull request.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from tests.docs_examples.parser import (
    EXECUTABLE_LANGUAGES,
    INERT_LANGUAGES,
    DirectiveSyntaxError,
    iter_fences,
)

GUIDE_DIR = Path(__file__).resolve().parent.parent / "docs" / "guide"
DOCS_ROOT = GUIDE_DIR.parent

#: Ratchets. Raising either of these is a deliberate act that shows up in a diff
#: and needs a justification in the pull request; that is the whole point.
MAX_SKIPPED_BLOCKS = 130
MIN_EXECUTED_BLOCKS = 260

#: Looks like a command rather than prose or sample output.
_COMMAND_LIKE = re.compile(r"^\s*(gpio\s|import geoparquet_io|from geoparquet_io\s)")

GUIDE_PAGES = sorted(GUIDE_DIR.glob("*.md"))


def _all_fences():
    for page in GUIDE_PAGES:
        yield from iter_fences(page, DOCS_ROOT)


def test_guide_pages_are_present():
    """A collection bug that finds no pages would make every test below vacuous."""
    assert len(GUIDE_PAGES) >= 20


@pytest.mark.parametrize("page", GUIDE_PAGES, ids=lambda p: p.name)
def test_every_fence_uses_a_known_language(page: Path):
    """No aliases. ```sh and ```py look runnable but would never run."""
    known = set(EXECUTABLE_LANGUAGES) | set(INERT_LANGUAGES)
    unknown = [
        f"{block.path.name}:{block.line} uses ```{block.lang}"
        for block in iter_fences(page, DOCS_ROOT)
        if block.lang not in known
    ]
    assert not unknown, (
        "Unknown fence language(s):\n  "
        + "\n  ".join(unknown)
        + f"\nUse one of {sorted(known)}. Executable ones are {list(EXECUTABLE_LANGUAGES)}; "
        "anything else is treated as inert prose and never runs."
    )


@pytest.mark.parametrize("page", GUIDE_PAGES, ids=lambda p: p.name)
def test_directives_parse(page: Path):
    """A typo'd directive must fail here, not quietly leave a block unguarded."""
    try:
        list(iter_fences(page, DOCS_ROOT))
    except DirectiveSyntaxError as exc:
        pytest.fail(f"{page.name}: {exc}")


@pytest.mark.parametrize("page", GUIDE_PAGES, ids=lambda p: p.name)
def test_opting_out_requires_a_reason(page: Path):
    """``skip`` with no reason is an unexplained hole in the coverage."""
    unexplained = [
        f"{block.path.name}:{block.line}"
        for block in iter_fences(page, DOCS_ROOT)
        if block.executable and block.directives.skip and not block.directives.skip_reason.strip()
    ]
    assert not unexplained, (
        "Blocks skipped without a reason:\n  "
        + "\n  ".join(unexplained)
        + '\nWrite <!-- doctest: skip="why" --> so the next reader knows.'
    )


@pytest.mark.parametrize("page", GUIDE_PAGES, ids=lambda p: p.name)
def test_commands_are_not_hidden_in_inert_fences(page: Path):
    """A command fenced as prose escapes the harness. Say so with a directive.

    Some genuinely belong in an inert fence — the PowerShell variant of a bash
    example, say — but that has to be a stated decision, not an accident.
    """
    hidden = [
        f"{block.path.name}:{block.line} (```{block.lang or 'text'})"
        for block in iter_fences(page, DOCS_ROOT)
        if not block.executable
        and _COMMAND_LIKE.match(block.source.lstrip("\n"))
        and not block.directives.present
    ]
    assert not hidden, (
        "Command-like content in a fence the harness cannot run:\n  "
        + "\n  ".join(hidden)
        + "\nEither fence it as ```bash/```python so it is executed, or mark it"
        ' <!-- doctest: skip="why this cannot run" -->.'
    )


def test_opt_outs_do_not_creep():
    """Ratchet: opting a block out has to stay unusual enough to notice."""
    skipped = [b for b in _all_fences() if b.executable and b.directives.skip]
    assert len(skipped) <= MAX_SKIPPED_BLOCKS, (
        f"{len(skipped)} guide blocks are skipped, over the agreed ceiling of "
        f"{MAX_SKIPPED_BLOCKS}. Make the example runnable instead, or raise the "
        "ceiling in this test and say why in the pull request."
    )


def test_most_examples_actually_run():
    """Ratchet the other way: the suite must keep executing real examples."""
    executed = [b for b in _all_fences() if b.executable and b.directives.runs]
    assert len(executed) >= MIN_EXECUTED_BLOCKS, (
        f"only {len(executed)} guide blocks execute, below the floor of "
        f"{MIN_EXECUTED_BLOCKS}; the docs-as-tests harness is being hollowed out."
    )
