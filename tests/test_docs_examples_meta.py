"""Guard the docs-as-tests harness against silent opt-outs.

The point of running the guides' examples is that a broken example turns CI red.
That only holds if every example is *reachable* by the harness. There are four
ways a block can slip out of reach, and one test here for each:

1. It is fenced with a language the harness does not execute (```sh, ```py,
   ```console) — collected by nobody, noticed by nobody.
2. A stray ``` marker upstream shifts the fence pairing, so the block is
   swallowed into the body of a phantom one. This was live in benchmarks.md and
   hid a real bash block; nothing about the rendered page looked wrong.
3. It carries a directive the parser cannot read, or opts out with no stated
   reason.
4. It is fenced as prose (```text) while actually containing commands.

Plus ratchets in both directions: opt-outs may not creep up, and the number of
blocks that actually execute may not quietly fall.

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

#: Ratchets, set ~15 blocks either side of the current counts (228 skipped, 232
#: executed - review moved 17 fences to skip: illustrative WFS endpoints that
#: could only fail, plus install/uninstall commands that mutate the real
#: environment). The headroom keeps ordinary doc edits from tripping them while
#: still catching a drift of any size; moving either number is a deliberate act
#: that shows up in a diff and needs a justification in the pull request.
MAX_SKIPPED_BLOCKS = 243
MIN_EXECUTED_BLOCKS = 217

#: Looks like a command rather than prose or sample output.
_COMMAND_LIKE = re.compile(r"^\s*(gpio\s|import geoparquet_io|from geoparquet_io\s)")

#: Commands that reach outside the seeded temp directory and mutate the real
#: machine. The harness runs fences unconfined as the developer, so an
#: unskipped fence with one of these does not test a doc example - it
#: installs, uninstalls, or escalates on whoever runs the suite. This already
#: happened three times before the guard existed (pipx installs from PyPI and
#: a uv tool uninstall of the developer's global gpio).
_MUTATES_ENVIRONMENT = re.compile(
    r"^\s*(sudo\s"
    r"|pipx\s+(install|uninstall|inject|uninject)\s"
    r"|pip\s+(install|uninstall)\s"
    r"|uv\s+tool\s"
    r"|brew\s+(install|uninstall)\s"
    r"|apt(-get)?\s+(install|remove)\s"
    r"|rm\s+-[a-z]*rf?\s+[~/])"
)

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
def test_every_fence_marker_belongs_to_a_block(page: Path):
    """No stray ``` markers: every one opens or closes a block the parser found.

    This is the totality guard the language check cannot give. A single stray
    marker does not look like an error — the page still renders — but it shifts
    the pairing of every fence after it, so a real ```bash block downstream gets
    swallowed into the body of a phantom one and is never collected. That is a
    silent hole in the coverage, which is exactly what this harness exists to
    prevent. (One such stray was live in benchmarks.md and hid a real block.)
    """
    lines = page.read_text(encoding="utf-8").split("\n")
    markers = {i for i, line in enumerate(lines, start=1) if line.strip().startswith("```")}
    paired = set()
    for block in iter_fences(page, DOCS_ROOT):
        paired.add(block.line)
        paired.add(block.end_line)
    stray = sorted(markers - paired)
    # The first unpaired marker is the one that broke the pairing; the rest are
    # usually its downstream fallout, so lead with it.
    assert not stray, (
        f"{page.name}: first stray ``` is on line {stray[0]}; unpaired marker(s) "
        f"on line(s) {stray} do not open or close any "
        "block the parser can pair. A stray marker silently swallows the next "
        "real code block — delete it, or close the fence it belongs to."
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


@pytest.mark.parametrize("page", GUIDE_PAGES, ids=lambda p: p.name)
def test_no_runnable_fence_mutates_the_real_environment(page: Path):
    """A fence the harness will execute must stay inside its seeded directory.

    Showing an install command in the docs is fine; running one is not. Any
    fence that installs, uninstalls, or escalates must carry a ``skip``
    directive so the harness never executes it.
    """
    offenders = [
        f"{block.path.name}:{block.line}: {line.strip()}"
        for block in iter_fences(page, DOCS_ROOT)
        if block.executable and block.directives.runs and block.lang == "bash"
        for line in block.source.split("\n")
        if _MUTATES_ENVIRONMENT.match(line)
    ]
    assert not offenders, (
        "Runnable fence(s) contain commands that mutate the real environment:\n  "
        + "\n  ".join(offenders)
        + '\nMark the fence <!-- doctest: skip="..." --> so the harness never runs it.'
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
