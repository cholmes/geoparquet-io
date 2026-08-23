"""Pytest glue: turn each fenced block in ``docs/guide/*.md`` into a test item.

Test ids are ``guide/sort.md:L14[bash]`` and the reported location is the doc
file and the fence's line, so a failure points straight at the example that
broke rather than at harness code.
"""

from __future__ import annotations

import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from tests.docs_examples.parser import Block, iter_blocks, strip_prompts
from tests.docs_examples.seeder import seed_workdir

#: Pages whose blocks run in the fast suite. Everything else lands in the slow
#: lane. Deliberately small: the fast subset is a smoke test that the core
#: commands in the docs still work, not a second full run.
FAST_PAGES = frozenset({"sort.md", "piping.md", "check.md"})

#: Wall-clock ceiling for a single example. Generous — it exists to stop a
#: prompting or hanging command from wedging CI, not to police runtimes.
BLOCK_TIMEOUT_SECONDS = 120


class DocExampleFile(pytest.File):
    """A guide page. Collects one item per executable fenced block."""

    def collect(self):
        docs_root = Path(str(self.path)).parent.parent
        for block in iter_blocks(Path(str(self.path)), docs_root):
            yield DocExampleItem.from_parent(self, name=block.test_id, block=block)


class DocExampleItem(pytest.Item):
    """One ``bash`` or ``python`` example from a guide page."""

    def __init__(self, *, block: Block, **kwargs):
        super().__init__(**kwargs)
        self.block = block
        self._apply_marks()

    def _apply_marks(self) -> None:
        directives = self.block.directives
        self.add_marker(pytest.mark.docs_example)
        if directives.network:
            self.add_marker(pytest.mark.network)
        if _is_slow(self.block):
            self.add_marker(pytest.mark.slow)
        if directives.skip:
            reason = directives.skip_reason or "marked skip by a doctest directive"
            self.add_marker(pytest.mark.skip(reason=reason))
        if directives.needs_tippecanoe and shutil.which("tippecanoe") is None:
            self.add_marker(pytest.mark.skip(reason="tippecanoe not installed"))
        if directives.needs_ogr and shutil.which("ogr2ogr") is None:
            self.add_marker(pytest.mark.skip(reason="ogr2ogr not installed"))

    def runtest(self) -> None:
        source = strip_prompts(self.block.source)
        if self.block.directives.prelude:
            source = self.block.directives.prelude + "\n" + source
        if self.block.directives.menu:
            if self.block.lang != "bash":
                pytest.fail("the menu directive only applies to bash blocks")
            problem = check_menu_is_splittable(source)
            if problem:
                pytest.fail(f"menu directive refused: {problem}")
            for statement in split_statements(source):
                self._run_in_fresh_dir(statement)
            return
        self._run_in_fresh_dir(source)

    def _run_in_fresh_dir(self, source: str) -> None:
        workdir = seed_workdir(self._tmp_base() / "work")
        for command in self.block.directives.setup:
            self._run("bash", command, workdir, phase="setup")
        self._run(self.block.lang, source, workdir, phase="block")

    def _tmp_base(self) -> Path:
        slug = self.block.test_id.replace("/", "_").replace(":", "_").replace("[", "_")
        return self.config._tmp_path_factory.mktemp(slug.rstrip("]")[:30], numbered=True)

    def _run(self, lang: str, source: str, workdir: Path, *, phase: str) -> None:
        script = workdir / (".doctest_block.sh" if lang == "bash" else ".doctest_block.py")
        script.write_text(source if lang == "python" else f"set -euo pipefail\n{source}\n")
        argv = ["bash", str(script)] if lang == "bash" else [sys.executable, str(script)]
        try:
            completed = subprocess.run(
                argv,
                cwd=workdir,
                env=_subprocess_env(),
                capture_output=True,
                text=True,
                timeout=BLOCK_TIMEOUT_SECONDS,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise DocExampleFailure(
                self.block, phase, source, -1, "", f"timed out after {exc.timeout}s"
            ) from None
        if completed.returncode != 0:
            raise DocExampleFailure(
                self.block, phase, source, completed.returncode, completed.stdout, completed.stderr
            )

    def repr_failure(self, excinfo, style=None):  # noqa: ARG002
        if isinstance(excinfo.value, DocExampleFailure):
            return str(excinfo.value)
        return super().repr_failure(excinfo)

    def reportinfo(self):
        # The 0-based line pytest wants; points at the opening fence.
        return self.path, self.block.line - 1, self.block.test_id


class DocExampleFailure(Exception):
    """A documentation example that exited non-zero.

    Formats itself as the doc location, the source that ran, and the output,
    because the useful debugging material is the example, not a Python traceback
    through the harness.
    """

    def __init__(
        self, block: Block, phase: str, source: str, returncode: int, stdout: str, stderr: str
    ):
        super().__init__(block.test_id)
        self.block, self.phase = block, phase
        self.source, self.returncode = source, returncode
        self.stdout, self.stderr = stdout, stderr

    def __str__(self) -> str:
        where = f"{self.block.path}:{self.block.line}"
        header = f"{self.block.lang} example failed (exit {self.returncode}) at {where}"
        if self.phase == "setup":
            header = f"doctest setup= command failed (exit {self.returncode}) for {where}"
        parts = [header, "", _indent(self.source, "  | ")]
        if self.stdout.strip():
            parts += ["", "--- stdout ---", _tail(self.stdout)]
        if self.stderr.strip():
            parts += ["", "--- stderr ---", _tail(self.stderr)]
        parts += [
            "",
            "Fix the example, or mark it in the doc with an HTML comment on the",
            'line above the fence, e.g. <!-- doctest: skip="needs credentials" -->.',
        ]
        return "\n".join(parts)


#: Shell syntax that makes "one line, one independent command" untrue. A menu
#: block containing any of it is refused rather than mis-split.
_NOT_A_MENU = re.compile(r"^\s*(for|while|if|case|function)\b|<<|\$\(|`|^\s*\w+=", re.M)


def split_statements(source: str) -> list[str]:
    """Split a menu block into its individual commands.

    Deliberately simple: a statement starts at a non-indented, non-comment line
    and continues while lines end in ``\\``, ``|``, ``&&`` or ``||``. Comments
    are carried along with the command they introduce so a failure still reads
    like the doc. Anything more shell-like than that is rejected by
    :func:`check_menu_is_splittable` instead of being guessed at.
    """
    statements: list[str] = []
    pending: list[str] = []
    continuing = False
    for line in source.split("\n"):
        stripped = line.strip()
        if not stripped and not continuing:
            continue
        starts_new = not continuing and not line[:1].isspace()
        if starts_new and any(not ln.strip().startswith("#") for ln in pending):
            statements.append("\n".join(pending))
            pending = []
        pending.append(line)
        continuing = stripped.endswith(("\\", "|", "&&", "||"))
    if pending and any(not ln.strip().startswith("#") for ln in pending):
        statements.append("\n".join(pending))
    return statements


def check_menu_is_splittable(source: str) -> str | None:
    """Return why ``source`` may not be treated as a menu, or ``None``."""
    match = _NOT_A_MENU.search(source)
    if match:
        return (
            f"contains shell syntax that is not a standalone command "
            f"({match.group(0).strip()!r}); the lines are a script, not a menu"
        )
    return None


def _is_slow(block: Block) -> bool:
    """Fast lane membership. Directives win over the page default."""
    if block.directives.fast:
        return False
    if block.directives.slow:
        return True
    return block.path.name not in FAST_PAGES


def _subprocess_env() -> dict[str, str]:
    """Environment for an example: the project's venv first on ``PATH``."""
    env = dict(os.environ)
    venv_bin = str(Path(sys.executable).parent)
    env["PATH"] = venv_bin + os.pathsep + env.get("PATH", "")
    env["NO_COLOR"] = "1"
    env["COLUMNS"] = "100"
    # Examples must never inherit the harness's own coverage instrumentation.
    env.pop("COV_CORE_SOURCE", None)
    return env


def _indent(text: str, prefix: str) -> str:
    return "\n".join(prefix + line for line in text.rstrip("\n").split("\n"))


def _tail(text: str, limit: int = 40) -> str:
    lines = text.rstrip("\n").split("\n")
    if len(lines) <= limit:
        return _indent(text, "  ")
    return _indent(
        "\n".join([f"... {len(lines) - limit} earlier lines ...", *lines[-limit:]]), "  "
    )
