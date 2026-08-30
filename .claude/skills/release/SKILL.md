---
name: release
description: Use when cutting a geoparquet-io release - builds the CHANGELOG section from merged PRs in the house style, stops for human review, then bumps, tags and posts the GitHub release notes.
---

# Releasing geoparquet-io

Every release section of `CHANGELOG.md` is generated, never hand-written. It is
GitHub's own release-note list — one line per pull request, with its author and
number — grouped into Keep a Changelog sections, under two to four paragraphs of
highlights that you write.

**There is a mandatory human review checkpoint at step 5. Nothing is bumped,
tagged or published before the user approves the changelog section.**

## 1. Preflight

```bash
git checkout main && git pull
gh run list --branch main --limit 5          # main must be green
git status --short                            # must be clean
uv run pytest -n auto -m "not slow and not network and not meta"
```

Pick the version from what merged: a `!` title or a withdrawn command means
minor at least (this project is 1.x and pre-1.0 rules no longer apply), new
commands mean minor, fixes alone mean patch.

## 2. Generate the section

```bash
uv run python scripts/release_notes.py <version> --previous v<previous> --write
```

This replaces the `## Unreleased` block with the new section. It refuses to run
twice for one version. Without `--write` it prints to stdout, which is the way
to preview.

## 3. Retitle whatever landed in `### Uncategorized`

Pull requests whose titles carry no conventional-commit type land there. Do not
edit the changelog by hand and do not touch the pull request: add a rewritten
title to `scripts/release_title_overrides.json`, keyed by PR number, and run the
generator again. The type you give it is what files the entry.

```json
"645": "fix(geometry): stop the SIGSEGV in repair on tables with NULL geometry rows"
```

Read each pull request before you retitle it, and write the line a user should
read. Two judgements the script cannot make:

- **Give it a `!`** when the change removes a command, changes a default, or
  withdraws a capability, even though the merged title had none. Squash merges
  take the PR title, so a `!` written in a commit body is already lost. This is
  how an entry reaches `### Breaking`.
- Human dependency decisions — a pin relaxed, a floor raised for a CVE — take
  `build(deps)` so they sit with the bot bumps.

Regenerate until `### Uncategorized` is gone. Rerunning is safe: the overrides
file makes the whole section reproducible, so nothing is lost to a second run.

## 4. Write the highlights

Replace the `<!-- TODO ... -->` placeholder with two to four paragraphs, written
after reading the Breaking, Added and Changed entries. Cover, in this order:

1. What is newly possible — the commands and capabilities added.
2. The theme of the fixes, named concretely, not "various bug fixes".
3. Every breaking change, with what a user has to do about it.
4. **Every first-time contributor, by name, with what they contributed.**

The last paragraph is not optional. Get the material from:

```bash
uv run python scripts/release_notes.py <version> --previous v<previous> --contributors
```

That prints every pull request each new contributor wrote, not only the first
one GitHub names. For each person: **say how many pull requests they sent when it
was more than one**, summarize the work in a sentence or two — the actual
substance of it, so they can see they were read — thank them, and close by
inviting them back. Do not reduce a run of ten pull requests to "various fixes".

Do not restate the entry list. Aim for what a user needs to decide whether to
upgrade, and what a contributor needs to feel their work was noticed.

## 5. Human review — STOP HERE

Show the user the rendered section and wait for explicit approval. Say what you
want checked:

- the version number
- the summary paragraphs
- any entry you moved out of `### Uncategorized`, and anything you promoted to
  `### Breaking`

Do not run step 6 until the user approves. If the release ships alongside other
work, put the changelog in that pull request and let review happen there.

## 6. Bump and open the release PR

`update_changelog_on_bump` is off, so `cz bump` touches versions only and leaves
the section you just wrote alone.

```bash
git checkout -b release/v<version>
uv run cz bump --yes                 # pyproject.toml + [tool.commitizen].version
git push -u origin release/v<version>
gh pr create --title "bump: version <previous> → <version>"
```

Merging that PR fires `.github/workflows/publish.yml`, which tags `v<version>`,
publishes to PyPI, and creates the GitHub release.

## 7. Post the release notes

The workflow's release body is a placeholder. Replace it with the section you
wrote, plus a link back to the changelog anchor:

```bash
python3 - <<'PY' > /tmp/notes.md
import pathlib, re
v = "<version>"
t = pathlib.Path("CHANGELOG.md").read_text()
start = t.index(f"## v{v} ")
end = t.find("\n## ", start + 1)
body = t[start:end].split("\n", 1)[1].strip()
anchor = f"v{v}-" + "".join(c for c in "<date>" if c.isdigit() or c == "-")
print(body)
print()
print(f"Full changelog entry: https://github.com/geoparquet/geoparquet-io/blob/main/CHANGELOG.md#{anchor}")
PY

gh release edit v<version> --notes-file /tmp/notes.md
```

The anchor GitHub gives `## v1.4.0 (2026-08-30)` is `#v140-2026-08-30`: the
heading lowercased, dots and parentheses dropped, spaces to hyphens. Open the
link and confirm it lands on the heading before you finish.

## 8. Verify

```bash
uv run python -c "import geoparquet_io; print(geoparquet_io.__version__)"
pip index versions geoparquet-io          # PyPI has the new version
gh release view v<version>
```

## Conventions this skill enforces

- One line per pull request: `- <title> by @<author> in #<number>`. Titles come
  from the pull request, or from `scripts/release_title_overrides.json` where one
  did not follow the convention. `.github/workflows/pr-title.yml` checks new
  titles, so the overrides file should stop growing.
- Sections in order: Breaking, Added, Changed, Fixed, Documentation, Internal,
  Dependencies, then New Contributors and the Full Changelog link.
- Housekeeping sits below the user-facing sections; a reader can stop at
  Documentation.
- `docs/CHANGELOG.md` is generated from the root file by the `doc-sync`
  pre-commit hook. Never edit it.
