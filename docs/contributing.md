# Contributing

## Prerequisites

- Python 3.10+
- [uv](https://docs.astral.sh/uv/)

## Setup

```bash
git clone https://github.com/geoparquet/geoparquet-io.git
cd geoparquet-io
uv sync --all-extras
uv run pre-commit install
```

## Tests

```bash
uv run pytest                                          # Full suite
uv run pytest tests/test_yourfile.py -v               # Single file
uv run pytest -m "not slow and not network"           # Fast tests only
```

Coverage minimum: 67% (enforced in CI).

<!-- BEGIN GENERATED: test-markers -->
### Test Markers

| Marker | Description |
|--------|-------------|
| `@pytest.mark.slow` | marks tests as slow (deselect with '-m "not slow"') |
| `@pytest.mark.network` | marks tests requiring network access (deselect with '-m "not network"') |
| `@pytest.mark.integration` | marks end-to-end integration tests |
<!-- END GENERATED: test-markers -->

## Code Quality

All handled by pre-commit:

```bash
uv run pre-commit run --all-files
```

## Documentation

```bash
uv run mkdocs serve
```

## Commits

Use [Conventional Commits](https://www.conventionalcommits.org/): `type(scope): message`

Types: `feat`, `fix`, `docs`, `refactor`, `test`, `chore`

## Architecture

New CLI commands need corresponding Python API:

1. Core logic in `geoparquet_io/core/<feature>.py`
2. CLI wrapper in `geoparquet_io/cli/main.py`
3. Python API in `geoparquet_io/api/table.py` and `api/ops.py`

See `CLAUDE.md` for full architecture details.

## Releasing

(Maintainers only)

### Using `cz bump` (recommended)

[Commitizen](https://commitizen-tools.github.io/commitizen/) automates version bumps, changelog updates, and tagging:

```bash
uv run cz bump              # Auto-determine version from commit history
uv run cz bump --increment PATCH  # Force a patch bump (1.0.0 → 1.0.1)
uv run cz bump --increment MINOR  # Force a minor bump (1.0.0 → 1.1.0)
uv run cz bump --increment MAJOR  # Force a major bump (1.0.0 → 2.0.0)
uv run cz bump --prerelease beta  # Pre-release (1.0.0 → 1.1.0b0)
uv run cz bump --dry-run          # Preview without making changes
```

This will:

1. Bump the version in `pyproject.toml` and `geoparquet_io/cli/main.py`
2. Update `CHANGELOG.md` from conventional commit messages
3. Create a commit and git tag (e.g., `v1.1.0`)

After bumping:

```bash
git push origin main --tags
```

Then create a [GitHub Release](https://github.com/geoparquet/geoparquet-io/releases/new) from the new tag. The `publish.yml` workflow will automatically build and publish to PyPI.

### Version files

Version is maintained in two places (kept in sync by `cz bump` and enforced by CI):

- `pyproject.toml` — `version = "x.y.z"` (package metadata)
- `geoparquet_io/cli/main.py` — `__version__ = "x.y.z"` (CLI `--version` output)

## License

By contributing, you agree that your contributions will be licensed under the Apache 2.0 License.
