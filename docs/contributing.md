# Contributing to geoparquet-io

Thank you for your interest in contributing!

## Development Setup

### Prerequisites

- Python 3.10+
- [uv](https://docs.astral.sh/uv/) package manager

### Getting Started

```bash
git clone https://github.com/geoparquet/geoparquet-io.git
cd geoparquet-io
uv sync --all-extras
uv run pre-commit install
```

## Testing

### Quick Test (Your Changes)

```bash
uv run pytest tests/test_yourfile.py -v -m "not slow and not network"
```

### Full Test Suite

```bash
uv run pytest --cov=geoparquet_io --cov-report=term-missing
```

Coverage minimum: 67% (enforced in CI).

!!! tip "External Contributors"
    Some tests check tooling availability (codespell, mypy, etc.). These may fail locally
    but run in CI—focus on tests relevant to your changes.

<!-- BEGIN GENERATED: test-markers -->
### Test Markers

| Marker | Description |
|--------|-------------|
| `@pytest.mark.slow` | marks tests as slow (deselect with '-m "not slow"') |
| `@pytest.mark.network` | marks tests requiring network access (deselect with '-m "not network"') |
| `@pytest.mark.integration` | marks end-to-end integration tests |
<!-- END GENERATED: test-markers -->

## Code Quality

**All handled by pre-commit.** See `.pre-commit-config.yaml` for the full list.

```bash
uv run pre-commit run --all-files
```

Style config in `pyproject.toml [tool.ruff]`. Key settings:
- Line length: 100
- Double quotes
- Type hints encouraged

### Pre-Push Hooks (Optional)

```bash
uv run pre-commit install --hook-type pre-push
export ENABLE_PRE_PUSH_TESTS=1  # Enable fast tests before push
```

## Making Changes

### Branch Naming

- `feature/description` - New features
- `fix/description` - Bug fixes
- `docs/description` - Documentation

### Commit Messages

**Enforced by commitizen hook.** Use [Conventional Commits](https://www.conventionalcommits.org/):

```text
<type>(scope): description
```

| Type | Use for |
|------|---------|
| `feat` | New features |
| `fix` | Bug fixes |
| `docs` | Documentation |
| `refactor` | Code changes that don't add features or fix bugs |
| `test` | Adding or updating tests |
| `chore` | Maintenance tasks |

**Examples:**
```text
feat(convert): Add streaming mode for large files
fix(bbox): Correct metadata format for GeoParquet 1.1
docs(readme): Update installation instructions
```

### Pull Request Process

1. Create branch from `main`
2. Make changes + add tests
3. Run `uv run pre-commit run --all-files`
4. Push and create PR
5. Fill in PR template, link issues

### PR Requirements

- [ ] Tests pass for your changes
- [ ] Code formatted (pre-commit handles this)
- [ ] Documentation updated if needed
- [ ] CHANGELOG.md updated for user-facing changes

## Writing Tests

```python
def test_feature_description():
    """Brief description of what this test verifies."""
    # Arrange
    input_data = create_test_data()

    # Act
    result = function_under_test(input_data)

    # Assert
    assert result == expected_value
```

Add fixtures to `tests/conftest.py`. Use markers for slow/network tests.

## Architecture Note

New CLI commands need corresponding Python API:

1. Core logic in `geoparquet_io/core/<feature>.py`
2. CLI wrapper in `geoparquet_io/cli/main.py`
3. Python API in `geoparquet_io/api/table.py` and `api/ops.py`

See `CLAUDE.md` for full architecture details.

## Release Process

(For maintainers only)

1. Update version in `pyproject.toml`
2. Update `CHANGELOG.md`
3. Create git tag: `git tag v0.x.0 && git push origin v0.x.0`
4. GitHub Actions builds and publishes to PyPI

## Questions?

- Open an issue for bugs or feature requests
- Check existing issues before creating new ones

## License

By contributing, you agree that your contributions will be licensed under the Apache 2.0 License.
