# Installation

## Quick Install

**CLI tool**:
```bash
pipx install --pip-args='--pre' geoparquet-io
# or: uv tool install --prerelease=allow geoparquet-io
```

**Python library**:
```bash
pip install --pre geoparquet-io
# or: uv add --prerelease=allow geoparquet-io
```

!!! note "Pre-release versions"
    geoparquet-io is currently in beta. The `--pre` flag (pip/pipx) or `--prerelease=allow` (uv) is required until version 1.0.0 is released.

pipx and uv tool install the CLI in isolation while keeping it globally available. Use pip/uv add when you need the Python API in your project.

## Installing Plugins

gpio supports plugins that add specialized format support. Install plugins alongside the main tool:

```bash
# Install with PMTiles support
uv tool install --prerelease=allow geoparquet-io --with gpio-pmtiles
pipx install --pip-args='--pre' geoparquet-io --preinstall gpio-pmtiles

# Add plugin to existing installation
uv tool install --prerelease=allow --with gpio-pmtiles geoparquet-io
pipx inject geoparquet-io gpio-pmtiles --pip-args='--pre'
```

Available plugins:

- **[gpio-pmtiles](https://github.com/geoparquet-io/gpio-pmtiles)** - Convert between GeoParquet and PMTiles

See the [Plugins Guide](../guide/plugins.md) for more information.

## From Source

For the latest development version:

```bash
# Install from PyPI into current environment
uv pip install geoparquet-io

# Or install from source
git clone https://github.com/geoparquet/geoparquet-io.git
cd geoparquet-io
uv sync --all-extras
```

## Requirements

- **Python**: 3.10 or higher
- **PyArrow**: 12.0.0+
- **DuckDB**: 1.5.0+

All dependencies are automatically installed when you install geoparquet-io.

## Optional Dependencies

### Development Tools

For contributing to geoparquet-io:

```bash
uv sync --all-extras
# or
pip install geoparquet-io[dev]
```

This installs:

- pytest for testing
- ruff for linting
- pre-commit for git hooks
- mypy for type checking

### Documentation

For building documentation:

```bash
uv pip install geoparquet-io[docs]
# or
pip install geoparquet-io[docs]
```

This installs:

- mkdocs for documentation generation
- mkdocs-material theme
- mkdocstrings for API documentation

## Verifying Installation

After installation, verify everything works:

```bash
# Check version
gpio --version

# Get help
gpio --help

# Run a simple command (requires a GeoParquet file)
gpio inspect your_file.parquet
```

## Shell Completion

Enable tab completion for `gpio` commands in your shell:

**Bash:**
```bash
# Add to ~/.bashrc
eval "$(_GPIO_COMPLETE=bash_source gpio)"
```

**Zsh:**
```bash
# Add to ~/.zshrc
eval "$(_GPIO_COMPLETE=zsh_source gpio)"
```

**Fish:**
```bash
# Add to ~/.config/fish/config.fish
eval (env _GPIO_COMPLETE=fish_source gpio)
```

After adding the appropriate line to your shell config, restart your shell or source the config file:
```bash
source ~/.bashrc    # Bash
source ~/.zshrc     # Zsh
source ~/.config/fish/config.fish  # Fish
```

Once enabled, you can tab-complete commands, subcommands, and options:
```bash
gpio <TAB>          # Shows: add, check, convert, extract, inspect, ...
gpio add <TAB>      # Shows: bbox, h3, kdtree, quadkey, ...
gpio add bbox --<TAB>  # Shows available options
```

## Upgrading

To upgrade to the latest version:

```bash
# CLI tool
pipx upgrade geoparquet-io --pip-args='--pre'
# or: uv tool upgrade --prerelease=allow geoparquet-io

# Python library
pip install --upgrade --pre geoparquet-io
# or: uv add --prerelease=allow geoparquet-io
```

## Uninstalling

To remove geoparquet-io:

```bash
# CLI tool
pipx uninstall geoparquet-io
# or: uv tool uninstall geoparquet-io

# Python library
pip uninstall geoparquet-io
# or: uv remove geoparquet-io
```

## Platform Support

geoparquet-io is tested on:

- **Operating Systems**: Linux, macOS, Windows
- **Python Versions**: 3.10, 3.11, 3.12, 3.13
- **Architectures**: x86_64, ARM64

## Troubleshooting

### DuckDB Installation Issues

If you encounter issues with DuckDB installation, try:

```bash
pip install --upgrade duckdb
```

### PyArrow Compatibility

Ensure you have PyArrow 12.0.0 or higher:

```bash
pip install --upgrade pyarrow>=12.0.0
```

### Using Virtual Environments with uv

uv automatically manages virtual environments, but if you need a fresh environment:

```bash
uv venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
uv pip install geoparquet-io
```

## Next Steps

Once installed, head to the [Quick Start Guide](quickstart.md) to learn how to use geoparquet-io.
