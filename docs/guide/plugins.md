# Plugins

gpio supports a plugin system that allows you to extend its functionality with specialized format support and additional commands. Plugins are distributed as separate Python packages and are automatically discovered when installed.

## Installing Plugins

### During Initial Installation

Install gpio with plugins in one command:

=== "uv"
    ```bash
    # Install gpio with PMTiles support
    uv tool install geoparquet-io --with gpio-pmtiles

    # Install with multiple plugins
    uv tool install geoparquet-io --with gpio-pmtiles --with gpio-other
    ```

=== "pipx"
    ```bash
    # Install gpio with PMTiles support
    pipx install geoparquet-io --preinstall gpio-pmtiles

    # Install with multiple plugins (chain --preinstall flags)
    pipx install geoparquet-io --preinstall gpio-pmtiles --preinstall gpio-other
    ```

### Adding to Existing Installation

Add plugins to an already-installed gpio tool:

=== "uv"
    ```bash
    # Add plugin to existing installation
    uv tool install --with gpio-pmtiles geoparquet-io
    ```

=== "pipx"
    ```bash
    # Inject plugin into existing installation
    pipx inject geoparquet-io gpio-pmtiles
    ```

### In Python Projects

For Python API usage, install plugins as regular dependencies:

=== "uv"
    ```bash
    uv add geoparquet-io gpio-pmtiles
    ```

=== "pip"
    ```bash
    pip install geoparquet-io gpio-pmtiles
    ```

## Available Plugins

### gpio-pmtiles

**Repository**: [github.com/geoparquet-io/gpio-pmtiles](https://github.com/geoparquet-io/gpio-pmtiles)
**PyPI**: [pypi.org/project/gpio-pmtiles](https://pypi.org/project/gpio-pmtiles/)

Convert between GeoParquet and PMTiles format for efficient web map tiles.

**Installation**:
```bash
uv tool install geoparquet-io --with gpio-pmtiles
```

**Commands**:
```bash
# Convert GeoParquet to PMTiles
gpio pmtiles create input.parquet output.pmtiles
```

**Use Cases**:
- Generating tilesets from GeoParquet for web mapping
- Building vector tile pipelines

## How Plugins Work

### Plugin Discovery

gpio uses Python's [entry points](https://packaging.python.org/en/latest/specifications/entry-points/) system to discover plugins. When you run a `gpio` command:

1. gpio loads its built-in commands
2. Searches for packages with `gpio.plugins` entry points
3. Automatically loads and registers plugin commands
4. Makes plugin commands available under the `gpio` CLI

### Entry Point Registration

Plugins register themselves by declaring an entry point in their `pyproject.toml`:

```toml
[project.entry-points."gpio.plugins"]
pmtiles = "gpio_pmtiles.cli:pmtiles"
```

This tells gpio:
- The plugin provides a command group named `pmtiles`
- The command implementation is at `gpio_pmtiles.cli:pmtiles`

### Verifying Plugin Installation

Check if a plugin is installed and discoverable:

```bash
# List all installed tools (including plugins)
uv tool list
# or
pipx list

# Check if plugin command is available
gpio pmtiles --help
```

## Removing Plugins

### From Tool Installation

=== "uv"
    ```bash
    # Reinstall without the plugin
    uv tool uninstall geoparquet-io
    uv tool install geoparquet-io
    ```

=== "pipx"
    ```bash
    # Remove injected plugin
    pipx uninject geoparquet-io gpio-pmtiles
    ```

### From Python Projects

=== "uv"
    ```bash
    uv remove gpio-pmtiles
    ```

=== "pip"
    ```bash
    pip uninstall gpio-pmtiles
    ```

## Developing Plugins

Want to create your own gpio plugin? Here's what you need:

### Plugin Structure

A minimal gpio plugin consists of:

```
gpio-myplugin/
├── pyproject.toml          # Plugin metadata and entry point
├── gpio_myplugin/
│   ├── __init__.py
│   └── cli.py              # Click command group
└── tests/
    └── test_myplugin.py
```

### Entry Point Setup

In `pyproject.toml`:

```toml
[project]
name = "gpio-myplugin"
version = "0.1.0"
dependencies = [
    "geoparquet-io>=0.9.0",
    "click>=8.0.0",
]

[project.entry-points."gpio.plugins"]
myplugin = "gpio_myplugin.cli:myplugin"
```

### Command Implementation

In `gpio_myplugin/cli.py`:

```python
import click

@click.group()
def myplugin():
    """My custom plugin commands."""
    pass

@myplugin.command()
@click.argument('input_file')
@click.argument('output_file')
def process(input_file, output_file):
    """Process a file with my plugin."""
    click.echo(f"Processing {input_file} -> {output_file}")
    # Your implementation here
```

### Best Practices

1. **Naming**: Use `gpio-<format>` pattern (e.g., `gpio-pmtiles`, `gpio-flatgeobuf`)
2. **Dependencies**: Declare `geoparquet-io` as a dependency
3. **Documentation**: Include README with installation and usage
4. **Testing**: Add tests for your plugin commands
5. **Entry Point**: Use `gpio.plugins` as the entry point group
6. **Command Group**: Make your top-level command a Click group for extensibility

### Publishing

Once your plugin is ready:

1. **Test locally**:
   ```bash
   cd gpio-myplugin
   uv tool install geoparquet-io --with .
   gpio myplugin --help
   ```

2. **Publish to PyPI**:
   ```bash
   uv build
   uv publish
   ```

3. **Create repository**: Follow the `gpio-<format>` naming convention
4. **Submit PR**: Add your plugin to this documentation

## Plugin Ideas

Looking for inspiration? Consider creating plugins for:

- **Formats**: FlatGeobuf, Shapefile, GeoJSON-seq, GeoPackage streaming
- **Spatial Operations**: Spatial joins, buffering, intersections
- **Cloud Services**: Specific cloud storage integrations
- **Visualization**: Map preview, quick visualization tools
- **Analytics**: Statistical analysis, spatial clustering

## Support

- **Questions**: [geoparquet-io Discussions](https://github.com/geoparquet/geoparquet-io/discussions)
- **Issues**: [geoparquet-io Issues](https://github.com/geoparquet/geoparquet-io/issues)
- **Plugin-specific**: Check each plugin's repository for support
