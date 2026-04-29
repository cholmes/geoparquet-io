"""PMTiles generation using tippecanoe subprocess.

Orchestrates a streaming pipeline: GeoParquet → gpio commands → tippecanoe → PMTiles.
Requires tippecanoe to be installed and available in PATH.
"""

import shutil
import subprocess
import sys
from pathlib import Path

from geoparquet_io.core.logging_config import debug, success


class TippecanoeNotFoundError(Exception):
    """Raised when tippecanoe is not found in PATH."""

    def __init__(self):
        super().__init__(
            "tippecanoe not found in PATH.\n\n"
            "To use gpio pmtiles, install tippecanoe:\n"
            "  macOS:  brew install tippecanoe\n"
            "  Ubuntu: sudo apt install tippecanoe\n"
            "  Source: https://github.com/felt/tippecanoe#installation\n\n"
            "Alternatively, use the streaming approach:\n"
            "  gpio convert geojson data.parquet | tippecanoe -P -o output.pmtiles"
        )


def _validate_path(path: str) -> None:
    """
    Validate file path to prevent shell injection.

    Raises:
        ValueError: If path contains shell metacharacters
    """
    dangerous_chars = [";", "|", "&", "$", "`", "\n", "\r"]
    for char in dangerous_chars:
        if char in path:
            raise ValueError(
                f"Path contains dangerous character '{char}': {path}\n"
                "File paths must not contain shell metacharacters."
            )


def _get_gpio_executable() -> str:
    """Get the path to the gpio executable in the current Python environment."""
    python_bin_dir = Path(sys.executable).parent
    gpio_path = python_bin_dir / "gpio"

    if gpio_path.exists() and gpio_path.is_file():
        return str(gpio_path)

    gpio_in_path = shutil.which("gpio")
    if gpio_in_path:
        return gpio_in_path

    return "gpio"


def _check_tippecanoe() -> bool:
    """Check if tippecanoe is available in PATH."""
    return shutil.which("tippecanoe") is not None


def _build_gpio_commands(
    input_path: str,
    bbox: str | None,
    where: str | None,
    include_cols: str | None,
    precision: int,
    verbose: bool,
    profile: str | None,
    src_crs: str | None,
) -> list[list[str]]:
    """
    Build the gpio command(s) for GeoJSON conversion.

    Returns a list of commands to be piped together.
    """
    gpio_exe = _get_gpio_executable()

    needs_reproject = src_crs is not None
    needs_extract = bbox or where or include_cols

    if needs_reproject or needs_extract:
        commands: list[list[str]] = []

        if needs_reproject:
            assert src_crs is not None  # Type narrowing for mypy
            reproject_cmd = [
                gpio_exe,
                "convert",
                "reproject",
                input_path,
                "-",
                "--dst-crs",
                "EPSG:4326",
                "--src-crs",
                src_crs,
            ]
            if verbose:
                reproject_cmd.append("--verbose")
            if profile:
                reproject_cmd.extend(["--profile", profile])
            commands.append(reproject_cmd)
            next_input = "-"
        else:
            next_input = input_path

        if needs_extract:
            extract_cmd = [gpio_exe, "extract", "geoparquet", next_input]

            if bbox:
                extract_cmd.extend(["--bbox", bbox])
            if where:
                extract_cmd.extend(["--where", where])
            if include_cols:
                extract_cmd.extend(["--include-cols", include_cols])
            if verbose:
                extract_cmd.append("--verbose")
            if profile and not needs_reproject:
                extract_cmd.extend(["--profile", profile])

            commands.append(extract_cmd)
            next_input = "-"

        convert_cmd = [gpio_exe, "convert", "geojson", next_input, "--precision", str(precision)]

        if verbose:
            convert_cmd.append("--verbose")
        if profile:
            convert_cmd.extend(["--profile", profile])

        commands.append(convert_cmd)

        return commands

    convert_cmd = [gpio_exe, "convert", "geojson", input_path, "--precision", str(precision)]

    if verbose:
        convert_cmd.append("--verbose")
    if profile:
        convert_cmd.extend(["--profile", profile])

    return [convert_cmd]


def _build_tippecanoe_command(
    output_path: str,
    layer: str | None,
    min_zoom: int | None,
    max_zoom: int | None,
    verbose: bool,
    attribution: str | None = None,
) -> list[str]:
    """Build the tippecanoe command with production-quality settings."""
    cmd = ["tippecanoe", "-P", "-o", output_path]

    if layer:
        cmd.extend(["-l", layer])
    else:
        layer_name = Path(output_path).stem
        cmd.extend(["-l", layer_name])

    if attribution is None:
        attribution = '<a href="https://geoparquet.io/" target="_blank">geoparquet-io</a>'
    cmd.append(f"--attribution={attribution}")

    if min_zoom is not None and max_zoom is not None:
        cmd.extend(["-Z", str(min_zoom), "-z", str(max_zoom)])
    elif min_zoom is not None:
        cmd.extend(["-Z", str(min_zoom), "-zg"])
    elif max_zoom is not None:
        cmd.extend(["-z", str(max_zoom)])
    else:
        cmd.append("-zg")

    cmd.append("--simplify-only-low-zooms")
    cmd.append("--no-simplification-of-shared-nodes")
    cmd.append("--no-tile-size-limit")
    cmd.append("--drop-densest-as-needed")

    if verbose:
        cmd.append("--progress-interval=1")

    return cmd


def _run_pipeline(
    gpio_commands: list[list[str]],
    tippecanoe_cmd: list[str],
    verbose: bool,
) -> None:
    """Execute the pipeline of commands."""
    if verbose:
        if len(gpio_commands) == 1:
            cmd_str = " ".join(gpio_commands[0])
            debug(f"Running: {cmd_str} | {' '.join(tippecanoe_cmd)}")
        else:
            cmd_str = " | ".join(" ".join(cmd) for cmd in gpio_commands)
            debug(f"Running: {cmd_str} | {' '.join(tippecanoe_cmd)}")

    processes: list[subprocess.Popen[bytes]] = []

    try:
        for i, cmd in enumerate(gpio_commands):
            stdin_source = processes[-1].stdout if processes else None

            proc = subprocess.Popen(
                cmd,
                stdin=stdin_source,
                stdout=subprocess.PIPE,
                stderr=None if verbose else subprocess.PIPE,
            )
            processes.append(proc)

            if i > 0 and processes[-2].stdout:
                processes[-2].stdout.close()

        tippecanoe_proc = subprocess.Popen(
            tippecanoe_cmd,
            stdin=processes[-1].stdout if processes else None,
            stdout=None if verbose else subprocess.PIPE,
            stderr=None,
        )
        processes.append(tippecanoe_proc)

        if len(processes) > 1 and processes[-2].stdout:
            processes[-2].stdout.close()

        tippecanoe_proc.communicate()

        if tippecanoe_proc.returncode != 0:
            raise RuntimeError(f"tippecanoe failed with exit code {tippecanoe_proc.returncode}")

        # Drain stderr and wait for earlier processes
        # Note: stdout is already closed for piping, so we only drain stderr
        for proc in processes[:-1]:
            if proc.stderr:
                proc.stderr.read()
                proc.stderr.close()
            proc.wait()
            if proc.returncode != 0:
                cmd_name = proc.args[0] if isinstance(proc.args, list) else "command"
                raise RuntimeError(f"{cmd_name} failed with exit code {proc.returncode}")

    except KeyboardInterrupt:
        for proc in processes:
            proc.terminate()
        raise
    except Exception:
        for proc in processes:
            if proc.poll() is None:
                proc.terminate()
        raise


def create_pmtiles_from_geoparquet(
    input_path: str,
    output_path: str,
    *,
    layer: str | None = None,
    min_zoom: int | None = None,
    max_zoom: int | None = None,
    bbox: str | None = None,
    where: str | None = None,
    include_cols: str | None = None,
    precision: int = 6,
    verbose: bool = False,
    profile: str | None = None,
    src_crs: str | None = None,
    attribution: str | None = None,
) -> None:
    """
    Create PMTiles using gpio streaming + tippecanoe subprocess.

    Orchestrates subprocesses to:
    1. Reproject if needed (gpio convert reproject)
    2. Filter/transform if needed (gpio extract)
    3. Stream GeoJSON from GeoParquet (gpio convert geojson)
    4. Generate PMTiles using tippecanoe

    Args:
        input_path: Path to input GeoParquet file
        output_path: Path for output PMTiles file
        layer: Layer name in PMTiles (defaults to output filename)
        min_zoom: Minimum zoom level (optional)
        max_zoom: Maximum zoom level (optional, auto-detected if not set)
        bbox: Bounding box filter as "minx,miny,maxx,maxy"
        where: SQL WHERE clause for filtering
        include_cols: Comma-separated list of columns to include
        precision: Coordinate decimal precision (default: 6)
        verbose: Enable verbose output
        profile: AWS profile name for S3 files
        src_crs: Source CRS for reprojection to WGS84
        attribution: Attribution HTML for the tiles

    Raises:
        TippecanoeNotFoundError: If tippecanoe is not in PATH
        ValueError: If paths contain shell metacharacters
        RuntimeError: If any subprocess fails
    """
    _validate_path(input_path)
    _validate_path(output_path)

    if not _check_tippecanoe():
        raise TippecanoeNotFoundError()

    gpio_commands = _build_gpio_commands(
        input_path, bbox, where, include_cols, precision, verbose, profile, src_crs
    )

    tippecanoe_cmd = _build_tippecanoe_command(
        output_path, layer, min_zoom, max_zoom, verbose, attribution
    )

    _run_pipeline(gpio_commands, tippecanoe_cmd, verbose)

    if verbose:
        success(f"Created {output_path}")
