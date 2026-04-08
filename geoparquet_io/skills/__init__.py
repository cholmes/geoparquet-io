"""LLM skills for geoparquet-io.

This package contains skill files that help LLMs (ChatGPT, Claude, etc.)
work effectively with the gpio CLI tool.

Skills are markdown files with structured instructions that teach LLMs
how to use gpio for spatial data workflows.

Usage:
    gpio skills              # List available skills
    gpio skills --show       # Print skill content to stdout
    gpio skills --copy .     # Copy skill to current directory
"""

from importlib.resources import files
from pathlib import Path


def get_skill_path(name: str = "geoparquet") -> Path:
    """Get the path to a bundled skill file.

    Args:
        name: Skill name (without .md extension). Default: "geoparquet"

    Returns:
        Path to the skill markdown file.

    Raises:
        FileNotFoundError: If skill doesn't exist.
    """
    filename = f"{name}.md"

    # Try importlib.resources first (works for installed packages)
    try:
        skill_file = files(__package__) / filename
        if skill_file.is_file():
            return Path(str(skill_file))
    except (TypeError, AttributeError):
        pass

    # Fallback for development (running from source)
    fallback = Path(__file__).parent / filename
    if fallback.is_file():
        return fallback

    raise FileNotFoundError(f"Skill '{name}' not found")


def get_skill_content(name: str = "geoparquet") -> str:
    """Get the content of a bundled skill file.

    Args:
        name: Skill name (without .md extension). Default: "geoparquet"

    Returns:
        Skill content as string.
    """
    skill_path = get_skill_path(name)
    return skill_path.read_text(encoding="utf-8")


def list_skills() -> list[str]:
    """List all available skill names.

    Returns:
        List of skill names (without .md extension).
    """
    # Try importlib.resources first (works for installed packages)
    try:
        skills_dir = files(__package__)
        return sorted(
            p.name[:-3]  # Remove .md extension
            for p in skills_dir.iterdir()
            if p.name.endswith(".md")
        )
    except (TypeError, AttributeError):
        pass

    # Fallback for development (running from source)
    fallback_dir = Path(__file__).parent
    return sorted(p.stem for p in fallback_dir.glob("*.md"))
