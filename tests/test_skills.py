"""Tests for the gpio skills command and skills package."""

import pytest
from click.testing import CliRunner

from geoparquet_io.cli.main import cli
from geoparquet_io.skills import get_skill_content, get_skill_path, list_skills


class TestSkillsPackage:
    """Test the skills package functions."""

    def test_list_skills_returns_geoparquet(self):
        """list_skills() should include 'geoparquet' skill."""
        skills = list_skills()
        assert "geoparquet" in skills

    def test_list_skills_returns_sorted(self):
        """list_skills() should return sorted list."""
        skills = list_skills()
        assert skills == sorted(skills)

    def test_get_skill_path_returns_existing_file(self):
        """get_skill_path() should return path to existing .md file."""
        path = get_skill_path("geoparquet")
        assert path.exists()
        assert path.suffix == ".md"
        assert path.name == "geoparquet.md"

    def test_get_skill_path_raises_for_unknown(self):
        """get_skill_path() should raise FileNotFoundError for unknown skill."""
        with pytest.raises(FileNotFoundError, match="not found"):
            get_skill_path("nonexistent-skill")

    def test_get_skill_content_returns_markdown(self):
        """get_skill_content() should return markdown with expected sections."""
        content = get_skill_content("geoparquet")
        assert "# GeoParquet" in content or "gpio" in content.lower()
        # Should have some CLI examples
        assert "gpio" in content

    def test_get_skill_content_raises_for_unknown(self):
        """get_skill_content() should raise for unknown skill."""
        with pytest.raises(FileNotFoundError, match="not found"):
            get_skill_content("nonexistent-skill")


class TestSkillsCLI:
    """Test the gpio skills CLI command."""

    @pytest.fixture
    def runner(self):
        return CliRunner()

    def test_skills_lists_available(self, runner):
        """gpio skills should list available skills."""
        result = runner.invoke(cli, ["skills"])
        assert result.exit_code == 0
        assert "geoparquet" in result.output
        assert "Available gpio skills" in result.output

    def test_skills_show_outputs_content(self, runner):
        """gpio skills --show should print skill content."""
        result = runner.invoke(cli, ["skills", "--show"])
        assert result.exit_code == 0
        # Should contain markdown content
        assert "gpio" in result.output
        # Should be substantial (the skill file is large)
        assert len(result.output) > 500

    def test_skills_show_with_name(self, runner):
        """gpio skills --show --name geoparquet should work."""
        result = runner.invoke(cli, ["skills", "--show", "--name", "geoparquet"])
        assert result.exit_code == 0
        assert "gpio" in result.output

    def test_skills_show_unknown_name_fails(self, runner):
        """gpio skills --show --name unknown should fail."""
        result = runner.invoke(cli, ["skills", "--show", "--name", "unknown"])
        assert result.exit_code != 0
        assert "not found" in result.output.lower()

    def test_skills_copy_creates_file(self, runner, tmp_path):
        """gpio skills --copy should copy skill to directory."""
        result = runner.invoke(cli, ["skills", "--copy", str(tmp_path)])
        assert result.exit_code == 0
        assert "Copied skill to" in result.output

        # Verify file was created
        copied_file = tmp_path / "geoparquet.md"
        assert copied_file.exists()
        content = copied_file.read_text()
        assert "gpio" in content

    def test_skills_copy_to_nonexistent_dir_fails(self, runner, tmp_path):
        """gpio skills --copy to nonexistent dir should fail."""
        nonexistent = tmp_path / "does-not-exist"
        result = runner.invoke(cli, ["skills", "--copy", str(nonexistent)])
        assert result.exit_code != 0
        assert "Not a directory" in result.output

    def test_skills_copy_with_name(self, runner, tmp_path):
        """gpio skills --copy --name should work."""
        result = runner.invoke(cli, ["skills", "--copy", str(tmp_path), "--name", "geoparquet"])
        assert result.exit_code == 0
        assert (tmp_path / "geoparquet.md").exists()
