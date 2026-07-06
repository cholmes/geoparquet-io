"""Tests for mutation testing configuration."""

import subprocess
import sys
from pathlib import Path
from typing import Any

import pytest
import yaml

if sys.version_info >= (3, 11):
    import tomllib
else:
    import tomli as tomllib

PROJECT_ROOT = Path(__file__).parent.parent


@pytest.fixture()
def pyproject_config() -> dict[str, Any]:
    """Load and return parsed pyproject.toml."""
    pyproject = PROJECT_ROOT / "pyproject.toml"
    with open(pyproject, "rb") as f:
        return tomllib.load(f)


@pytest.fixture()
def mutmut_config(pyproject_config: dict[str, Any]) -> dict[str, Any]:
    """Return the [tool.mutmut] config section."""
    assert "mutmut" in pyproject_config.get("tool", {}), "mutmut config missing from [tool.mutmut]"
    return pyproject_config["tool"]["mutmut"]


@pytest.fixture()
def nightly_workflow() -> dict[str, Any]:
    """Load and return parsed nightly workflow YAML."""
    workflow_path = PROJECT_ROOT / ".github" / "workflows" / "nightly.yml"
    assert workflow_path.exists(), "nightly.yml workflow missing"
    with open(workflow_path) as f:
        return yaml.safe_load(f)


def _get_triggers(workflow: dict[str, Any]) -> dict[str, Any]:
    """Extract the triggers section from a workflow, handling PyYAML's 'on' -> True quirk."""
    # PyYAML parses the YAML key 'on' as boolean True
    triggers = workflow.get("on") if "on" in workflow else workflow.get(True)
    assert triggers is not None, "Missing 'on' triggers section in workflow"
    return triggers


class TestMutmutConfig:
    """Test mutmut is properly configured."""

    @pytest.mark.skipif(
        sys.platform == "win32",
        reason="mutmut CLI has issues on Windows; mutation testing runs on Ubuntu only",
    )
    def test_mutmut_installed(self):
        """Verify mutmut is installed and reports a version."""
        result = subprocess.run(
            [sys.executable, "-m", "mutmut", "--version"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        assert result.returncode == 0, f"mutmut --version failed:\n{result.stdout}\n{result.stderr}"
        # mutmut version output should contain the word "mutmut" or a version number
        output = result.stdout.lower() + result.stderr.lower()
        assert "mutmut" in output or any(c.isdigit() for c in output)

    def test_mutmut_config_exists(self, mutmut_config: dict[str, Any]):
        """Verify mutmut config has source_paths."""
        assert "source_paths" in mutmut_config
        assert any("geoparquet_io" in p for p in mutmut_config["source_paths"])

    def test_mutmut_paths_exist(self, mutmut_config: dict[str, Any]):
        """Verify source_paths points to existing directories."""
        paths = mutmut_config["source_paths"]

        for path in paths:
            full_path = PROJECT_ROOT / path.rstrip("/")
            assert full_path.exists(), f"Mutation path does not exist: {path}"
            assert full_path.is_dir(), f"Mutation path is not a directory: {path}"

    def test_mutmut_tests_dir_exists(self, mutmut_config: dict[str, Any]):
        """Verify pytest_add_cli_args_test_selection points to existing directories."""
        test_paths = mutmut_config.get("pytest_add_cli_args_test_selection", ["tests/"])
        for test_path in test_paths:
            full_path = PROJECT_ROOT / test_path.rstrip("/")
            assert full_path.exists(), f"Tests directory does not exist: {test_path}"
            assert full_path.is_dir(), f"Tests path is not a directory: {test_path}"

    def test_mutmut_excludes_slow_and_network_tests(self, mutmut_config: dict[str, Any]):
        """Verify per-mutant pytest runs are fast-suite only.

        mutmut 3 runs pytest in-process and ignores a `runner` string, so the
        marker filter must live in pytest_add_cli_args. Without it, every
        mutant would run slow/network tests (nondeterministic and far too
        slow).
        """
        args = mutmut_config.get("pytest_add_cli_args", [])
        assert "not slow and not network" in args, (
            "mutmut pytest_add_cli_args must filter to fast tests"
        )
        assert "--no-cov" in args, (
            "mutmut pytest_add_cli_args must disable coverage (per-mutant "
            "overhead, and the global --cov-fail-under gate would misfire)"
        )

    def test_mutmut_also_copies_test_prerequisites(self, mutmut_config: dict[str, Any]):
        """Verify everything tests read relative to the repo root is copied.

        Tests run from mutmut's mutants/ tree; anything missing from
        also_copy fails there while passing in a normal checkout.
        """
        also_copy = mutmut_config.get("also_copy", [])
        for required in ("scripts/", "context/"):
            assert required in also_copy, f"mutmut also_copy must include {required}"
        for entry in also_copy:
            assert (PROJECT_ROOT / entry.rstrip("/")).exists(), (
                f"also_copy entry does not exist: {entry}"
            )

    def test_mutmut_dev_dependency(self, pyproject_config: dict[str, Any]):
        """Verify mutmut is in dev optional-dependencies."""
        dev_deps = (
            pyproject_config.get("project", {}).get("optional-dependencies", {}).get("dev", [])
        )
        has_mutmut = any("mutmut" in dep for dep in dev_deps)
        assert has_mutmut, "mutmut not found in [project.optional-dependencies] dev"

    def test_pyyaml_dev_dependency(self, pyproject_config: dict[str, Any]):
        """Verify pyyaml is in dev dependencies (needed for workflow tests)."""
        dev_deps = (
            pyproject_config.get("project", {}).get("optional-dependencies", {}).get("dev", [])
        )
        has_pyyaml = any("pyyaml" in dep.lower() for dep in dev_deps)
        assert has_pyyaml, "pyyaml not found in [project.optional-dependencies] dev"

    def test_tomli_dev_dependency(self, pyproject_config: dict[str, Any]):
        """Verify tomli is in dev dependencies (Python 3.10 compat)."""
        dev_deps = (
            pyproject_config.get("project", {}).get("optional-dependencies", {}).get("dev", [])
        )
        has_tomli = any("tomli" in dep for dep in dev_deps)
        assert has_tomli, "tomli not found in [project.optional-dependencies] dev"


class TestNightlyWorkflow:
    """Test nightly workflow configuration."""

    def test_nightly_workflow_exists(self):
        """Verify nightly workflow file exists."""
        workflow = PROJECT_ROOT / ".github" / "workflows" / "nightly.yml"
        assert workflow.exists(), "nightly.yml workflow missing"

    def test_nightly_workflow_valid_yaml(self, nightly_workflow: dict[str, Any]):
        """Verify nightly workflow is valid YAML with required top-level keys."""
        _get_triggers(nightly_workflow)
        assert "jobs" in nightly_workflow
        assert "mutation" in nightly_workflow["jobs"]

    def test_nightly_has_schedule_trigger(self, nightly_workflow: dict[str, Any]):
        """Verify nightly workflow has schedule trigger."""
        triggers = _get_triggers(nightly_workflow)
        assert "schedule" in triggers, "Missing schedule trigger"
        # Verify schedule has at least one cron entry
        schedules = triggers["schedule"]
        assert isinstance(schedules, list), "schedule should be a list"
        assert len(schedules) > 0, "schedule should have at least one cron entry"
        assert "cron" in schedules[0], "schedule entry should have a cron field"

    def test_nightly_has_manual_trigger(self, nightly_workflow: dict[str, Any]):
        """Verify nightly workflow can be triggered manually."""
        triggers = _get_triggers(nightly_workflow)
        assert "workflow_dispatch" in triggers, "Missing manual trigger (workflow_dispatch)"

    def test_nightly_mutation_job_has_timeout(self, nightly_workflow: dict[str, Any]):
        """Verify mutation job has a reasonable timeout set."""
        mutation_job = nightly_workflow["jobs"]["mutation"]
        assert "timeout-minutes" in mutation_job, "Missing timeout-minutes on mutation job"
        timeout = mutation_job["timeout-minutes"]
        assert timeout >= 60, f"Timeout {timeout}m too short; should be >= 60 minutes"
        assert timeout <= 360, f"Timeout {timeout}m unreasonably large; should be <= 360 minutes"

    def test_nightly_mutation_job_runs_on(self, nightly_workflow: dict[str, Any]):
        """Verify mutation job specifies a runner."""
        mutation_job = nightly_workflow["jobs"]["mutation"]
        assert "runs-on" in mutation_job, "Missing runs-on in mutation job"

    def test_nightly_mutation_job_has_steps(self, nightly_workflow: dict[str, Any]):
        """Verify mutation job has steps defined."""
        mutation_job = nightly_workflow["jobs"]["mutation"]
        assert "steps" in mutation_job, "Missing steps in mutation job"
        steps = mutation_job["steps"]
        assert len(steps) > 0, "Mutation job should have at least one step"

        # Verify essential steps exist by checking for key actions/commands
        step_contents = " ".join(
            str(step.get("run", "")) + str(step.get("uses", "")) for step in steps
        )
        assert "checkout" in step_contents, "Missing checkout step"
        assert "mutmut" in step_contents, "Missing mutmut run step"

    def test_nightly_has_artifact_upload(self, nightly_workflow: dict[str, Any]):
        """Verify mutation report artifact is uploaded."""
        mutation_job = nightly_workflow["jobs"]["mutation"]
        steps = mutation_job["steps"]
        upload_steps = [s for s in steps if "upload-artifact" in str(s.get("uses", ""))]
        assert len(upload_steps) > 0, "Missing artifact upload step"
