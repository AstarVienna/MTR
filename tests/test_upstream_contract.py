"""Contract tests: MTR's bundled snapshot and policy tables against upstream checkouts.

Skipped unless METIS_PIPELINE_DIR / METIS_SIMULATIONS_DIR / METIS_IRDB_DIR point
at checkouts (the weekly CI job sets them). The Pipeline's EDPS workflow files
are parsed with ``ast`` rather than imported, so neither EDPS nor ScopeSim is
needed.
"""

import ast
import importlib.util
import json
from pathlib import Path

import pytest
import yaml
from packaging.version import Version

from metis_test_runner import tables, upstream
from metis_test_runner.installer import Installer

pytestmark = pytest.mark.upstream

EXAMPLES = Path(__file__).resolve().parent.parent / "src" / "metis_test_runner" / "examples"

# Sub-workflows MTR names that metis_wkf.py does not import (upstream gap,
# Pipeline PR #168). When this assertion fails, upstream fixed it: drop the entry.
_NOT_IN_UMBRELLA = {"metis.metis_lm_ravc_wkf", "metis.metis_lm_app_wkf"}

# TECH_TO_WORKFLOW keys whose workflow has no task for that tech, with the reason.
# When the test says one is no longer needed, drop it.
_TECH_EXCEPTIONS = {
    "LMS": "legacy YAML spelling of IFU; Simulations writes it unchanged",
    "PUP,N": "the Pipeline has an N_PUPIL_RAW rule but no task for it yet",
}

_REGENERATE = "regenerate it: python -m metis_test_runner.upstream PIPELINE_DIR IRDB_DIR"


@pytest.fixture(scope="module")
def derived(upstream_pipeline_dir):
    return upstream.derive_pipeline(upstream_pipeline_dir)


@pytest.fixture(scope="module")
def snapshot():
    return json.loads(upstream.SNAPSHOT_PATH.read_text())


@pytest.fixture(scope="module")
def rule_tech(derived):
    """Classification tag -> DPR.TECH for every raw rule."""
    return {tag: tech for _, _, tech, tag in derived["dpr_to_tag"]}


class TestPipelineContract:
    @pytest.mark.parametrize("key", ["dpr_to_tag", "chains", "task_products"])
    def test_snapshot_matches_pipeline(self, derived, snapshot, key):
        assert snapshot[key] == derived[key], f"bundled snapshot is out of date; {_REGENERATE}"

    def test_umbrella_imports_the_sub_workflows(self, upstream_pipeline_dir):
        umbrella = upstream_pipeline_dir / "metisp" / "workflows" / "metis" / "metis_wkf.py"
        imported = {
            f"metis.{node.module}"
            for node in ast.parse(umbrella.read_text()).body
            if isinstance(node, ast.ImportFrom) and node.level == 1
        }
        assert set(tables.TECH_TO_WORKFLOW.values()) - imported == _NOT_IN_UMBRELLA

    def test_every_pipeline_tech_is_routed(self, rule_tech):
        assert sorted(set(rule_tech.values()) - set(tables.TECH_TO_WORKFLOW)) == []

    def test_each_tech_reaches_a_task_in_its_workflow(self, derived, rule_tech):
        unreached = set()
        for tech, wf in tables.TECH_TO_WORKFLOW.items():
            dpr_tech = tables.YAML_TECH_TO_DPR_TECH.get(tech, tech)
            if not any(rule_tech.get(tag) == dpr_tech for _, tag, _ in derived["chains"].get(wf, [])):
                unreached.add(tech)
        assert unreached == set(_TECH_EXCEPTIONS)


def _update_headers_tech_rewrite(sims_dir: Path) -> dict[str, str]:
    """``if tech == "X": header[... "DPR TECH"] = "Y"`` pairs from Simulations' updateHeaders."""
    tree = ast.parse((sims_dir / "metis_simulations" / "scopesimWrapper.py").read_text())
    func = next(n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == "updateHeaders")
    rewrite = {}
    for node in ast.walk(func):
        test = getattr(node, "test", None)
        if not (
            isinstance(node, ast.If)
            and isinstance(test, ast.Compare)
            and isinstance(test.left, ast.Name)
            and test.left.id == "tech"
            and isinstance(test.comparators[0], ast.Constant)
        ):
            continue
        for stmt in node.body:
            target = stmt.targets[0] if isinstance(stmt, ast.Assign) else None
            if (
                isinstance(target, ast.Subscript)
                and isinstance(target.slice, ast.Constant)
                and str(target.slice.value).endswith("DPR TECH")
                and isinstance(stmt.value, ast.Constant)
            ):
                rewrite[test.comparators[0].value] = stmt.value.value
    return rewrite


def _simulations_list(sims_dir: Path, name: str) -> list:
    """Read a module-level list literal from simulationDefinitions.py (which imports ScopeSim)."""
    tree = ast.parse((sims_dir / "metis_simulations" / "simulationDefinitions.py").read_text())
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(getattr(t, "id", None) == name for t in node.targets):
            return ast.literal_eval(node.value)
    pytest.fail(f"{name} not found in simulationDefinitions.py")


class TestSimulationsContract:
    def test_yaml_tech_rewrite_matches_simulations(self, upstream_simulations_dir):
        rewrite = _update_headers_tech_rewrite(upstream_simulations_dir)
        assert rewrite, "found no DPR TECH rewrite in scopesimWrapper.updateHeaders"
        assert {k: v for k, v in rewrite.items() if k != v} == tables.YAML_TECH_TO_DPR_TECH

    @pytest.mark.parametrize("example", sorted(EXAMPLES.glob("*.yaml")), ids=lambda p: p.name)
    def test_bundled_yaml_uses_known_values(self, upstream_simulations_dir, example):
        # typeVals is not checked: upstream it lacks DARK, so it is incomplete.
        tech_vals = set(_simulations_list(upstream_simulations_dir, "techVals"))
        problems = set()
        for block in yaml.safe_load(example.read_text()).values():
            props = block["properties"]
            if props["tech"] not in tech_vals:
                problems.add(f"tech {props['tech']!r}")
            if "ndfilter_name" in props:
                problems.add("'ndfilter_name' (renamed to 'nd_filter_name')")
        assert sorted(problems) == []

    @pytest.mark.parametrize("example", sorted(EXAMPLES.glob("*.csv")), ids=lambda p: p.name)
    def test_bundled_csv_loads(self, upstream_simulations_dir, example):
        # csvParser imports only the standard library and PyYAML.
        spec = importlib.util.spec_from_file_location(
            "_mtr_csvparser", upstream_simulations_dir / "metis_simulations" / "csvParser.py"
        )
        csv_parser = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(csv_parser)
        assert csv_parser.loadCSV(str(example))


class TestIrdbContract:
    def test_snapshot_modes_match_the_pinned_release(self, upstream_irdb_dir, snapshot):
        assert snapshot["mode_to_tech"] == upstream.derive_irdb_modes(upstream_irdb_dir), _REGENERATE

    def test_pinned_scopesim_loads_the_pinned_release(self, upstream_irdb_dir):
        default = yaml.safe_load_all((upstream_irdb_dir / "METIS" / "default.yaml").read_text())
        needs = next(d["needs_scopesim"] for d in default if isinstance(d, dict) and "needs_scopesim" in d)
        pin = next(r for r in Installer.PIP_REQUIREMENTS if r.startswith("scopesim=="))
        assert Version(needs.lstrip("v")) <= Version(pin.split("==")[1])
