"""Contract tests: MTR's bundled snapshot and policy tables against upstream checkouts.

Skipped unless METIS_PIPELINE_DIR / METIS_SIMULATIONS_DIR / METIS_IRDB_DIR point
at checkouts (the weekly CI job sets them). The Pipeline's EDPS workflow files
are parsed with ``ast`` rather than imported, so neither EDPS nor ScopeSim is
needed.
"""

import ast
import importlib.util
import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml
from packaging.version import Version

from metis_test_runner import tables, upstream
from metis_test_runner.installer import Installer
from metis_test_runner.run_metis import _build_sim_script

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


# Runs MTR's simulation script with ScopeSim stubbed out and simulate() logging
# its arguments instead of simulating. Pool workers are forked, so they log to a file.
_SIM_RUNNER = """\
import json, runpy, sys, types
sims, script, log = sys.argv[1:]
sys.path.insert(0, sims)
scopesim = types.ModuleType("scopesim")
scopesim.rc = types.SimpleNamespace(__config__={})
sys.modules["scopesim"] = scopesim
sys.modules["scopesim_templates"] = types.ModuleType("scopesim_templates")
import metis_simulations
wrapper = types.ModuleType("metis_simulations.scopesimWrapper")
def simulate(fname, rcp, small=False, skip_psf=False, progID=None, subDir=None):
    with open(log, "a") as fh:
        fh.write(json.dumps([str(fname), progID, subDir]) + "\\n")
simulate.__module__ = wrapper.__name__  # picklable for Pool
wrapper.simulate = simulate
sys.modules[wrapper.__name__] = wrapper
runpy.run_path(script, run_name="__main__")
"""


class TestSimulationRunContract:
    """MTR's params reach METIS_Simulations' simulate() unchanged."""

    @staticmethod
    def _run(sims_dir, tmp_path, inputs, do_calib=0, write_yaml=False):
        out = tmp_path / f"out-{do_calib}-{write_yaml}"
        script, runner, log = tmp_path / "sim.py", tmp_path / "runner.py", tmp_path / "calls.jsonl"
        script.write_text(
            _build_sim_script(
                out_dir=str(out),
                do_calib=do_calib,
                do_static=False,
                n_cores=1,
                input_list=[str(p) for p in inputs],
                sims_root=str(sims_dir),
                write_yaml=write_yaml,
            )
        )
        runner.write_text(_SIM_RUNNER)
        log.unlink(missing_ok=True)
        cp = subprocess.run(
            [sys.executable, "-B", str(runner), str(sims_dir), str(script), str(log)],
            capture_output=True,
            text=True,
            timeout=300,
            cwd=tmp_path,
        )
        assert cp.returncode == 0, cp.stderr[-3000:]
        return [json.loads(line) for line in log.read_text().splitlines()] if log.exists() else []

    def test_params_reach_simulate(self, upstream_simulations_dir, tmp_path):
        calls = self._run(upstream_simulations_dir, tmp_path, [EXAMPLES / "small_test.yaml"])
        assert calls
        # A non-None upstream --progID default would override MTR's value; MTR would then have to pass -k.
        assert {(prog_id, sub_dir) for _, prog_id, sub_dir in calls} == {(None, "")}

    def test_calib_frames_are_generated(self, upstream_simulations_dir, tmp_path):
        inputs = [EXAMPLES / "small_test.yaml"]
        without = self._run(upstream_simulations_dir, tmp_path, inputs, do_calib=0)
        with_calib = self._run(upstream_simulations_dir, tmp_path, inputs, do_calib=1)
        assert len(with_calib) > len(without)

    def test_csv_to_yaml_does_not_simulate(self, upstream_simulations_dir, tmp_path):
        csv = shutil.copy(EXAMPLES / "small_test_img_lm.csv", tmp_path)
        assert self._run(upstream_simulations_dir, tmp_path, [csv], do_calib=1, write_yaml=True) == []
        assert (tmp_path / "small_test_img_lm.yaml").is_file()


class TestIrdbContract:
    def test_snapshot_modes_match_the_pinned_release(self, upstream_irdb_dir, snapshot):
        assert snapshot["mode_to_tech"] == upstream.derive_irdb_modes(upstream_irdb_dir), _REGENERATE

    def test_pinned_scopesim_loads_the_pinned_release(self, upstream_irdb_dir):
        default = yaml.safe_load_all((upstream_irdb_dir / "METIS" / "default.yaml").read_text())
        needs = next(d["needs_scopesim"] for d in default if isinstance(d, dict) and "needs_scopesim" in d)
        pin = next(r for r in Installer.PIP_REQUIREMENTS if r.startswith("scopesim=="))
        assert Version(needs.lstrip("v")) <= Version(pin.split("==")[1])
