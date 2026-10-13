"""Contract tests: MTR's hard-coded tables against real upstream checkouts.

Skipped unless METIS_PIPELINE_DIR / METIS_SIMULATIONS_DIR point at checkouts
(the weekly CI job sets them). The Pipeline's EDPS workflow files are parsed
with ``ast`` rather than imported, so neither EDPS nor ScopeSim is needed.
"""

import ast
import importlib.util
from pathlib import Path

import pytest
import yaml

from metis_test_runner.archive import TASK_PRODUCTS
from metis_test_runner.run_metis import DPR_TO_TAG, WORKFLOW_TASK_CHAIN

pytestmark = pytest.mark.upstream

EXAMPLES = Path(__file__).resolve().parent.parent / "src" / "metis_test_runner" / "examples"

# Pipeline meta-target constant -> MTR's spelling in WORKFLOW_TASK_CHAIN.
_META = {"SCIENCE": "science", "QC1_CALIB": "qc1calib"}

# Deliberate deviations from the parsed workflow, each with its reason.
_META_EXCEPTIONS = {
    # The raw IFU_SCI_RAW gates sci_reduce; the SCIENCE meta target sits on
    # metis_ifu_postprocess, which has no raw main input.
    "metis_ifu_sci_reduce": "science",
}

# Sub-workflows MTR names that metis_wkf.py does not import (upstream gap,
# Pipeline PR #168). When this assertion fails, upstream fixed it: drop the entry.
_NOT_IN_UMBRELLA = {"metis.metis_lm_ravc_wkf", "metis.metis_lm_app_wkf"}


def _chain_calls(node):
    """Flatten ``f(a).g(b).h(c)`` into ``[("f", call), ("g", call), ("h", call)]``."""
    calls = []
    while isinstance(node, ast.Call):
        func = node.func
        if isinstance(func, ast.Attribute):
            calls.append((func.attr, node))
            node = func.value
        elif isinstance(func, ast.Name):
            calls.append((func.id, node))
            break
        else:
            break
    return calls[::-1]


def _kwd_key(node):
    """``metis_kwd.dpr_catg`` or ``"dpr.catg"`` -> ``"dpr_catg"``."""
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value.replace(".", "_")
    return None


def _parse_workflows(wkf_dir: Path) -> dict:
    """Return the rules, data sources, tasks and umbrella imports of the workflow package."""
    assigns = []  # (module stem, variable, call chain)
    umbrella = set()
    for py in sorted(wkf_dir.glob("*.py")):
        tree = ast.parse(py.read_text())
        for node in tree.body:
            if py.stem == "metis_wkf" and isinstance(node, ast.ImportFrom) and node.level == 1:
                umbrella.add(f"metis.{node.module}")
            if (
                isinstance(node, ast.Assign)
                and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)
            ):
                assigns.append((py.stem, node.targets[0].id, _chain_calls(node.value)))

    rules = {}  # variable -> (tag, {keyword: value})
    for _, var, calls in assigns:
        if calls and calls[0][0] == "classification_rule":
            args = calls[0][1].args
            keywords = {}
            if len(args) > 1 and isinstance(args[1], ast.Dict):
                for k, v in zip(args[1].keys, args[1].values, strict=True):
                    if isinstance(v, ast.Constant):
                        keywords[_kwd_key(k)] = v.value
            rules[var] = (args[0].value, keywords)

    sources = {}  # variable -> {tag}
    for _, var, calls in assigns:
        if calls and calls[0][0] == "data_source":
            sources[var] = {
                rules[a.id][0]
                for name, call in calls
                if name == "with_classification_rule"
                for a in call.args
                if isinstance(a, ast.Name) and a.id in rules
            }

    tasks = {}  # task name -> {"module", "main", "meta"}
    for module, _, calls in assigns:
        if not calls or calls[0][0] != "task":
            continue
        main, meta = None, set()
        for name, call in calls:
            if name == "with_main_input" and call.args and isinstance(call.args[0], ast.Name):
                main = sources.get(call.args[0].id)
            elif name == "with_meta_targets" and call.args and isinstance(call.args[0], ast.List):
                meta |= {_META.get(e.id, e.id) for e in call.args[0].elts if isinstance(e, ast.Name)}
        tasks[calls[0][1].args[0].value] = {"module": module, "main": main, "meta": meta}

    return {"rules": dict(rules.values()), "tasks": tasks, "umbrella": umbrella}


@pytest.fixture(scope="module")
def pipeline(upstream_pipeline_dir):
    wkf = upstream_pipeline_dir / "metisp" / "workflows" / "metis"
    if not wkf.is_dir():
        pytest.fail(f"no EDPS workflow package at {wkf}")
    return _parse_workflows(wkf)


class TestPipelineContract:
    def test_workflow_modules_exist(self, upstream_pipeline_dir):
        wkf = upstream_pipeline_dir / "metisp" / "workflows"
        missing = [w for w in WORKFLOW_TASK_CHAIN if not (wkf / (w.replace(".", "/") + ".py")).is_file()]
        assert missing == []

    def test_umbrella_imports_the_sub_workflows(self, pipeline):
        missing = set(WORKFLOW_TASK_CHAIN) - pipeline["umbrella"] - {"metis.metis_wkf"}
        assert missing == _NOT_IN_UMBRELLA

    def test_chain_tasks_exist(self, pipeline):
        missing = sorted(
            f"{wf}: {task}"
            for wf, chain in WORKFLOW_TASK_CHAIN.items()
            for task, _, _ in chain
            if task not in pipeline["tasks"]
        )
        assert missing == []

    def test_chain_main_input_tags_match(self, pipeline):
        wrong = []
        for wf, chain in WORKFLOW_TASK_CHAIN.items():
            for task, tag, _ in chain:
                upstream = pipeline["tasks"].get(task, {}).get("main")
                if upstream is not None and tag not in upstream:
                    wrong.append(f"{task}: MTR {tag}, pipeline {sorted(upstream)}")
        assert wrong == []

    def test_chain_meta_targets_match(self, pipeline):
        wrong = []
        for chain in WORKFLOW_TASK_CHAIN.values():
            for task, _, meta in chain:
                if task not in pipeline["tasks"] or _META_EXCEPTIONS.get(task) == meta:
                    continue
                upstream = pipeline["tasks"][task]["meta"]
                expected = {meta} if meta else set()
                if upstream != expected:
                    wrong.append(f"{task}: MTR {meta}, pipeline {sorted(upstream)}")
        assert wrong == []

    def test_dpr_to_tag_matches_the_rules(self, pipeline):
        wrong = []
        for (catg, typ, tech), tag in DPR_TO_TAG.items():
            kw = pipeline["rules"].get(tag)
            if kw is None:
                wrong.append(f"{tag}: no such classification rule")
            elif (kw.get("dpr_catg"), kw.get("dpr_type"), kw.get("dpr_tech")) != (catg, typ, tech):
                wrong.append(f"{tag}: MTR {(catg, typ, tech)}, pipeline {kw}")
        assert wrong == []

    def test_every_raw_rule_is_in_dpr_to_tag(self, pipeline):
        raw = {tag for tag, kw in pipeline["rules"].items() if "dpr_type" in kw and "dpr_tech" in kw}
        assert sorted(raw - set(DPR_TO_TAG.values())) == []

    def test_task_products_are_classification_tags(self, pipeline):
        wrong = sorted(
            f"{task}: {catg}"
            for task, entry in TASK_PRODUCTS.items()
            for catg in entry.produces
            if catg not in pipeline["rules"]
        )
        assert wrong == []


def _simulations_list(sims_dir: Path, name: str) -> list:
    """Read a module-level list literal from simulationDefinitions.py (which imports ScopeSim)."""
    tree = ast.parse((sims_dir / "metis_simulations" / "simulationDefinitions.py").read_text())
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(getattr(t, "id", None) == name for t in node.targets):
            return ast.literal_eval(node.value)
    pytest.fail(f"{name} not found in simulationDefinitions.py")


class TestSimulationsContract:
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
