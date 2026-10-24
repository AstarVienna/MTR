"""Derive MTR's lookup tables from the METIS_Pipeline EDPS workflow files.

The workflow modules import ``edps``, which is not one of MTR's dependencies,
so they are parsed with :mod:`ast` instead of imported. Only module-level
assignments count, which is also all EDPS sees: tasks defined inside
``@subworkflow`` functions are never instantiated upstream.

Run ``python -m metis_test_runner.upstream PIPELINE_DIR IRDB_DIR`` to refresh
the snapshot bundled with MTR (see :mod:`metis_test_runner.tables`).
"""

from __future__ import annotations

import argparse
import ast
import json
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path

import yaml

#: EDPS meta-target constants (``from edps import SCIENCE, ...``) and their values.
_META = {"SCIENCE": "science", "QC1_CALIB": "qc1calib", "QC0": "qc0", "CALCHECKER": "calchecker"}

#: The umbrella workflow imports every sub-workflow; it is not a chain of its own.
_UMBRELLA_STEM = "metis_wkf"

SNAPSHOT_PATH = Path(__file__).with_name("upstream_snapshot.json")


def _chain_calls(node: ast.AST) -> list[tuple[str, ast.Call]]:
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


@dataclass
class _Module:
    defs: dict[str, list[tuple[str, ast.Call]]] = field(default_factory=dict)
    strings: dict[str, str] = field(default_factory=dict)
    star: list[str] = field(default_factory=list)
    named: dict[str, tuple[str, str]] = field(default_factory=dict)
    module_aliases: dict[str, str] = field(default_factory=dict)


def _read_modules(pkg_dir: Path) -> dict[str, _Module]:
    mods = {}
    for py in sorted(pkg_dir.glob("*.py")):
        mod = _Module()
        for node in ast.parse(py.read_text(), filename=str(py)).body:
            if isinstance(node, ast.ImportFrom) and node.level == 1:
                for alias in node.names:
                    if node.module is None:  # from . import metis_keywords as metis_kwd
                        mod.module_aliases[alias.asname or alias.name] = alias.name
                    elif alias.name == "*":
                        mod.star.append(node.module)
                    else:
                        mod.named[alias.asname or alias.name] = (node.module, alias.name)
            elif (
                isinstance(node, ast.Assign)
                and len(node.targets) == 1
                and isinstance(node.targets[0], ast.Name)
            ):
                name = node.targets[0].id
                if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
                    mod.strings[name] = node.value.value
                elif calls := _chain_calls(node.value):
                    mod.defs[name] = calls  # a later assignment wins, as at runtime
        mods[py.stem] = mod
    return mods


@dataclass
class _Task:
    name: str
    main_tags: list[str]
    raw: bool
    meta: set[str]
    parents: set[tuple[str, str]]


class _Package:
    """Resolves names across the workflow package the way Python's imports would."""

    def __init__(self, pkg_dir: Path):
        self.mods = _read_modules(pkg_dir)
        self._ns: dict[str, dict[str, tuple[str, str]]] = {}
        self.rules: dict[tuple[str, str], tuple[str, dict[str, str]]] = {}
        self.tasks: dict[tuple[str, str], _Task] = {}
        # (task ref) -> product tags its consumers ask for
        self.products: dict[tuple[str, str], set[str]] = {}

    def namespace(self, stem: str, _seen: frozenset = frozenset()) -> dict[str, tuple[str, str]]:
        """``{name: (home module, variable)}`` for every call-chain object visible in *stem*."""
        if stem in self._ns:
            return self._ns[stem]
        if stem in _seen or stem not in self.mods:
            return {}
        mod, seen = self.mods[stem], _seen | {stem}
        ns = {}
        for src in mod.star:
            ns.update({k: v for k, v in self.namespace(src, seen).items() if not k.startswith("_")})
        for name, (src, orig) in mod.named.items():
            if ref := self.namespace(src, seen).get(orig):
                ns[name] = ref
        ns.update({name: (stem, name) for name in mod.defs})
        self._ns[stem] = ns
        return ns

    def _resolve(self, stem: str, node: ast.AST) -> tuple[str, str] | None:
        return self.namespace(stem).get(node.id) if isinstance(node, ast.Name) else None

    def _calls(self, ref: tuple[str, str]) -> list[tuple[str, ast.Call]]:
        return self.mods[ref[0]].defs[ref[1]]

    def _kind(self, ref: tuple[str, str]) -> str:
        return self._calls(ref)[0][0]

    def _keyword(self, stem: str, node: ast.AST) -> str | None:
        """``metis_kwd.dpr_catg`` or ``"dpr.catg"`` -> ``"dpr.catg"``."""
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            return node.value
        if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
            target = self.mods[stem].module_aliases.get(node.value.id)
            if target in self.mods:
                return self.mods[target].strings.get(node.attr)
        return None

    def rule(self, ref: tuple[str, str]) -> tuple[str, dict[str, str]]:
        if ref not in self.rules:
            args = self._calls(ref)[0][1].args
            keywords = {}
            if len(args) > 1 and isinstance(args[1], ast.Dict):
                for k, v in zip(args[1].keys, args[1].values, strict=True):
                    key = self._keyword(ref[0], k) if k is not None else None
                    if key and isinstance(v, ast.Constant) and isinstance(v.value, str):
                        keywords[key] = v.value
            self.rules[ref] = (args[0].value, keywords)
        return self.rules[ref]

    def _rule_tags(self, stem: str, nodes) -> list[str]:
        tags = []
        for node in nodes:
            ref = self._resolve(stem, node)
            if ref and self._kind(ref) == "classification_rule":
                tags.append(self.rule(ref)[0])
        return tags

    def _source_tags(self, ref: tuple[str, str]) -> list[str]:
        return [
            tag
            for name, call in self._calls(ref)
            if name == "with_classification_rule"
            for tag in self._rule_tags(ref[0], call.args)
        ]

    @staticmethod
    def _categories(call: ast.Call) -> list[ast.AST]:
        node = (
            call.args[1]
            if len(call.args) > 1
            else next((k.value for k in call.keywords if k.arg == "categories"), None)
        )
        return node.elts if isinstance(node, ast.List) else []

    def task(self, ref: tuple[str, str]) -> _Task:
        if ref in self.tasks:
            return self.tasks[ref]
        stem, calls = ref[0], self._calls(ref)
        task = _Task(name=calls[0][1].args[0].value, main_tags=[], raw=False, meta=set(), parents=set())
        self.tasks[ref] = task
        for name, call in calls:
            if name == "with_meta_targets" and call.args and isinstance(call.args[0], ast.List):
                for e in call.args[0].elts:
                    if isinstance(e, ast.Name):
                        task.meta.add(_META.get(e.id, e.id.lower()))
                    elif isinstance(e, ast.Constant):
                        task.meta.add(e.value)
            elif name in ("with_main_input", "with_associated_input") and call.args:
                src = self._resolve(stem, call.args[0])
                if src is None:
                    continue
                kind = self._kind(src)
                if kind == "data_source" and name == "with_main_input":
                    task.main_tags, task.raw = self._source_tags(src), True
                elif kind == "task":
                    task.parents.add(src)
                    self.task(src)
                    wanted = self._rule_tags(stem, self._categories(call))
                    self.products.setdefault(src, set()).update(wanted)
                    if name == "with_main_input":
                        task.main_tags = wanted
        return task

    def scan(self) -> None:
        """Visit every module-level rule and task, so products see all consumers."""
        for stem, mod in self.mods.items():
            for var in mod.defs:
                kind = self._kind((stem, var))
                if kind == "task":
                    self.task((stem, var))
                elif kind == "classification_rule":
                    self.rule((stem, var))

    def workflow_tasks(self, stem: str) -> set[tuple[str, str]]:
        """Tasks EDPS loads for *stem*: those visible in the module plus their ancestors."""
        members = {ref for ref in self.namespace(stem).values() if self._kind(ref) == "task"}
        todo = list(members)
        while todo:
            for parent in self.tasks[todo.pop()].parents:
                if parent not in members:
                    members.add(parent)
                    todo.append(parent)
        return members


def _chain(pkg: _Package, members: set[tuple[str, str]], sci_tags: set[str]) -> list[list]:
    """Raw-input tasks in calibration-plan order: ``[task, main tag, meta target]``."""
    depth: dict[tuple[str, str], int] = {}

    def dep(ref):
        if ref not in depth:
            depth[ref] = 1 + max((dep(p) for p in pkg.tasks[ref].parents & members), default=0)
        return depth[ref]

    def meta(task):
        if "science" in task.meta:
            return "science"
        if "qc1calib" in task.meta:
            return "qc1calib"
        # An ungated task reducing a SCIENCE raw (metis_ifu_sci_reduce) only
        # runs under -m science in practice.
        return "science" if set(task.main_tags) & sci_tags else None

    rows = []
    for ref in members:
        task = pkg.tasks[ref]
        if not task.raw:
            continue
        m = meta(task)
        rows += [(dep(ref), m == "science", task.name, tag, m) for tag in task.main_tags]
    return [[name, tag, m] for *_, name, tag, m in sorted(rows)]


def derive_pipeline(pipeline_dir: Path) -> dict:
    """Return the Pipeline-derived tables for the checkout at *pipeline_dir*.

    *pipeline_dir* is the repository root or its ``metisp/workflows`` folder.
    Raises ``ValueError`` when the result looks unusable (an unsupported layout
    or a Pipeline too old to declare its products).
    """
    pipeline_dir = Path(pipeline_dir)
    candidates = [pipeline_dir / "metis", pipeline_dir / "metisp" / "workflows" / "metis"]
    pkg_dir = next((c for c in candidates if (c / f"{_UMBRELLA_STEM}.py").is_file()), None)
    if pkg_dir is None:
        raise ValueError(f"no EDPS workflow package (metis/{_UMBRELLA_STEM}.py) under {pipeline_dir}")
    pkg = _Package(pkg_dir)
    pkg.scan()

    dpr_to_tag = {}
    for tag, kw in sorted(pkg.rules.values(), key=lambda rule: rule[0]):
        if "dpr.type" in kw and "dpr.tech" in kw:
            dpr_to_tag.setdefault((kw.get("dpr.catg"), kw["dpr.type"], kw["dpr.tech"]), tag)
    sci_tags = {tag for (catg, _, _), tag in dpr_to_tag.items() if catg == "SCIENCE"}

    chains = {
        f"{pkg_dir.name}.{stem}": _chain(pkg, pkg.workflow_tasks(stem), sci_tags)
        for stem in sorted(pkg.mods)
        if stem.endswith("_wkf") and stem != _UMBRELLA_STEM
    }
    products_by_name = {pkg.tasks[ref].name: tags for ref, tags in pkg.products.items()}
    task_products = {
        name: sorted(products_by_name[name])
        for chain in chains.values()
        for name, _, meta in chain
        if meta != "science" and products_by_name.get(name)
    }

    if not dpr_to_tag or not task_products or not all(chains.values()):
        raise ValueError(
            f"{pkg_dir}: derived tables are incomplete (Pipeline older than 2026-05 "
            "declares no task products, or the workflow layout changed)"
        )
    return {
        "dpr_to_tag": [[*key, tag] for key, tag in sorted(dpr_to_tag.items(), key=lambda kv: kv[1])],
        "chains": chains,
        "task_products": dict(sorted(task_products.items())),
    }


def derive_irdb_modes(irdb_dir: Path) -> dict[str, str]:
    """``{mode: DPR.TECH}`` from the METIS package's ``default.yaml`` mode list."""
    default = Path(irdb_dir) / "METIS" / "default.yaml"
    modes = {}
    for doc in yaml.safe_load_all(default.read_text()):
        if isinstance(doc, dict) and "mode_yamls" in doc:
            for mode in doc["mode_yamls"]:
                tech = (mode.get("properties") or {}).get("tech")
                if mode.get("name") and tech:
                    modes[mode["name"]] = tech
    if not modes:
        raise ValueError(f"{default}: no mode carries a 'tech' property (irdb older than 2026-09?)")
    return dict(sorted(modes.items()))


def git_head(path: Path) -> str | None:
    """Short commit of the git checkout containing *path*, or None."""
    try:
        out = subprocess.run(
            ["git", "-C", str(path), "rev-parse", "--short=8", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    return out.stdout.strip() if out.returncode == 0 else None


def build_snapshot(pipeline_dir: Path, irdb_dir: Path, irdb_release: str) -> dict:
    return {
        "pipeline_commit": git_head(pipeline_dir),
        "irdb_release": irdb_release,
        **derive_pipeline(pipeline_dir),
        "mode_to_tech": derive_irdb_modes(irdb_dir),
    }


def _tables_only(snapshot: dict) -> dict:
    return {k: v for k, v in snapshot.items() if k != "pipeline_commit"}


def main(argv: list[str] | None = None) -> int:
    from .run_metis import IRDB_RELEASES

    parser = argparse.ArgumentParser(
        prog="python -m metis_test_runner.upstream",
        description="Regenerate (or --check) MTR's bundled snapshot of the upstream tables.",
    )
    parser.add_argument("pipeline_dir", type=Path, help="METIS_Pipeline checkout")
    parser.add_argument(
        "irdb_dir",
        type=Path,
        help=f"directory holding METIS/default.yaml of irdb release {IRDB_RELEASES['METIS']} "
        "(an inst_pkgs folder, or an irdb clone at that tag)",
    )
    parser.add_argument("--check", action="store_true", help="exit 1 if the bundled snapshot is out of date")
    args = parser.parse_args(argv)

    fresh = build_snapshot(args.pipeline_dir, args.irdb_dir, IRDB_RELEASES["METIS"])
    if args.check:
        bundled = json.loads(SNAPSHOT_PATH.read_text())
        if _tables_only(bundled) == _tables_only(fresh):
            print(f"{SNAPSHOT_PATH.name} is up to date")
            return 0
        for key in _tables_only(fresh):
            if bundled.get(key) != fresh[key]:
                print(f"{SNAPSHOT_PATH.name}: {key} differs from upstream", file=sys.stderr)
        print(
            "Regenerate it with: python -m metis_test_runner.upstream PIPELINE_DIR IRDB_DIR", file=sys.stderr
        )
        return 1
    SNAPSHOT_PATH.write_text(json.dumps(fresh, indent=1) + "\n")
    print(f"Wrote {SNAPSHOT_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
