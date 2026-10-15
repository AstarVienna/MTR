"""MTR's lookup tables: DPR keywords, task chains, products, tech and mode routing.

The Pipeline-derived tables come from the EDPS workflow files EDPS itself
runs (``workflow_dir`` in ``~/.edps/application.properties``, else the Pipeline
checkout), so they follow the installed ref. Without a checkout (a fresh GUI,
pipx with a container runner) they come from the snapshot bundled with MTR,
which CI keeps in sync with Pipeline main. The constants below are MTR's own
choices, which no upstream file states.
"""

from __future__ import annotations

import configparser
import json
import sys
from dataclasses import dataclass
from pathlib import Path

from . import paths, upstream

#: The workflow MTR always runs; it imports every sub-workflow.
UMBRELLA_WORKFLOW = "metis.metis_wkf"

# Sub-workflow for each tech, as written in YAML ``properties.tech`` or the FITS
# ``DPR.TECH``. IMAGE,LM appears in five workflows upstream, and the RAVC / APP /
# PUP techs are Simulations labels, so this mapping is a choice, not data.
# fmt: off
TECH_TO_WORKFLOW = {
    "IMAGE,LM": "metis.metis_lm_img_wkf",
    "IMAGE,N":  "metis.metis_n_img_wkf",
    "LMS":      "metis.metis_ifu_wkf",    # legacy, warned about in scan_yaml_inputs
    "IFU":      "metis.metis_ifu_wkf",
    "LSS,LM":   "metis.metis_lm_lss_wkf",
    "LSS,N":    "metis.metis_n_lss_wkf",
    "RAVC,LM":  "metis.metis_lm_ravc_wkf",
    "RAVC,IFU": "metis.metis_ifu_wkf",
    "APP,LM":   "metis.metis_lm_app_wkf",
    "PUP,LM":   "metis.metis_pupil_imaging_wkf",
    "PUP,N":    "metis.metis_pupil_imaging_wkf",
}
# fmt: on

#: YAML techs METIS_Simulations rewrites before writing DPR.TECH
#: (``scopesimWrapper.updateHeaders``); the contract test checks this.
YAML_TECH_TO_DPR_TECH = {"RAVC,LM": "IMAGE,LM", "APP,LM": "IMAGE,LM", "RAVC,IFU": "IFU"}


def normalize_tech(tech: str) -> str:
    """Upper-case, trimmed, no spaces around commas: the form of the keys above."""
    return ",".join(part.strip() for part in tech.upper().split(","))


def normalize_mode(mode: str) -> str:
    return mode.strip().lower()


@dataclass(frozen=True)
class Tables:
    source: str
    dpr_to_tag: dict[tuple[str | None, str, str], str]
    #: workflow -> ``[(task, raw main-input tag, meta target)]``, calibration-plan order
    chains: dict[str, list[tuple[str, str, str | None]]]
    #: calibration task -> PRO.CATGs downstream tasks consume from it
    task_products: dict[str, tuple[str, ...]]
    #: ScopeSim mode -> DPR.TECH, from the pinned irdb release
    mode_to_tech: dict[str, str]

    def lookup_dpr_tag(self, catg: str | None, typ: str, tech: str) -> str | None:
        """Classification tag for a DPR triple, or None.

        Falls back to the catg-agnostic entry, mirroring the sky rules that do
        not test DPR.CATG.
        """
        return self.dpr_to_tag.get((catg, typ, tech)) or self.dpr_to_tag.get((None, typ, tech))

    def workflow_for_tech(self, tech: str) -> str | None:
        return TECH_TO_WORKFLOW.get(normalize_tech(tech))

    def workflow_for_mode(self, mode: str) -> str | None:
        tech = self.mode_to_tech.get(normalize_mode(mode))
        return TECH_TO_WORKFLOW.get(tech) if tech else None

    def raw_tags(self) -> set[str]:
        return set(self.dpr_to_tag.values())

    def product_tags(self) -> set[str]:
        return {catg for produces in self.task_products.values() for catg in produces}


def _from_dict(data: dict, source: str, irdb: dict) -> Tables:
    return Tables(
        source=source,
        dpr_to_tag={(c, t, tech): tag for c, t, tech, tag in data["dpr_to_tag"]},
        chains={wf: [tuple(row) for row in chain] for wf, chain in data["chains"].items()},
        task_products={task: tuple(p) for task, p in data["task_products"].items()},
        mode_to_tech=irdb["mode_to_tech"],
    )


def _snapshot() -> dict:
    return json.loads(upstream.SNAPSHOT_PATH.read_text())


def edps_workflow_dir() -> Path | None:
    """``workflow_dir`` from EDPS's configuration: the workflows EDPS really runs."""
    cfg = configparser.ConfigParser(interpolation=None)
    try:
        cfg.read(Path.home() / ".edps" / "application.properties")
        value = cfg.get("application", "workflow_dir", fallback="").strip()
    except (configparser.Error, OSError, UnicodeDecodeError):
        return None
    return Path(value).expanduser() if value else None


def workflow_dir() -> Path | None:
    """The local METIS workflow folder to derive from, or None."""
    for d in (edps_workflow_dir(), paths.pipeline_dir() / "metisp" / "workflows"):
        if d is not None and (d / "metis" / "metis_wkf.py").is_file():
            return d
    return None


_cache: Tables | None = None
_snapshot_only = False


def use_snapshot(only: bool = True) -> None:
    """Ignore local checkouts (docker / podman run the container's own Pipeline)."""
    global _snapshot_only, _cache
    _snapshot_only, _cache = only, None


def get() -> Tables:
    """The tables for this process, loaded on first use."""
    global _cache
    if _cache is None:
        _cache = _load()
    return _cache


def _load() -> Tables:
    snap = _snapshot()
    snap_source = f"bundled snapshot (Pipeline @{snap['pipeline_commit'] or 'unknown'})"
    wf_dir = None if _snapshot_only else workflow_dir()
    if wf_dir is None:
        return _from_dict(snap, snap_source, snap)
    try:
        derived = upstream.derive_pipeline(wf_dir)
    except (OSError, SyntaxError, ValueError, LookupError, AttributeError) as exc:
        print(
            f"Warning: cannot read the Pipeline workflows in {wf_dir} ({exc}); using the {snap_source}.",
            file=sys.stderr,
        )
        return _from_dict(snap, snap_source, snap)
    commit = upstream.git_head(wf_dir)
    return _from_dict(derived, f"{wf_dir}" + (f" @{commit}" if commit else ""), snap)
