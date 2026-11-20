"""Find the ADARI report images of a run; they stay where EDPS writes them, in its job folders.

EDPS runs ``genreport`` in ``<base_dir>/<INSTRUMENT>/<task>/<job id>/<report>/`` and lists the
images in ``output.json`` there. A run's jobs are those whose ``input.sof`` names a file under one
of the run's input roots, or a product of another of the run's jobs.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

#: Written into the run's output root after EDPS finishes.
INDEX_NAME = "reports.json"


@dataclass(frozen=True)
class Panel:
    path: Path
    description: str
    prodcatg: str


@dataclass(frozen=True)
class Report:
    task: str
    job_dir: Path
    name: str
    panels: tuple[Panel, ...]
    #: genreport's stderr tail when it produced no output.json, else ""
    error: str = ""


def _sof_paths(sof: Path) -> list[Path]:
    try:
        lines = sof.read_text().splitlines()
    except OSError:
        return []
    return [Path(line.split()[0]) for line in lines if line.strip() and not line.lstrip().startswith("#")]


def run_jobs(base_dir: Path, roots: list[Path]) -> list[Path]:
    """EDPS job folders under *base_dir* that belong to the run whose inputs live under *roots*."""
    jobs = {sof.parent: _sof_paths(sof) for sof in base_dir.glob("*/*/*/input.sof")}
    member: set[Path] = set()
    changed = True
    while changed:  # products of member jobs make their consumers members too
        changed = False
        for job, inputs in jobs.items():
            if job not in member and any(
                p.parent in member or any(p.is_relative_to(r) for r in roots) for p in inputs
            ):
                member.add(job)
                changed = True
    return sorted(member, key=lambda j: (j / "input.sof").stat().st_mtime)


def write_index(output_root: Path, base_dir: Path, roots: list[Path]) -> list[Path]:
    """Record the run's job folders in *output_root*; returns them."""
    jobs = run_jobs(base_dir, roots)
    data = {"base_dir": str(base_dir), "jobs": [str(j) for j in jobs]}
    (output_root / INDEX_NAME).write_text(json.dumps(data, indent=2) + "\n")
    return jobs


def job_reports(job: Path) -> list[Report]:
    reports = []
    for d in sorted(p for p in job.iterdir() if p.is_dir() and (p / "genreport.stdout").exists()):
        out = d / "output.json"
        try:
            entries = json.loads(out.read_text())
        except (OSError, ValueError):
            try:
                error = (d / "genreport.stderr").read_text(errors="replace").strip()[-2000:]
            except OSError:
                error = ""
            reports.append(
                Report(job.parent.name, job, d.name, (), error or "genreport wrote no output.json")
            )
            continue
        # The recorded path is absolute at generation time; the image sits next to output.json.
        panels = tuple(
            Panel(d / Path(e["filename"]).name, e.get("description", ""), e.get("prodcatg") or "")
            for e in entries
            if "filename" in e
        )
        reports.append(Report(job.parent.name, job, d.name, panels))
    return reports


def load_run(output_root: Path, base_dir: Path | None = None) -> list[Report]:
    """Reports of the run in *output_root*, from its index or, lacking one, by scanning *base_dir*."""
    index = output_root / INDEX_NAME
    try:
        jobs = [Path(j) for j in json.loads(index.read_text())["jobs"]]
    except (OSError, ValueError, KeyError, TypeError):
        jobs = run_jobs(base_dir, [output_root]) if base_dir is not None and base_dir.is_dir() else []
    return [r for job in jobs if job.is_dir() for r in job_reports(job)]


def list_runs(output_dir: Path) -> list[Path]:
    """Run folders (holding a ``pipeline/`` folder) in *output_dir*, newest first."""
    if not output_dir.is_dir():
        return []
    runs = [d for d in output_dir.iterdir() if (d / "pipeline").is_dir()]
    return sorted(runs, key=lambda d: d.stat().st_mtime, reverse=True)
