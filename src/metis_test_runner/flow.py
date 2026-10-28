"""The data flow MTR drives, from simulation through the Pipeline to the archive.

Built from :mod:`tables` (the installed Pipeline's workflow files, or the bundled
snapshot), so it follows the code rather than a drawing. No Qt here: the GUI's
data-flow window (:mod:`flow_view`) draws what :func:`build_flow` returns.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

from . import tables

STATIC_ID = "static"
ARCHIVE_ID = "archive"
#: Layout spacing in rows: above an expanded lane (its header), between lanes, between raw frames
LANE_HEAD = 0.85
LANE_GAP = 0.35
RAW_PITCH = 0.6


@dataclass
class Node:
    id: str
    kind: str  # "sim", "static", "raw", "task", "group" (a collapsed lane) or "archive"
    label: str
    tooltip: str
    lane: str | None
    col: int
    row: float = 0.0
    #: Rows a "sim" frame spans around its lane's raw frames; 0 for a plain node
    span: float = 0.0
    badges: list[str] = field(default_factory=list)
    #: Run-tab overlay: "present", "target", "running", "missing" or "idle"
    state: str | None = None


@dataclass
class Edge:
    src: str  # node id, or "lane:<lane>" for the archive's per-lane edges
    dst: str
    kind: str  # "sim", "main", "assoc", "static", "upload" or "fetch"
    tags: list[str]


@dataclass
class Lane:
    id: str
    label: str
    top: float
    rows: float
    expanded: bool | None  # None: cannot collapse


@dataclass
class Flow:
    nodes: dict[str, Node]
    edges: list[Edge]
    lanes: list[Lane]
    source: str
    notes: list[str] = field(default_factory=list)


@dataclass
class RunInput:
    """The Run tab's next run, as far as the overlay needs it."""

    files: list[str]
    fits_dirs: list[str]
    simulate: bool = True
    pipeline: bool = True
    auto_fetch: bool = False


@dataclass
class Overlay:
    present: set[str]  # classification tags the run starts from
    flags: list[str]  # the EDPS target flags run_metis would pass
    running: set[str]  # tasks the targets reach that have input data
    missing: set[str]  # calibration tasks auto-fetch would replace from the archive
    notes: list[str]
    simulate: bool = True


def lane_label(lane: str) -> str:
    """``metis.metis_lm_img_wkf`` -> ``lm_img``."""
    return lane.rsplit(".", 1)[-1].removeprefix("metis_").removesuffix("_wkf")


def lanes() -> list[str]:
    return list(tables.get().graph["lanes"])


# ---------------------------------------------------------------------------
# Overlay: what the Run tab's input would execute
# ---------------------------------------------------------------------------


def _have_astropy() -> bool:
    try:
        import astropy.io.fits  # noqa: F401
    except ImportError:
        return False
    return True


def _running(flags: list[str], present: set[str]) -> set[str]:
    """Tasks EDPS would run for *flags*: the targets and their ancestors that have input data."""
    t = tables.get()
    named = {v for k, v in zip(flags[::2], flags[1::2], strict=True) if k == "-t"}
    metas = {v for k, v in zip(flags[::2], flags[1::2], strict=True) if k == "-m"}
    science = {name for chain in t.chains.values() for name, _, m in chain if m == "science"}
    reached = [
        n
        for n, info in t.graph["tasks"].items()
        if n in named or metas & set(info["meta"]) or ("science" in metas and n in science)
    ]
    parents: dict[str, list[str]] = defaultdict(list)
    main: dict[str, tuple[str, list[str]]] = {}
    for origin, dst, role, tags in t.graph["inputs"]:
        if origin.startswith("task:"):
            parents[dst].append(origin[5:])
        if role == "main":
            main[dst] = (origin, tags)

    needed: set[str] = set()
    while reached:
        n = reached.pop()
        if n not in needed:
            needed.add(n)
            reached += parents[n]

    def has_data(n: str) -> bool:
        origin, tags = main.get(n, ("", []))
        if set(tags) & present:  # the raws, or the parent's products given as input
            return True
        return origin.startswith("task:") and has_data(origin[5:])

    return {n for n in needed if has_data(n)}


def overlay_for(run: RunInput) -> Overlay:
    """Mirror run_metis's input scan and EDPS target choice for *run*."""
    from . import archive, run_metis

    if not run.simulate and not run.pipeline:
        return Overlay(set(), [], set(), set(), ["CSV to YAML translation only: nothing runs."], False)
    notes = []
    yaml_files = [f for f in run.files if Path(f).suffix.lower() in (".yaml", ".yml")]
    n_csv = sum(Path(f).suffix.lower() == ".csv" for f in run.files)
    tags: set[str] = set()
    wfs: set[str] = set()
    has_science = False
    if run.simulate:
        if yaml_files:
            try:
                tags, has_science, wfs = run_metis.scan_yaml_inputs(yaml_files)
            except (OSError, ValueError) as exc:
                notes.append(f"Cannot read the input: {exc}")
        if n_csv:
            notes.append(
                f"{n_csv} CSV input(s) not shown: Simulations converts them, "
                "so their frames are only known after simulating."
            )
        if not run.files:
            notes.append("The Run tab has no input files.")
    elif run.pipeline:
        if not run.fits_dirs:
            notes.append("The Run tab has no pipeline input folders.")
        elif not _have_astropy():
            notes.append("Reading the pipeline input FITS headers needs astropy.")
        else:
            for d in run.fits_dirs:
                found, found_wfs = run_metis.scan_fits_inputs(d)
                tags |= found
                wfs |= found_wfs
            has_science = run_metis.derive_has_science(tags, wfs)
    if not run.pipeline:
        notes.append("Simulate only: the pipeline does not run.")
        return Overlay(tags, [], set(), set(), notes, run.simulate)

    flags = run_metis.infer_edps_targets_for_workflows(tags, has_science, wfs)
    missing = [
        (task, catg)
        for wf in sorted(wfs)
        for task, catg in archive.identify_missing_calibrations(wf, tags, has_science)
    ]
    if flags:
        notes.append("EDPS target: " + " ".join(flags))
    elif tags:
        notes.append("No EDPS target inferred; EDPS would use the workflow default.")
    if missing:
        catgs = ", ".join(sorted({c for _, c in missing}))
        how = (
            "auto-fetch will try the archive" if run.auto_fetch else "add them as input or enable auto-fetch"
        )
        notes.append(f"Missing master calibrations: {catgs} ({how}).")
    return Overlay(tags, flags, _running(flags, tags), {t for t, _ in missing}, notes, run.simulate)


# ---------------------------------------------------------------------------
# Graph and layout
# ---------------------------------------------------------------------------


def _place(ids: list[str], desired: dict[str, float], nodes: dict[str, Node], pitch: float = 1.0) -> None:
    """Rows as close to *desired* as possible, at least *pitch* apart."""
    y = float("-inf")
    for nid in sorted(ids, key=lambda i: (desired[i], i)):
        y = max(desired[nid], y + pitch)
        nodes[nid].row = y


def _mean(values: list[float], default: float = 0.0) -> float:
    return sum(values) / len(values) if values else default


def _count(n: int, noun: str) -> str:
    return f"{n} {noun}{'' if n == 1 else 's'}"


def _short(task: str, lane: str) -> str:
    """``metis_ifu_dark`` in the ifu lane -> ``dark``: the lane header already names it."""
    return task.removeprefix("metis_").removeprefix(f"{lane_label(lane)}_")


def build_flow(
    expanded: set[str] | None = None, archive_installed: bool = False, overlay: Overlay | None = None
) -> Flow:
    """The flow in one lane per sub-workflow and columns by dependency depth.

    Lanes in *expanded* (all if None) show their tasks; the others collapse to
    one box. A task belongs to the lane of the module defining it.
    Columns: simulated raw frames, Pipeline tasks, archive.
    """
    t = tables.get()
    g = t.graph
    all_lanes = list(g["lanes"])
    expanded = set(all_lanes) if expanded is None else expanded
    home = {n: info["lanes"][0] for n, info in g["tasks"].items()}
    raw_tags = t.raw_tags()
    tech_of = {tag: tech for (_, _, tech), tag in t.dpr_to_tag.items()}
    modes_of: dict[str, list[str]] = defaultdict(list)
    for mode, tech in t.mode_to_tech.items():
        modes_of[tech].append(mode)

    def unit(task: str) -> str:
        """The node *task* is drawn as: itself, or its collapsed lane."""
        return f"task:{task}" if home[task] in expanded else f"group:{home[task]}"

    nodes: dict[str, Node] = {}
    edges: dict[tuple[str, str], Edge] = {}

    def link(src: str, dst: str, kind: str, tags: list[str]) -> None:
        if (src, dst) not in edges:
            edges[src, dst] = Edge(src, dst, kind, list(tags))
            return
        e = edges[src, dst]
        e.kind = "main" if kind == "main" else e.kind
        e.tags += [x for x in tags if x not in e.tags]

    task_inputs: dict[str, list[str]] = defaultdict(list)
    statics: dict[str, list[str]] = defaultdict(list)
    consumers: dict[str, list[str]] = defaultdict(list)
    raw_inputs = []
    for origin, dst, role, tags in g["inputs"]:
        kind, name = origin.split(":", 1)
        if kind == "task":
            if unit(name) != unit(dst):
                link(unit(name), unit(dst), role, tags)
            task_inputs[dst].append(f"{role:<6} {', '.join(tags)}  ← {name}")
        elif set(g["sources"][name]) & raw_tags:
            consumers[name].append(dst)
            raw_inputs.append((name, dst, role, tags))
            task_inputs[dst].append(f"{role:<6} {', '.join(tags)}")
        else:
            link(STATIC_ID, unit(dst), "static", tags)
            statics[dst] += [x for x in tags if x not in statics[dst]]
            task_inputs[dst].append(f"static {', '.join(tags)}")

    # Raw frames sit in their first expanded consumer's lane, inside its simulations frame;
    # a collapsed lane gets a plain simulations node instead.
    for name, users in consumers.items():
        shown_in = [home[u] for u in users if home[u] in expanded]
        if not shown_in:
            continue
        tags = g["sources"][name]
        ln = min(shown_in, key=all_lanes.index)
        rid = f"raw:{name}"
        nodes[rid] = Node(
            rid,
            "raw",
            ", ".join(tags),
            f"{', '.join(tags)}  (data source {name})\nused by: {', '.join(sorted(set(users)))}",
            ln,
            0,
        )
        link(f"sim:{ln}", rid, "sim", tags)
    for name, dst, role, tags in raw_inputs:
        if home[dst] in expanded:
            link(f"raw:{name}", f"task:{dst}", role, tags)
        else:
            link(f"sim:{home[dst]}", f"group:{home[dst]}", "sim", tags)

    sim_tags: dict[str, list[str]] = defaultdict(list)
    for (src, _), e in edges.items():
        if src.startswith("sim:"):
            sim_tags[src] += [x for x in e.tags if x not in sim_tags[src]]
    for sid, tags in sim_tags.items():
        techs = sorted({tech_of.get(tag, "?") for tag in tags})
        modes = [f"  {tech}: {', '.join(modes_of.get(tech, [])) or 'no irdb mode'}" for tech in techs]
        nodes[sid] = Node(
            sid,
            "sim",
            f"Simulations\n{', '.join(techs)}",
            "METIS_Simulations (ScopeSim) writes raw FITS whose\n"
            "DPR CATG/TYPE/TECH the Pipeline classifies.\n"
            "ScopeSim modes per DPR.TECH:\n" + "\n".join(modes) + f"\nRaw frames: {', '.join(sorted(tags))}",
            sid[4:],
            0,
        )

    for n, info in g["tasks"].items():
        if home[n] not in expanded:
            continue
        nid = f"task:{n}"
        products = t.task_products.get(n, ())
        tip = [n, f"recipe: {info['recipe'] or '-'}"]
        if info["meta"]:
            tip.append(f"meta targets: {', '.join(info['meta'])}")
        tip.append(f"sub-workflows: {', '.join(lane_label(ln) for ln in info['lanes'])}")
        tip += ["inputs:", *(f"  {line}" for line in task_inputs[n])]
        if products:
            tip.append(f"products used downstream: {', '.join(products)}")
        badges = list(info["meta"])
        if statics[n]:
            badges.append(f"+{len(statics[n])} static")
        nodes[nid] = Node(nid, "task", _short(n, home[n]), "\n".join(tip), home[n], 0, badges=badges)

    for ln in all_lanes:
        members = sorted(n for n in g["tasks"] if home[n] == ln)
        if ln in expanded or not members:
            continue
        n_raw = sum(any(home[u] == ln for u in users) for users in consumers.values())
        nodes[f"group:{ln}"] = Node(
            f"group:{ln}",
            "group",
            f"{lane_label(ln)}\n{_count(len(members), 'task')} · {_count(n_raw, 'raw type')}",
            "Click to show its tasks.\n" + "\n".join(f"  {m}" for m in members),
            ln,
            0,
        )

    # Tasks and collapsed lanes, one column per dependency depth
    parents: dict[str, set[str]] = defaultdict(set)
    for src, dst in edges:
        if src.startswith(("task:", "group:")) and dst.startswith(("task:", "group:")):
            parents[dst].add(src)
    depth: dict[str, int] = {}

    def dep(nid: str, seen: frozenset[str] = frozenset()) -> int:
        if nid in seen:  # collapsed lanes feeding each other
            return 0
        if nid not in depth:
            depth[nid] = 1 + max((dep(p, seen | {nid}) for p in parents[nid]), default=-1)
        return depth[nid]

    for nid, node in nodes.items():
        if node.kind in ("task", "group"):
            node.col = 1 + dep(nid)

    if statics:
        every = sorted({x for tags in statics.values() for x in tags})
        nodes[STATIC_ID] = Node(
            STATIC_ID,
            "static",
            "Static calibrations\nSimulations --static",
            "Static calibration prototypes, generated once and cached:\n" + "\n".join(every),
            None,
            0,
        )

    if archive_installed:
        nodes[ARCHIVE_ID] = Node(
            ARCHIVE_ID,
            "archive",
            "Archive\nMetisWISE",
            "Upload: Archive tab, raw frames (DPR) and products (PRO.CATG).\n"
            "Download: --auto-fetch-calibrations puts missing masters\n"
            "into the pipeline input.",
            None,
            max(n.col for n in nodes.values()) + 1,
        )
        for ln in all_lanes:
            masters = [p for n in g["tasks"] if home[n] == ln for p in t.task_products.get(n, ())]
            link(f"lane:{ln}", ARCHIVE_ID, "upload", [])
            if masters:
                link(ARCHIVE_ID, f"lane:{ln}", "fetch", sorted(set(masters)))

    # Rows: an expanded lane's tasks column by column, then its raw frames beside their consumers
    out_lanes = []
    top = 0.0
    if STATIC_ID in nodes:
        out_lanes.append(Lane(STATIC_ID, "static calibrations", top, 1, None))
        top += 1 + LANE_GAP
    for ln in all_lanes:
        members = [nid for nid, node in nodes.items() if node.lane == ln]
        if not members:
            continue
        if ln in expanded:
            top += LANE_HEAD
            rank = {name: i for i, (name, _, _) in enumerate(t.chains.get(ln, []))}
            task_ids = [m for m in members if nodes[m].kind == "task"]
            for col in sorted({nodes[m].col for m in task_ids}):
                ids = [m for m in task_ids if nodes[m].col == col]
                desired = {m: _mean([nodes[p].row for p in parents[m] if nodes[p].lane == ln]) for m in ids}
                roots = [m for m in ids if not any(nodes[p].lane == ln for p in parents[m])]
                for i, m in enumerate(sorted(roots, key=lambda m: (rank.get(m[5:], len(rank)), m))):
                    desired[m] = float(i)
                _place(ids, desired, nodes)
            raws = [m for m in members if nodes[m].kind == "raw"]
            desired = {
                m: _mean([nodes[dst].row for (src, dst) in edges if src == m and nodes[dst].lane == ln])
                for m in raws
            }
            _place(raws, desired, nodes, RAW_PITCH)
            if raws:
                sim = nodes[f"sim:{ln}"]
                sim.row = min(nodes[m].row for m in raws)
                sim.span = max(nodes[m].row for m in raws) - sim.row + 1
            rows = max(nodes[m].row + max(nodes[m].span, 1) for m in members)
        else:
            rows = 1.0
        for m in members:
            nodes[m].row += top
        out_lanes.append(Lane(ln, lane_label(ln), top, rows, ln in expanded))
        top += rows + LANE_GAP
    if ARCHIVE_ID in nodes:
        nodes[ARCHIVE_ID].row = max(top - LANE_GAP - 1, 0) / 2

    notes = []
    if overlay is not None:
        notes = list(overlay.notes)
        named = {v for k, v in zip(overlay.flags[::2], overlay.flags[1::2], strict=True) if k == "-t"}
        for node in nodes.values():
            if node.kind in ("task", "group"):
                names = {node.id[5:]} if node.kind == "task" else {n for n in home if home[n] == node.lane}
                if names & overlay.running:
                    node.state = "target" if names & overlay.running & named else "running"
                else:
                    node.state = "missing" if names & overlay.missing else "idle"
            elif node.kind in ("raw", "sim"):
                tags = set(g["sources"][node.id[4:]]) if node.kind == "raw" else set(sim_tags[node.id])
                live = overlay.simulate or node.kind == "raw"
                node.state = "present" if live and tags & overlay.present else "idle"

    return Flow(nodes, list(edges.values()), out_lanes, t.source, notes)
