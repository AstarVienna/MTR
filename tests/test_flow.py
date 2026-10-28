"""The data-flow graph (flow.py), built from the bundled snapshot."""

from itertools import combinations
from pathlib import Path

import pytest
import yaml

from metis_test_runner import flow, tables

EXAMPLES = Path(__file__).resolve().parent.parent / "src" / "metis_test_runner" / "examples"
IFU = "metis.metis_ifu_wkf"
LM_IMG = "metis.metis_lm_img_wkf"
PUPIL = "metis.metis_pupil_imaging_wkf"


@pytest.fixture
def dark_only(tmp_path):
    """small_test.yaml without its DETLIN frames: the lingain masters are missing."""
    blocks = yaml.safe_load((EXAMPLES / "small_test.yaml").read_text())
    path = tmp_path / "dark_only.yaml"
    path.write_text(yaml.safe_dump({k: b for k, b in blocks.items() if b["properties"]["type"] == "DARK"}))
    return str(path)


def _ids(f, kind):
    return {nid for nid, node in f.nodes.items() if node.kind == kind}


class TestBuildFlow:
    def test_every_umbrella_task_and_link(self):
        f = flow.build_flow()
        graph = tables.get().graph
        assert _ids(f, "task") == {f"task:{n}" for n in graph["tasks"]}
        task_links = {(o, f"task:{d}") for o, d, _, _ in graph["inputs"] if o.startswith("task:")}
        assert {(e.src, e.dst) for e in f.edges if e.src.startswith("task:")} == task_links

    def test_raw_frames_come_from_simulations_and_statics_from_one_node(self):
        f = flow.build_flow()
        raw = _ids(f, "raw")
        assert raw and all(nid[4:] in tables.get().graph["sources"] for nid in raw)
        fed = {e.dst for e in f.edges if e.kind == "sim"}
        assert fed == raw
        assert _ids(f, "static") == {flow.STATIC_ID}
        assert all(e.src == flow.STATIC_ID for e in f.edges if e.kind == "static")

    def test_archive_only_when_installed(self):
        assert flow.ARCHIVE_ID not in flow.build_flow().nodes
        f = flow.build_flow(archive_installed=True)
        assert flow.ARCHIVE_ID in f.nodes
        uploads = {e.src for e in f.edges if e.kind == "upload"}
        assert uploads == {f"lane:{ln}" for ln in flow.lanes()}
        fetch = {e.dst: e.tags for e in f.edges if e.kind == "fetch"}
        assert "LINEARITY_IFU" in fetch[f"lane:{IFU}"]

    def test_collapsed_lanes_are_one_box_each(self):
        f = flow.build_flow(set())
        assert _ids(f, "task") == _ids(f, "raw") == set()
        assert _ids(f, "group") == {f"group:{ln}" for ln in flow.lanes()}
        links = {(e.src, e.dst) for e in f.edges}
        # pupil imaging reuses the LM imaging calibrations
        assert (f"group:{LM_IMG}", f"group:{PUPIL}") in links
        assert (f"sim:{IFU}", f"group:{IFU}") in links
        assert all(lane.rows == 1 for lane in f.lanes)
        assert f.nodes[f"group:{IFU}"].label == "ifu\n11 tasks · 9 raw types"

    def test_expanding_one_lane_links_it_to_the_collapsed_rest(self):
        f = flow.build_flow({PUPIL})
        assert _ids(f, "task") == {"task:metis_pupil_imaging"}
        assert (f"group:{LM_IMG}", "task:metis_pupil_imaging") in {(e.src, e.dst) for e in f.edges}
        assert [lane.expanded for lane in f.lanes] == [None, *(ln == PUPIL for ln in flow.lanes())]

    def test_task_labels_drop_the_lane_prefix(self):
        f = flow.build_flow()
        assert f.nodes["task:metis_lm_img_dark"].label == "dark"
        assert f.nodes["task:metis_pupil_imaging"].label == "pupil_imaging"

    def test_simulations_frame_spans_its_raw_frames(self):
        f = flow.build_flow({IFU})
        sim = f.nodes[f"sim:{IFU}"]
        rows = [n.row for n in f.nodes.values() if n.kind == "raw" and n.lane == IFU]
        assert (sim.row, sim.span) == pytest.approx((min(rows), max(rows) - min(rows) + 1))

    @pytest.mark.parametrize("expanded", [None, set(), {PUPIL}, {LM_IMG, IFU}])
    def test_nodes_in_a_column_do_not_overlap(self, expanded):
        f = flow.build_flow(expanded, archive_installed=True)
        for a, b in combinations(f.nodes.values(), 2):
            if a.col == b.col and not (a.span or b.span):
                pitch = flow.RAW_PITCH if a.kind == b.kind == "raw" else 1
                assert abs(a.row - b.row) >= pitch - 1e-9, (a.id, b.id)

    @pytest.mark.parametrize("expanded", [None, set(), {PUPIL}, {LM_IMG, IFU}])
    def test_every_link_points_right(self, expanded):
        f = flow.build_flow(expanded)
        for e in f.edges:
            if e.kind != "static" and not e.dst.startswith("raw:"):
                assert f.nodes[e.src].col < f.nodes[e.dst].col, (e.src, e.dst)

    def test_lane_label(self):
        assert flow.lane_label("metis.metis_lm_img_wkf") == "lm_img"


class TestOverlay:
    def test_small_test_runs_the_ifu_dark_chain(self):
        o = flow.overlay_for(flow.RunInput(files=[str(EXAMPLES / "small_test.yaml")], fits_dirs=[]))
        assert o.flags == ["-t", "metis_ifu_dark"]
        assert o.running == {"metis_ifu_dark", "metis_ifu_lingain"}
        assert o.missing == set()
        f = flow.build_flow({IFU}, overlay=o)
        assert f.nodes["task:metis_ifu_dark"].state == "target"
        assert f.nodes["task:metis_ifu_lingain"].state == "running"
        assert f.nodes["task:metis_ifu_wavecal"].state == "idle"
        assert f.nodes["raw:detlin_ifu_raw"].state == "present"
        assert f.nodes[f"sim:{IFU}"].state == "present"
        collapsed = flow.build_flow(set(), overlay=o)
        assert collapsed.nodes[f"group:{IFU}"].state == "target"
        assert collapsed.nodes[f"group:{LM_IMG}"].state == "idle"

    def test_missing_master_is_what_auto_fetch_would_get(self, dark_only):
        o = flow.overlay_for(flow.RunInput(files=[dark_only], fits_dirs=[], auto_fetch=True))
        assert o.running == {"metis_ifu_dark"}
        assert o.missing == {"metis_ifu_lingain"}
        assert any("auto-fetch will try the archive" in note for note in o.notes)
        assert flow.build_flow({IFU}, overlay=o).nodes["task:metis_ifu_lingain"].state == "missing"

    def test_simulate_only_runs_no_task(self, dark_only):
        o = flow.overlay_for(flow.RunInput(files=[dark_only], fits_dirs=[], pipeline=False))
        assert o.present == {"DARK_IFU_RAW"}
        assert o.running == set()

    def test_csv_and_translate_only_explain_themselves(self):
        csv = flow.overlay_for(flow.RunInput(files=["seq.csv"], fits_dirs=[]))
        assert "CSV" in csv.notes[0]
        translate = flow.overlay_for(
            flow.RunInput(files=["seq.csv"], fits_dirs=[], simulate=False, pipeline=False)
        )
        assert translate.notes == ["CSV to YAML translation only: nothing runs."]

    def test_pipeline_only_without_folders(self):
        o = flow.overlay_for(flow.RunInput(files=[], fits_dirs=[], simulate=False))
        assert o.notes == ["The Run tab has no pipeline input folders."]
        f = flow.build_flow(overlay=o)
        assert all(n.state == "idle" for n in f.nodes.values() if n.kind == "sim")
