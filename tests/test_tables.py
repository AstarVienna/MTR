"""The workflow parser (upstream.py) and the tables it feeds (tables.py)."""

import json
import textwrap

import pytest

from metis_test_runner import run_metis as rm
from metis_test_runner import tables, upstream


def _write_pkg(root, files):
    pkg = root / "metisp" / "workflows" / "metis"
    pkg.mkdir(parents=True)
    for name, body in files.items():
        (pkg / name).write_text(textwrap.dedent(body))
    return root


_FILES = {
    "metis_keywords.py": """
        dpr_catg = "dpr.catg"
        dpr_type = "dpr.type"
        dpr_tech = "dpr.tech"
    """,
    "metis_classification.py": """
        from edps import classification_rule
        from . import metis_keywords as kwd
        dark_class = classification_rule("DARK_RAW", {kwd.dpr_catg: "CALIB", kwd.dpr_type: "DARK", kwd.dpr_tech: "IFU"})
        flat_class = classification_rule("FLAT_RAW", {"dpr.catg": "CALIB", "dpr.type": "FLAT", "dpr.tech": "IFU"})
        sci_class = classification_rule("SCI_RAW", {kwd.dpr_catg: "SCIENCE", kwd.dpr_type: "OBJECT", kwd.dpr_tech: "IFU"})
        sky_class = classification_rule("SKY_RAW", {kwd.dpr_type: "SKY", kwd.dpr_tech: "IFU"})
        master_dark_class = classification_rule("MASTER_DARK")
        master_flat_class = classification_rule("MASTER_FLAT")
        sci_reduced_class = classification_rule("SCI_REDUCED")
        static_map_class = classification_rule("STATIC_MAP")
    """,
    "metis_datasources.py": """
        from edps import data_source
        from .metis_classification import *
        raw_dark = data_source().with_classification_rule(dark_class).build()
        raw_flat = data_source().with_classification_rule(flat_class).build()
        raw_sci = data_source().with_classification_rule(sci_class).build()
        raw_sky = data_source().with_classification_rule(sky_class).build()
        static_map = data_source().with_classification_rule(static_map_class).build()
    """,
    "metis_x_wkf.py": """
        from edps import task, SCIENCE, QC1_CALIB
        from .metis_datasources import *
        dark = task("x_dark").with_main_input(raw_dark).with_recipe("x_det_dark").build()
        flat = (task("x_flat").with_main_input(raw_flat)
                .with_associated_input(dark, [master_dark_class])
                .with_associated_input(static_map).build())
        sci = (task("x_sci").with_main_input(raw_sci)
               .with_associated_input(flat, [master_flat_class])
               .with_associated_input(dark, [master_dark_class]).build())
        sky = (task("x_sky").with_main_input(raw_sky)
               .with_associated_input(flat, [master_flat_class])
               .with_meta_targets([SCIENCE]).build())
        telluric = task("x_telluric").with_main_input(sci, [sci_reduced_class]).build()

        def unused():
            ghost = task("x_ghost").with_main_input(raw_sci).build()
    """,
    "metis_wkf.py": """
        from .metis_x_wkf import *
    """,
}


@pytest.fixture
def pipeline(tmp_path):
    return _write_pkg(tmp_path / "METIS_Pipeline", _FILES)


class TestDerivePipeline:
    def test_dpr_triples_resolve_keyword_aliases_and_strings(self, pipeline):
        dpr = {tuple(row[:3]): row[3] for row in upstream.derive_pipeline(pipeline)["dpr_to_tag"]}
        assert dpr == {
            ("CALIB", "DARK", "IFU"): "DARK_RAW",
            ("CALIB", "FLAT", "IFU"): "FLAT_RAW",
            ("SCIENCE", "OBJECT", "IFU"): "SCI_RAW",
            (None, "SKY", "IFU"): "SKY_RAW",
        }

    def test_chain_order_meta_and_dropped_tasks(self, pipeline):
        chain = upstream.derive_pipeline(pipeline)["chains"]["metis.metis_x_wkf"]
        # Ordered by dependency depth, science last within a depth. The ungated
        # x_sci reduces a SCIENCE raw, so it counts as science; x_telluric
        # has no raw input and x_ghost is never instantiated.
        assert chain == [
            ["x_dark", "DARK_RAW", None],
            ["x_flat", "FLAT_RAW", None],
            ["x_sci", "SCI_RAW", "science"],
            ["x_sky", "SKY_RAW", "science"],
        ]

    def test_task_products_are_what_consumers_ask_for(self, pipeline):
        products = upstream.derive_pipeline(pipeline)["task_products"]
        assert products == {"x_dark": ["MASTER_DARK"], "x_flat": ["MASTER_FLAT"]}

    def test_graph_has_every_input_recipe_and_lane(self, pipeline):
        graph = upstream.derive_pipeline(pipeline)["graph"]
        assert graph["lanes"] == ["metis.metis_x_wkf"]
        assert sorted(graph["tasks"]) == ["x_dark", "x_flat", "x_sci", "x_sky", "x_telluric"]
        assert graph["tasks"]["x_dark"] == {
            "recipe": "x_det_dark",
            "meta": [],
            "lanes": ["metis.metis_x_wkf"],
        }
        assert graph["tasks"]["x_sky"]["meta"] == ["science"]
        assert graph["sources"]["static_map"] == ["STATIC_MAP"]
        flat_inputs = [row for row in graph["inputs"] if row[1] == "x_flat"]
        assert flat_inputs == [
            ["source:raw_flat", "x_flat", "main", ["FLAT_RAW"]],
            ["task:x_dark", "x_flat", "assoc", ["MASTER_DARK"]],
            ["source:static_map", "x_flat", "assoc", ["STATIC_MAP"]],
        ]
        assert ["task:x_sci", "x_telluric", "main", ["SCI_REDUCED"]] in graph["inputs"]

    def test_accepts_the_workflow_folder_too(self, pipeline):
        whole = upstream.derive_pipeline(pipeline)
        assert upstream.derive_pipeline(pipeline / "metisp" / "workflows") == whole

    def test_pipeline_without_products_is_rejected(self, tmp_path):
        files = dict(_FILES)
        files["metis_x_wkf.py"] = (
            _FILES["metis_x_wkf.py"].replace(", [master_dark_class]", "").replace(", [master_flat_class]", "")
        )
        with pytest.raises(ValueError, match="incomplete"):
            upstream.derive_pipeline(_write_pkg(tmp_path, files))

    def test_missing_package_is_rejected(self, tmp_path):
        with pytest.raises(ValueError, match="no EDPS workflow package"):
            upstream.derive_pipeline(tmp_path)


class TestIrdbModes:
    def test_modes_with_tech_only(self, tmp_path):
        (tmp_path / "METIS").mkdir()
        (tmp_path / "METIS" / "default.yaml").write_text(
            "---\nobject: instrument\n---\nmode_yamls:\n"
            "  - name: img_lm\n    properties: {tech: 'IMAGE,LM'}\n"
            "  - name: lms_extended\n    properties: {}\n"
        )
        assert upstream.derive_irdb_modes(tmp_path) == {"img_lm": "IMAGE,LM"}

    def test_release_without_tech_is_rejected(self, tmp_path):
        (tmp_path / "METIS").mkdir()
        (tmp_path / "METIS" / "default.yaml").write_text("mode_yamls:\n  - name: img_lm\n")
        with pytest.raises(ValueError, match="tech"):
            upstream.derive_irdb_modes(tmp_path)


class TestSnapshot:
    def test_snapshot_irdb_release_is_the_pinned_one(self):
        snap = json.loads(upstream.SNAPSHOT_PATH.read_text())
        assert snap["irdb_release"] == rm.IRDB_RELEASES["METIS"]

    def test_snapshot_loads(self):
        t = tables.get()
        assert t.source.startswith("bundled snapshot")
        assert t.lookup_dpr_tag("CALIB", "DARK", "IFU") == "DARK_IFU_RAW"
        assert t.chains[tables.TECH_TO_WORKFLOW["IFU"]]


class TestResolution:
    @pytest.fixture(autouse=True)
    def _live(self, monkeypatch, tmp_path):
        monkeypatch.setenv("HOME", str(tmp_path / "home"))
        monkeypatch.setenv("METIS_PIPELINE_DIR", str(tmp_path / "no-pipeline"))
        tables.use_snapshot(False)

    def _edps_config(self, tmp_path, workflow_dir):
        cfg = tmp_path / "home" / ".edps" / "application.properties"
        cfg.parent.mkdir(parents=True)
        cfg.write_text(f"[application]\nworkflow_dir={workflow_dir}\n")

    def test_no_checkout_uses_the_snapshot(self):
        assert tables.get().source.startswith("bundled snapshot")

    def test_edps_workflow_dir_wins(self, tmp_path, pipeline, monkeypatch):
        other = _write_pkg(tmp_path / "other", _FILES)
        monkeypatch.setenv("METIS_PIPELINE_DIR", str(other))
        self._edps_config(tmp_path, pipeline / "metisp" / "workflows")
        t = tables.get()
        assert t.source.startswith(str(pipeline / "metisp" / "workflows"))
        assert "metis.metis_x_wkf" in t.chains

    def test_pipeline_dir_is_the_fallback(self, pipeline, monkeypatch):
        monkeypatch.setenv("METIS_PIPELINE_DIR", str(pipeline))
        assert "metis.metis_x_wkf" in tables.get().chains

    def test_live_tables_keep_the_snapshot_modes(self, pipeline, monkeypatch):
        monkeypatch.setenv("METIS_PIPELINE_DIR", str(pipeline))
        assert tables.get().workflow_for_mode("img_lm") == tables.TECH_TO_WORKFLOW["IMAGE,LM"]

    def test_broken_checkout_falls_back_with_a_warning(self, pipeline, monkeypatch, capsys):
        (pipeline / "metisp" / "workflows" / "metis" / "metis_x_wkf.py").write_text("task(")
        monkeypatch.setenv("METIS_PIPELINE_DIR", str(pipeline))
        assert tables.get().source.startswith("bundled snapshot")
        assert "cannot read the Pipeline workflows" in capsys.readouterr().err

    def test_use_snapshot_ignores_the_checkout(self, pipeline, monkeypatch):
        monkeypatch.setenv("METIS_PIPELINE_DIR", str(pipeline))
        tables.use_snapshot()
        assert tables.get().source.startswith("bundled snapshot")


class TestYamlBlockTag:
    @pytest.mark.parametrize(
        ("do_catg", "props", "tag"),
        [
            # Simulations' file label differs from the Pipeline tag.
            (
                "LM_SLITLOSSES_RAW",
                {"catg": "CALIB", "type": "SLITLOSS", "tech": "LSS,LM"},
                "LM_ADC_SLITLOSS_RAW",
            ),
            ("DARK_LM_RAW", {"catg": "CALIB", "type": "DARK", "tech": "IMAGE,LM"}, "DARK_2RG_RAW"),
            # Simulations writes RAVC,LM as DPR.TECH IMAGE,LM.
            ("LM_RAVC_SCI_RAW", {"catg": "SCIENCE", "type": "OBJECT", "tech": "RAVC,LM"}, "LM_IMAGE_SCI_RAW"),
            # Spelling is normalised; sky ignores catg.
            ("X", {"catg": "science", "type": " sky ", "tech": "image, lm"}, "LM_IMAGE_SKY_RAW"),
            # An unknown triple keeps the label.
            ("MY_RAW", {"catg": "CALIB", "type": "DARK,WCU_OFF", "tech": "IMAGE,LM"}, "MY_RAW"),
            ("", {}, None),
        ],
    )
    def test_tag(self, do_catg, props, tag):
        assert rm.yaml_block_tag({"do.catg": do_catg}, props) == tag

    def test_slitloss_yaml_targets_the_lss_calibrations(self, tmp_path):
        f = tmp_path / "slitloss.yaml"
        f.write_text(
            "slitloss:\n  do.catg: LM_SLITLOSSES_RAW\n  mode: lss_l\n"
            "  properties: {catg: CALIB, type: SLITLOSS, tech: 'LSS,LM'}\n"
        )
        tags, has_sci, wfs = rm.scan_yaml_inputs([f])
        assert tags == {"LM_ADC_SLITLOSS_RAW"}
        assert rm.infer_edps_targets_for_workflows(tags, has_sci, wfs) == ["-m", "qc1calib"]
