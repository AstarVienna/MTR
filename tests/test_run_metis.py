"""
Unit tests for run_metis.py helper functions.

These tests cover the pure-Python logic exercised by the GitHub Actions CI
(run_edps.yaml / edps_runner.yaml) without requiring a live EDPS server,
ScopeSim installation, or any FITS data on disk.

Run with:
    python -m pytest test_run_metis.py
"""

import textwrap
import pytest
from pathlib import Path
from unittest.mock import patch, MagicMock

from metis_test_runner.run_metis import (
    DPR_TO_TAG,
    MODE_TO_WORKFLOW,
    TECH_TO_WORKFLOW,
    UMBRELLA_WORKFLOW,
    WORKFLOW_TASK_CHAIN,
    _build_sim_script,
    _check_default_env,
    _default_subprocess_env,
    _edps_base_cmd,
    _parse_line_range,
    _slice_csv,
    classify_fits_file,
    collect_tags_from_fits,
    infer_edps_target,
    infer_edps_targets_for_workflows,
    infer_workflow,
    known_workflows,
    parse_args,
    read_edps_port,
    scan_fits_inputs,
    scan_yaml_inputs,
)

import argparse


# ---------------------------------------------------------------------------
# read_edps_port
# ---------------------------------------------------------------------------

class TestReadEdpsPort:
    def test_returns_default_when_file_missing(self, tmp_path):
        """No application.properties → falls back to default."""
        with patch("metis_test_runner.run_metis.Path.home", return_value=tmp_path):
            assert read_edps_port(default=5000) == 5000

    def test_reads_port_from_properties_file(self, tmp_path):
        props_dir = tmp_path / ".edps"
        props_dir.mkdir()
        (props_dir / "application.properties").write_text("port=4444\n")
        with patch("metis_test_runner.run_metis.Path.home", return_value=tmp_path):
            assert read_edps_port() == 4444

    def test_ignores_malformed_port_value(self, tmp_path):
        props_dir = tmp_path / ".edps"
        props_dir.mkdir()
        (props_dir / "application.properties").write_text("port=not_a_number\n")
        with patch("metis_test_runner.run_metis.Path.home", return_value=tmp_path):
            assert read_edps_port(default=5000) == 5000

    def test_warns_on_malformed_port_value(self, tmp_path, capsys):
        # Silent fallback to default is confusing: user thinks EDPS is on
        # 5000 while the real config has a typo. We want a stderr breadcrumb.
        props_dir = tmp_path / ".edps"
        props_dir.mkdir()
        (props_dir / "application.properties").write_text("port=4444, 5555\n")
        with patch("metis_test_runner.run_metis.Path.home", return_value=tmp_path):
            assert read_edps_port(default=5000) == 5000
        err = capsys.readouterr().err
        assert "malformed" in err
        assert "4444, 5555" in err

    def test_picks_port_from_multiline_file(self, tmp_path):
        props_dir = tmp_path / ".edps"
        props_dir.mkdir()
        content = "server.host=localhost\nport=9999\nsome.other=value\n"
        (props_dir / "application.properties").write_text(content)
        with patch("metis_test_runner.run_metis.Path.home", return_value=tmp_path):
            assert read_edps_port() == 9999

    def test_custom_default_returned_when_no_file(self, tmp_path):
        with patch("metis_test_runner.run_metis.Path.home", return_value=tmp_path):
            assert read_edps_port(default=1234) == 1234


# ---------------------------------------------------------------------------
# infer_workflow
# ---------------------------------------------------------------------------

def _write_yaml(tmp_path, name, content):
    p = tmp_path / name
    p.write_text(textwrap.dedent(content))
    return p


def _write_csv(tmp_path, name, content=None):
    """Write a minimal CSV file. MTR never parses CSV content itself
    (it delegates to metis_simulations.csvParser at sim time), so a
    one-line placeholder is enough for the MTR-side tests."""
    p = tmp_path / name
    p.write_text(content or "Block,File,DIT,NDIT,Tech\nplaceholder,row,0.1,1,IMAGE_LM\n")
    return p


class TestInferWorkflow:
    # --- tech-based inference ---

    def test_lms_tech_maps_to_ifu_workflow(self, tmp_path):
        f = _write_yaml(tmp_path, "obs.yaml", """
            block1:
              do.catg: DETLIN_IFU_RAW
              mode: wcu_lms
              properties:
                tech: "LMS"
                catg: "CALIB"
        """)
        wf, has_sci, tags = infer_workflow([f])
        assert wf == "metis.metis_ifu_wkf"
        assert not has_sci
        assert "DETLIN_IFU_RAW" in tags

    def test_image_lm_tech_maps_to_lm_img_workflow(self, tmp_path):
        f = _write_yaml(tmp_path, "obs.yaml", """
            block1:
              do.catg: LM_IMAGE_SCI_RAW
              properties:
                tech: "IMAGE,LM"
                catg: "SCIENCE"
        """)
        wf, has_sci, tags = infer_workflow([f])
        assert wf == "metis.metis_lm_img_wkf"
        assert has_sci

    def test_image_n_tech_maps_to_n_img_workflow(self, tmp_path):
        f = _write_yaml(tmp_path, "obs.yaml", """
            block1:
              do.catg: N_IMAGE_SCI_RAW
              properties:
                tech: "IMAGE,N"
                catg: "SCIENCE"
        """)
        wf, has_sci, _ = infer_workflow([f])
        assert wf == "metis.metis_n_img_wkf"
        assert has_sci

    def test_lss_lm_tech_maps_to_lm_lss_workflow(self, tmp_path):
        f = _write_yaml(tmp_path, "obs.yaml", """
            block1:
              do.catg: LM_LSS_SCI_RAW
              properties:
                tech: "LSS,LM"
                catg: "SCIENCE"
        """)
        wf, _, _ = infer_workflow([f])
        assert wf == "metis.metis_lm_lss_wkf"

    def test_lss_n_tech_maps_to_n_lss_workflow(self, tmp_path):
        f = _write_yaml(tmp_path, "obs.yaml", """
            block1:
              do.catg: N_LSS_SCI_RAW
              properties:
                tech: "LSS,N"
                catg: "SCIENCE"
        """)
        wf, _, _ = infer_workflow([f])
        assert wf == "metis.metis_n_lss_wkf"

    # --- mode-based fallback ---

    def test_mode_lms_maps_to_ifu_workflow(self, tmp_path):
        f = _write_yaml(tmp_path, "obs.yaml", """
            block1:
              do.catg: IFU_SCI_RAW
              mode: lms
              properties:
                catg: "SCIENCE"
        """)
        wf, has_sci, _ = infer_workflow([f])
        assert wf == "metis.metis_ifu_wkf"
        assert has_sci

    def test_mode_img_lm_maps_to_lm_img_workflow(self, tmp_path):
        f = _write_yaml(tmp_path, "obs.yaml", """
            block1:
              do.catg: LM_IMAGE_SCI_RAW
              mode: img_lm
              properties:
                catg: "SCIENCE"
        """)
        wf, _, _ = infer_workflow([f])
        assert wf == "metis.metis_lm_img_wkf"

    # --- normalisation: whitespace and case tolerance ---

    def test_tech_with_whitespace_around_comma_still_resolves(self, tmp_path):
        # Human-edited YAML often has "IMAGE, LM" with a space; we shouldn't
        # force users to know the keys are spaceless.
        f = _write_yaml(tmp_path, "obs.yaml", """
            block1:
              do.catg: LM_IMAGE_SCI_RAW
              properties:
                tech: "IMAGE, LM"
                catg: "SCIENCE"
        """)
        wf, _, _ = infer_workflow([f])
        assert wf == "metis.metis_lm_img_wkf"

    def test_mode_with_wrong_case_still_resolves(self, tmp_path):
        # Mode keys are lowercase; accept upper-case YAML values too.
        f = _write_yaml(tmp_path, "obs.yaml", """
            block1:
              do.catg: LM_IMAGE_SCI_RAW
              mode: IMG_LM
              properties:
                catg: "SCIENCE"
        """)
        wf, _, _ = infer_workflow([f])
        assert wf == "metis.metis_lm_img_wkf"

    # --- tech takes priority over mode ---

    def test_tech_takes_priority_over_mode(self, tmp_path):
        f = _write_yaml(tmp_path, "obs.yaml", """
            block1:
              do.catg: IFU_SCI_RAW
              mode: img_lm
              properties:
                tech: "LMS"
                catg: "SCIENCE"
        """)
        wf, _, _ = infer_workflow([f])
        assert wf == "metis.metis_ifu_wkf"

    # --- multi-file / multi-block ---

    def test_multiple_yaml_files_merged(self, tmp_path):
        f1 = _write_yaml(tmp_path, "obs1.yaml", """
            block1:
              do.catg: DETLIN_IFU_RAW
              mode: wcu_lms
              properties:
                tech: "LMS"
                catg: "CALIB"
        """)
        f2 = _write_yaml(tmp_path, "obs2.yaml", """
            block2:
              do.catg: IFU_SCI_RAW
              mode: lms
              properties:
                tech: "LMS"
                catg: "SCIENCE"
        """)
        wf, has_sci, tags = infer_workflow([f1, f2])
        assert wf == "metis.metis_ifu_wkf"
        assert has_sci
        assert "DETLIN_IFU_RAW" in tags
        assert "IFU_SCI_RAW" in tags

    def test_has_science_false_for_calib_only(self, tmp_path):
        f = _write_yaml(tmp_path, "obs.yaml", """
            block1:
              do.catg: DARK_IFU_RAW
              mode: wcu_lms
              properties:
                tech: "LMS"
                catg: "CALIB"
        """)
        _, has_sci, _ = infer_workflow([f])
        assert not has_sci

    def test_science_catg_case_insensitive(self, tmp_path):
        f = _write_yaml(tmp_path, "obs.yaml", """
            block1:
              do.catg: IFU_SCI_RAW
              properties:
                tech: "LMS"
                catg: "science"
        """)
        _, has_sci, _ = infer_workflow([f])
        assert has_sci

    # --- error path ---

    def test_raises_for_unknown_tech_and_mode(self, tmp_path):
        f = _write_yaml(tmp_path, "obs.yaml", """
            block1:
              do.catg: SOMETHING_RAW
              mode: unknown_mode
              properties:
                tech: "UNKNOWN,TECH"
                catg: "CALIB"
        """)
        with pytest.raises(ValueError, match="Cannot determine workflow"):
            infer_workflow([f])

    def test_raises_for_empty_yaml(self, tmp_path):
        f = _write_yaml(tmp_path, "obs.yaml", """
            block1: null
        """)
        with pytest.raises(ValueError):
            infer_workflow([f])

    # --- CSV handling: introspection is skipped; workflow comes from FITS ---

    def test_raises_when_only_csv_inputs(self, tmp_path):
        """CSV-only input set → ValueError (workflow is inferred from FITS later)."""
        csv_file = _write_csv(tmp_path, "obs.csv")
        with pytest.raises(ValueError, match="cannot be inferred from CSV"):
            infer_workflow([csv_file])

    def test_csv_files_skipped_in_mixed_input(self, tmp_path):
        """When YAML+CSV are mixed, only YAML drives inference; CSV is ignored."""
        yaml_file = _write_yaml(tmp_path, "obs.yaml", """
            block1:
              do.catg: LM_IMAGE_SCI_RAW
              properties:
                tech: "IMAGE,LM"
                catg: "SCIENCE"
        """)
        csv_file = _write_csv(tmp_path, "obs.csv")
        wf, has_sci, tags = infer_workflow([yaml_file, csv_file])
        assert wf == "metis.metis_lm_img_wkf"
        assert has_sci
        assert tags == {"LM_IMAGE_SCI_RAW"}


class TestKnownWorkflows:
    def test_returns_sorted_unique_workflow_names(self):
        names = known_workflows()
        assert names == sorted(set(names))
        # Anchor a few representative entries so the GUI dropdown stays
        # populated; the full list comes from TECH_TO_WORKFLOW + MODE_TO_WORKFLOW.
        assert "metis.metis_lm_img_wkf" in names
        assert "metis.metis_ifu_wkf" in names

    def test_all_returned_workflows_are_in_lookup_tables(self):
        names = set(known_workflows())
        expected = set(TECH_TO_WORKFLOW.values()) | set(MODE_TO_WORKFLOW.values())
        assert names == expected


# ---------------------------------------------------------------------------
# infer_edps_target
# ---------------------------------------------------------------------------

class TestInferEdpsTarget:
    # --- IFU workflow ---

    def test_ifu_lingain_only(self):
        flags = infer_edps_target(
            "metis.metis_ifu_wkf",
            {"DETLIN_IFU_RAW"},
            has_science=False,
        )
        assert flags == ["-t", "metis_ifu_lingain"]

    def test_ifu_dark_deepest_calib(self):
        flags = infer_edps_target(
            "metis.metis_ifu_wkf",
            {"DETLIN_IFU_RAW", "DARK_IFU_RAW"},
            has_science=False,
        )
        assert flags == ["-t", "metis_ifu_dark"]

    def test_ifu_science_only(self):
        flags = infer_edps_target(
            "metis.metis_ifu_wkf",
            {"IFU_SCI_RAW"},
            has_science=True,
        )
        assert flags == ["-m", "science"]

    def test_ifu_rsrf_plus_science(self):
        flags = infer_edps_target(
            "metis.metis_ifu_wkf",
            {"IFU_RSRF_RAW", "IFU_SCI_RAW"},
            has_science=True,
        )
        assert flags == ["-t", "metis_ifu_rsrf", "-m", "science"]

    # --- LM IMG workflow ---

    def test_lm_img_calib_chain_dark(self):
        flags = infer_edps_target(
            "metis.metis_lm_img_wkf",
            {"DETLIN_2RG_RAW", "DARK_2RG_RAW"},
            has_science=False,
        )
        assert flags == ["-t", "metis_lm_img_dark"]

    def test_lm_img_science_only(self):
        flags = infer_edps_target(
            "metis.metis_lm_img_wkf",
            {"LM_IMAGE_SCI_RAW"},
            has_science=True,
        )
        assert flags == ["-m", "science"]

    def test_lm_img_full_calib_plus_science(self):
        tags = {
            "DETLIN_2RG_RAW", "DARK_2RG_RAW",
            "LM_FLAT_LAMP_RAW", "LM_DISTORTION_RAW",
            "LM_IMAGE_SCI_RAW",
        }
        flags = infer_edps_target(
            "metis.metis_lm_img_wkf", tags, has_science=True
        )
        assert flags == ["-t", "metis_lm_img_distortion", "-m", "science"]

    # --- LSS workflows use qc1calib ---

    def test_lm_lss_calib_uses_qc1calib(self):
        flags = infer_edps_target(
            "metis.metis_lm_lss_wkf",
            {"DETLIN_2RG_RAW", "DARK_2RG_RAW"},
            has_science=False,
        )
        assert flags == ["-m", "qc1calib"]

    def test_n_lss_calib_plus_science(self):
        flags = infer_edps_target(
            "metis.metis_n_lss_wkf",
            {"DETLIN_GEO_RAW", "N_LSS_SCI_RAW"},
            has_science=True,
        )
        assert flags == ["-m", "qc1calib", "-m", "science"]

    def test_lm_lss_science_only_no_calib_flag(self):
        """Science data without calibration tags → only -m science."""
        flags = infer_edps_target(
            "metis.metis_lm_lss_wkf",
            {"LM_LSS_SCI_RAW"},
            has_science=True,
        )
        assert flags == ["-m", "science"]

    # --- edge cases ---

    def test_no_matching_tags_returns_empty(self):
        flags = infer_edps_target(
            "metis.metis_ifu_wkf",
            {"TOTALLY_UNKNOWN_TAG"},
            has_science=False,
        )
        assert flags == []

    def test_unknown_workflow_returns_empty(self):
        flags = infer_edps_target(
            "metis.unknown_wkf",
            {"DETLIN_IFU_RAW"},
            has_science=False,
        )
        assert flags == []

    def test_empty_tags_with_science_flag(self):
        flags = infer_edps_target(
            "metis.metis_ifu_wkf",
            set(),
            has_science=True,
        )
        assert flags == ["-m", "science"]


# ---------------------------------------------------------------------------
# scan_yaml_inputs
# ---------------------------------------------------------------------------

class TestScanYamlInputs:
    def test_single_block_collects_workflow_and_tag(self, tmp_path):
        f = _write_yaml(tmp_path, "obs.yaml", """
            block1:
              do.catg: IFU_SCI_RAW
              properties:
                tech: "LMS"
                catg: "SCIENCE"
        """)
        tags, has_sci, wfs = scan_yaml_inputs([f])
        assert tags == {"IFU_SCI_RAW"}
        assert has_sci is True
        assert wfs == {"metis.metis_ifu_wkf"}

    def test_mixed_workflows_in_one_file(self, tmp_path):
        f = _write_yaml(tmp_path, "obs.yaml", """
            block1:
              do.catg: LM_IMAGE_SCI_RAW
              properties:
                tech: "IMAGE,LM"
                catg: "SCIENCE"
            block2:
              do.catg: IFU_SCI_RAW
              properties:
                tech: "LMS"
                catg: "SCIENCE"
        """)
        tags, has_sci, wfs = scan_yaml_inputs([f])
        assert tags == {"LM_IMAGE_SCI_RAW", "IFU_SCI_RAW"}
        assert has_sci is True
        assert wfs == {"metis.metis_lm_img_wkf", "metis.metis_ifu_wkf"}

    def test_mixed_workflows_across_files(self, tmp_path):
        f1 = _write_yaml(tmp_path, "obs1.yaml", """
            block1:
              do.catg: LM_IMAGE_SCI_RAW
              properties:
                tech: "IMAGE,LM"
                catg: "SCIENCE"
        """)
        f2 = _write_yaml(tmp_path, "obs2.yaml", """
            block1:
              do.catg: N_IMAGE_SCI_RAW
              properties:
                tech: "IMAGE,N"
                catg: "SCIENCE"
        """)
        tags, has_sci, wfs = scan_yaml_inputs([f1, f2])
        assert tags == {"LM_IMAGE_SCI_RAW", "N_IMAGE_SCI_RAW"}
        assert has_sci is True
        assert wfs == {"metis.metis_lm_img_wkf", "metis.metis_n_img_wkf"}

    def test_unknown_tech_does_not_raise(self, tmp_path):
        # Unlike infer_workflow, scan_yaml_inputs silently skips unrecognised
        # tech/mode values — the caller decides whether an empty sub-workflow
        # set is fatal.
        f = _write_yaml(tmp_path, "obs.yaml", """
            block1:
              do.catg: SOMETHING_RAW
              mode: unknown_mode
              properties:
                tech: "UNKNOWN,TECH"
                catg: "CALIB"
        """)
        tags, has_sci, wfs = scan_yaml_inputs([f])
        assert tags == {"SOMETHING_RAW"}
        assert has_sci is False
        assert wfs == set()

    def test_mode_fallback_when_tech_missing(self, tmp_path):
        f = _write_yaml(tmp_path, "obs.yaml", """
            block1:
              do.catg: IFU_SCI_RAW
              mode: lms
              properties:
                catg: "SCIENCE"
        """)
        _, _, wfs = scan_yaml_inputs([f])
        assert wfs == {"metis.metis_ifu_wkf"}

    def test_calib_only_has_no_science(self, tmp_path):
        f = _write_yaml(tmp_path, "obs.yaml", """
            block1:
              do.catg: DARK_IFU_RAW
              properties:
                tech: "LMS"
                catg: "CALIB"
        """)
        _, has_sci, _ = scan_yaml_inputs([f])
        assert has_sci is False

    def test_empty_yaml_returns_empties(self, tmp_path):
        f = _write_yaml(tmp_path, "obs.yaml", """
            block1: null
        """)
        tags, has_sci, wfs = scan_yaml_inputs([f])
        assert tags == set()
        assert has_sci is False
        assert wfs == set()


# ---------------------------------------------------------------------------
# infer_edps_targets_for_workflows  (multi-workflow target inference)
# ---------------------------------------------------------------------------

class TestInferEdpsTargetsForWorkflows:
    def test_single_sub_workflow_matches_single_workflow_helper(self):
        # When only one sub-workflow is active, the multi-workflow helper
        # must return the same flags as infer_edps_target for that workflow.
        wf = "metis.metis_ifu_wkf"
        data_tags = {"DETLIN_IFU_RAW"}
        flags = infer_edps_targets_for_workflows(data_tags, False, {wf})
        assert flags == infer_edps_target(wf, data_tags, has_science=False)

    def test_mixed_lm_img_and_ifu_emits_both_targets(self):
        data_tags = {"LM_DISTORTION_RAW", "IFU_RSRF_RAW"}
        flags = infer_edps_targets_for_workflows(
            data_tags,
            has_science=False,
            sub_workflows={"metis.metis_lm_img_wkf", "metis.metis_ifu_wkf"},
        )
        # Both deepest-calib tasks must be present in the same command line.
        assert "-t" in flags
        assert "metis_lm_img_distortion" in flags
        assert "metis_ifu_rsrf" in flags
        # Each -t introduces exactly one task — no stray -m flags here.
        assert flags.count("-t") == 2
        assert "-m" not in flags

    def test_mixed_with_science_appends_m_science_once(self):
        flags = infer_edps_targets_for_workflows(
            {"LM_DISTORTION_RAW", "IFU_RSRF_RAW", "IFU_SCI_RAW",
             "LM_IMAGE_SCI_RAW"},
            has_science=True,
            sub_workflows={"metis.metis_lm_img_wkf", "metis.metis_ifu_wkf"},
        )
        # Trailing -m science exactly once.
        assert flags[-2:] == ["-m", "science"]
        assert flags.count("science") == 1

    def test_qc1calib_dedup_across_two_lss_sub_workflows(self):
        # LM-LSS + N-LSS both have qc1calib-gated calibration chains.  When
        # both contribute, the umbrella command line must include exactly
        # one -m qc1calib.
        flags = infer_edps_targets_for_workflows(
            {"DETLIN_2RG_RAW", "DARK_2RG_RAW",
             "DETLIN_GEO_RAW", "DARK_GEO_RAW"},
            has_science=False,
            sub_workflows={"metis.metis_lm_lss_wkf", "metis.metis_n_lss_wkf"},
        )
        assert flags.count("qc1calib") == 1

    def test_shared_calib_task_dedup_across_lm_img_variants(self):
        # LM_IMG, LM_RAVC, LM_APP, PUPIL_IMAGING all share the same
        # metis_lm_img_* calibration task names.  When several of these are
        # in active_sub_workflows the same -t flag must not be emitted more
        # than once.
        flags = infer_edps_targets_for_workflows(
            {"DETLIN_2RG_RAW", "DARK_2RG_RAW"},
            has_science=False,
            sub_workflows={
                "metis.metis_lm_img_wkf",
                "metis.metis_lm_ravc_wkf",
                "metis.metis_lm_app_wkf",
            },
        )
        assert flags == ["-t", "metis_lm_img_dark"]

    def test_no_active_sub_workflows_returns_empty(self):
        flags = infer_edps_targets_for_workflows(
            {"LM_IMAGE_SCI_RAW"},
            has_science=False,
            sub_workflows=set(),
        )
        assert flags == []

    def test_only_science_active_workflows_yields_only_m_science(self):
        flags = infer_edps_targets_for_workflows(
            {"LM_IMAGE_SCI_RAW"},
            has_science=True,
            sub_workflows={"metis.metis_lm_img_wkf"},
        )
        assert flags == ["-m", "science"]

    def test_deterministic_flag_order_follows_workflow_chain_order(self):
        # Two calls with the same inputs must produce the same flag order
        # regardless of set iteration order (sub_workflows is a set).
        data_tags = {"LM_DISTORTION_RAW", "IFU_RSRF_RAW"}
        wfs = {"metis.metis_ifu_wkf", "metis.metis_lm_img_wkf"}
        first = infer_edps_targets_for_workflows(data_tags, False, wfs)
        second = infer_edps_targets_for_workflows(data_tags, False, wfs)
        assert first == second


# ---------------------------------------------------------------------------
# UMBRELLA_WORKFLOW
# ---------------------------------------------------------------------------

class TestUmbrellaWorkflow:
    def test_umbrella_constant_value(self):
        # The umbrella workflow is the EDPS workflow that imports every METIS
        # sub-workflow; main() always invokes EDPS with this name.
        assert UMBRELLA_WORKFLOW == "metis.metis_wkf"


# ---------------------------------------------------------------------------
# collect_tags_from_fits
# ---------------------------------------------------------------------------

class TestCollectTagsFromFits:
    def test_returns_empty_set_when_astropy_missing(self, tmp_path):
        """If astropy is not installed, returns empty set gracefully."""
        import builtins
        real_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "astropy.io.fits":
                raise ImportError("mocked missing astropy")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=mock_import):
            result = collect_tags_from_fits(tmp_path)
        assert result == set()

    def test_returns_empty_set_for_empty_directory(self, tmp_path):
        try:
            import astropy  # noqa: F401
        except ImportError:
            pytest.skip("astropy not installed")
        result = collect_tags_from_fits(tmp_path)
        assert result == set()

    def test_classifies_lm_science_fits_header(self, tmp_path):
        pytest.importorskip("astropy")
        from astropy.io import fits as afits

        hdr = afits.Header()
        hdr["HIERARCH ESO DPR CATG"] = "SCIENCE"
        hdr["HIERARCH ESO DPR TYPE"] = "OBJECT"
        hdr["HIERARCH ESO DPR TECH"] = "IMAGE,LM"
        hdul = afits.HDUList([afits.PrimaryHDU(header=hdr)])
        hdul.writeto(tmp_path / "sci.fits")

        tags = collect_tags_from_fits(tmp_path)
        assert "LM_IMAGE_SCI_RAW" in tags

    def test_classifies_ifu_dark_fits_header(self, tmp_path):
        pytest.importorskip("astropy")
        from astropy.io import fits as afits

        hdr = afits.Header()
        hdr["HIERARCH ESO DPR CATG"] = "CALIB"
        hdr["HIERARCH ESO DPR TYPE"] = "DARK"
        hdr["HIERARCH ESO DPR TECH"] = "IFU"
        hdul = afits.HDUList([afits.PrimaryHDU(header=hdr)])
        hdul.writeto(tmp_path / "dark.fits")

        tags = collect_tags_from_fits(tmp_path)
        assert "DARK_IFU_RAW" in tags

    def test_skips_fits_with_unknown_header_triple(self, tmp_path):
        pytest.importorskip("astropy")
        from astropy.io import fits as afits

        hdr = afits.Header()
        hdr["HIERARCH ESO DPR CATG"] = "UNKNOWN"
        hdr["HIERARCH ESO DPR TYPE"] = "GARBAGE"
        hdr["HIERARCH ESO DPR TECH"] = "XYZ"
        hdul = afits.HDUList([afits.PrimaryHDU(header=hdr)])
        hdul.writeto(tmp_path / "unknown.fits")

        tags = collect_tags_from_fits(tmp_path)
        assert tags == set()

    def test_collects_tags_from_multiple_fits_files(self, tmp_path):
        pytest.importorskip("astropy")
        from astropy.io import fits as afits

        for (catg, typ, tech), fname in [
            (("CALIB", "DETLIN", "IFU"), "detlin.fits"),
            (("CALIB", "DARK",   "IFU"), "dark.fits"),
            (("SCIENCE", "OBJECT", "IFU"), "sci.fits"),
        ]:
            hdr = afits.Header()
            hdr["HIERARCH ESO DPR CATG"] = catg
            hdr["HIERARCH ESO DPR TYPE"] = typ
            hdr["HIERARCH ESO DPR TECH"] = tech
            afits.HDUList([afits.PrimaryHDU(header=hdr)]).writeto(tmp_path / fname)

        tags = collect_tags_from_fits(tmp_path)
        assert tags == {"DETLIN_IFU_RAW", "DARK_IFU_RAW", "IFU_SCI_RAW"}

    def test_skips_non_fits_files_silently(self, tmp_path):
        pytest.importorskip("astropy")
        (tmp_path / "readme.txt").write_text("not a fits file")
        # Should not raise; no .fits files → empty set
        tags = collect_tags_from_fits(tmp_path)
        assert tags == set()

    def test_recurses_into_subdirectories(self, tmp_path):
        """--pipeline-input may point at an output-root; raw FITS live in
        sub-directories (e.g. ``sim/``) and must still be classified."""
        pytest.importorskip("astropy")
        from astropy.io import fits as afits

        sim_dir = tmp_path / "sim"
        sim_dir.mkdir()
        hdr = afits.Header()
        hdr["HIERARCH ESO DPR CATG"] = "CALIB"
        hdr["HIERARCH ESO DPR TYPE"] = "DARK"
        hdr["HIERARCH ESO DPR TECH"] = "IFU"
        afits.HDUList([afits.PrimaryHDU(header=hdr)]).writeto(sim_dir / "dark.fits")

        tags = collect_tags_from_fits(tmp_path)
        assert "DARK_IFU_RAW" in tags


# ---------------------------------------------------------------------------
# scan_fits_inputs
# ---------------------------------------------------------------------------

class TestScanFitsInputs:
    def test_returns_empty_when_astropy_missing(self, tmp_path):
        import builtins
        real_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "astropy.io.fits":
                raise ImportError("mocked missing astropy")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=mock_import):
            tags, wfs = scan_fits_inputs(tmp_path)
        assert tags == set()
        assert wfs == set()

    def test_single_workflow_from_fits(self, tmp_path):
        pytest.importorskip("astropy")
        from astropy.io import fits as afits

        hdr = afits.Header()
        hdr["HIERARCH ESO DPR CATG"] = "CALIB"
        hdr["HIERARCH ESO DPR TYPE"] = "DARK"
        hdr["HIERARCH ESO DPR TECH"] = "IFU"
        afits.HDUList([afits.PrimaryHDU(header=hdr)]).writeto(tmp_path / "dark.fits")

        tags, wfs = scan_fits_inputs(tmp_path)
        assert tags == {"DARK_IFU_RAW"}
        assert wfs == {"metis.metis_ifu_wkf"}

    def test_mixed_fits_yields_two_workflows(self, tmp_path):
        pytest.importorskip("astropy")
        from astropy.io import fits as afits

        for (catg, typ, tech), fname in [
            (("SCIENCE", "OBJECT", "IMAGE,LM"), "lm_sci.fits"),
            (("SCIENCE", "OBJECT", "IFU"),      "ifu_sci.fits"),
        ]:
            hdr = afits.Header()
            hdr["HIERARCH ESO DPR CATG"] = catg
            hdr["HIERARCH ESO DPR TYPE"] = typ
            hdr["HIERARCH ESO DPR TECH"] = tech
            afits.HDUList([afits.PrimaryHDU(header=hdr)]).writeto(tmp_path / fname)

        tags, wfs = scan_fits_inputs(tmp_path)
        assert tags == {"LM_IMAGE_SCI_RAW", "IFU_SCI_RAW"}
        assert wfs == {"metis.metis_lm_img_wkf", "metis.metis_ifu_wkf"}

    def test_pro_catg_only_files_contribute_tags_but_no_workflow(self, tmp_path):
        # Master calibration files (PRO.CATG only, no DPR.TECH) should
        # contribute their classification tag — but cannot pin down a
        # sub-workflow on their own.
        pytest.importorskip("astropy")
        from astropy.io import fits as afits

        hdr = afits.Header()
        hdr["HIERARCH ESO PRO CATG"] = "MASTER_DARK_2RG"
        afits.HDUList([afits.PrimaryHDU(header=hdr)]).writeto(tmp_path / "master.fits")

        tags, wfs = scan_fits_inputs(tmp_path)
        assert "MASTER_DARK_2RG" in tags
        assert wfs == set()

    def test_recurses_into_subdirectories(self, tmp_path):
        pytest.importorskip("astropy")
        from astropy.io import fits as afits

        sub = tmp_path / "sim"
        sub.mkdir()
        hdr = afits.Header()
        hdr["HIERARCH ESO DPR CATG"] = "CALIB"
        hdr["HIERARCH ESO DPR TYPE"] = "DARK"
        hdr["HIERARCH ESO DPR TECH"] = "IFU"
        afits.HDUList([afits.PrimaryHDU(header=hdr)]).writeto(sub / "dark.fits")

        tags, wfs = scan_fits_inputs(tmp_path)
        assert "DARK_IFU_RAW" in tags
        assert "metis.metis_ifu_wkf" in wfs


# ---------------------------------------------------------------------------
# classify_fits_file
# ---------------------------------------------------------------------------

class TestClassifyFitsFile:
    def test_returns_none_when_astropy_missing(self, tmp_path):
        import builtins
        real_import = builtins.__import__

        def mock_import(name, *args, **kwargs):
            if name == "astropy.io.fits":
                raise ImportError("mocked missing astropy")
            return real_import(name, *args, **kwargs)

        with patch("builtins.__import__", side_effect=mock_import):
            result = classify_fits_file(tmp_path / "any.fits")
        assert result is None

    def test_returns_none_for_missing_file(self, tmp_path):
        pytest.importorskip("astropy")
        assert classify_fits_file(tmp_path / "nonexistent.fits") is None

    def test_classifies_lm_flat_lamp(self, tmp_path):
        pytest.importorskip("astropy")
        from astropy.io import fits as afits

        hdr = afits.Header()
        hdr["HIERARCH ESO DPR CATG"] = "CALIB"
        hdr["HIERARCH ESO DPR TYPE"] = "FLAT,LAMP"
        hdr["HIERARCH ESO DPR TECH"] = "IMAGE,LM"
        path = tmp_path / "flat.fits"
        afits.HDUList([afits.PrimaryHDU(header=hdr)]).writeto(path)

        assert classify_fits_file(path) == "LM_FLAT_LAMP_RAW"

    def test_classifies_ifu_dark(self, tmp_path):
        pytest.importorskip("astropy")
        from astropy.io import fits as afits

        hdr = afits.Header()
        hdr["HIERARCH ESO DPR CATG"] = "CALIB"
        hdr["HIERARCH ESO DPR TYPE"] = "DARK"
        hdr["HIERARCH ESO DPR TECH"] = "IFU"
        path = tmp_path / "dark.fits"
        afits.HDUList([afits.PrimaryHDU(header=hdr)]).writeto(path)

        assert classify_fits_file(path) == "DARK_IFU_RAW"

    def test_unknown_triple_returns_none(self, tmp_path):
        pytest.importorskip("astropy")
        from astropy.io import fits as afits

        hdr = afits.Header()
        hdr["HIERARCH ESO DPR CATG"] = "UNKNOWN"
        hdr["HIERARCH ESO DPR TYPE"] = "XYZ"
        hdr["HIERARCH ESO DPR TECH"] = "MADEUP"
        path = tmp_path / "u.fits"
        afits.HDUList([afits.PrimaryHDU(header=hdr)]).writeto(path)

        assert classify_fits_file(path) is None

    def test_pro_catg_fallback(self, tmp_path):
        pytest.importorskip("astropy")
        from astropy.io import fits as afits

        hdr = afits.Header()
        hdr["HIERARCH ESO PRO CATG"] = "MASTER_DARK_2RG"
        path = tmp_path / "master.fits"
        afits.HDUList([afits.PrimaryHDU(header=hdr)]).writeto(path)

        assert classify_fits_file(path) == "MASTER_DARK_2RG"

    def test_headerless_fits_returns_none(self, tmp_path):
        pytest.importorskip("astropy")
        from astropy.io import fits as afits

        path = tmp_path / "bare.fits"
        afits.HDUList([afits.PrimaryHDU()]).writeto(path)

        assert classify_fits_file(path) is None


# ---------------------------------------------------------------------------
# _build_sim_script
# ---------------------------------------------------------------------------

class TestBuildSimScript:
    _base_kwargs = dict(
        out_dir="/tmp/sim",
        do_calib=False,
        do_static=True,
        n_cores=4,
        input_list=["/data/obs.yaml"],
        sims_root="/fake/METIS_Simulations",
    )

    def test_default_runner_includes_inst_pkgs_override(self):
        script = _build_sim_script(
            **self._base_kwargs,
            inst_pkgs_path="/home/user/.local/share/metis-test-runner/inst_pkgs",
        )
        assert "local_packages_path" in script
        assert "/home/user/.local/share/metis-test-runner/inst_pkgs" in script

    def test_native_runner_omits_inst_pkgs_override(self):
        script = _build_sim_script(**self._base_kwargs, inst_pkgs_path=None)
        assert "local_packages_path" not in script

    def test_script_contains_output_dir(self):
        script = _build_sim_script(**self._base_kwargs)
        assert "/tmp/sim" in script

    def test_script_contains_input_list(self):
        script = _build_sim_script(**self._base_kwargs)
        assert "/data/obs.yaml" in script

    def test_script_contains_csv_path_unchanged(self):
        """CSV paths must pass through to the generated script verbatim; the
        downstream metis_simulations loader dispatches by extension."""
        script = _build_sim_script(
            **{**self._base_kwargs, "input_list": ["/data/obs.csv"]}
        )
        assert "/data/obs.csv" in script
        assert "runSimulationBlock(['/data/obs.csv']" in script

    def test_script_contains_mixed_input_paths(self):
        script = _build_sim_script(
            **{**self._base_kwargs,
               "input_list": ["/data/obs.yaml", "/data/obs.csv"]}
        )
        assert "/data/obs.yaml" in script
        assert "/data/obs.csv" in script

    def test_script_is_valid_python(self):
        import ast
        script = _build_sim_script(**self._base_kwargs)
        # Should not raise
        ast.parse(script)

    def test_script_with_static_calibs_is_valid_python(self):
        import ast
        script = _build_sim_script(
            **self._base_kwargs,
            static_calibs_dir="/output/static_calibs",
        )
        ast.parse(script)

    def test_script_with_inst_pkgs_is_valid_python(self):
        import ast
        script = _build_sim_script(
            **self._base_kwargs,
            inst_pkgs_path="/home/user/inst_pkgs",
        )
        ast.parse(script)

    def test_script_contains_download_logic_when_inst_pkgs_set(self):
        script = _build_sim_script(
            **self._base_kwargs,
            inst_pkgs_path="/home/user/inst_pkgs",
        )
        assert "download_packages" in script
        assert "'METIS'" in script

    def test_script_omits_download_logic_when_no_inst_pkgs(self):
        script = _build_sim_script(**self._base_kwargs, inst_pkgs_path=None)
        assert "download_packages" not in script

    def test_script_contains_error_hint_with_inst_pkgs(self):
        script = _build_sim_script(
            **self._base_kwargs,
            inst_pkgs_path="/home/user/inst_pkgs",
        )
        assert "Package could not be found" in script
        assert "HINT:" in script
        assert "/home/user/inst_pkgs" in script

    def test_script_contains_error_hint_without_inst_pkgs(self):
        script = _build_sim_script(**self._base_kwargs, inst_pkgs_path=None)
        assert "Package could not be found" in script
        assert "HINT:" in script
        assert "No instrument packages path was configured" in script

    def test_script_uses_package_import(self):
        script = _build_sim_script(**self._base_kwargs)
        assert "from metis_simulations import runSimulationBlock" in script
        assert "\nimport runSimulationBlock" not in script

    def test_script_passes_args_to_runSimulationBlock(self):
        script = _build_sim_script(**self._base_kwargs)
        assert "params, [])" in script

    def test_script_do_static_never_set_in_params(self):
        # doStatic is always False in the params dict — static calibration
        # generation is handled separately via the cached generateStaticCalibs
        # call, not via runSimulationBlock()'s internal doStatic path.
        for val in (True, False, 1, 0):
            script = _build_sim_script(**{**self._base_kwargs, "do_static": val})
            assert "doStatic  = False" in script
            assert "params, [])" in script

    def test_script_generates_static_calibs_to_cache(self):
        script = _build_sim_script(
            **{**self._base_kwargs, "do_static": True},
            static_calibs_dir="/output/static_calibs",
        )
        assert "generateStaticCalibs" in script
        assert "/output/static_calibs" in script
        # Should check for existing files before regenerating.
        assert "PERSISTENCE_MAP_LM.fits" in script

    def test_script_skips_static_calibs_when_disabled(self):
        script = _build_sim_script(
            **{**self._base_kwargs, "do_static": False},
            static_calibs_dir="/output/static_calibs",
        )
        assert "generateStaticCalibs" not in script

    def test_script_skips_static_calibs_when_no_cache_dir(self):
        script = _build_sim_script(
            **{**self._base_kwargs, "do_static": True},
            static_calibs_dir=None,
        )
        assert "generateStaticCalibs" not in script

    def test_script_testrun_and_writeyaml_false_by_default(self):
        script = _build_sim_script(**self._base_kwargs)
        assert "testRun   = False" in script
        assert "writeYaml = False" in script

    def test_script_write_yaml_sets_testrun_and_writeyaml(self):
        # CSV->YAML dry run: both testRun and writeYaml must be True so
        # metis_simulations translates the CSV and skips simulation.
        import ast
        script = _build_sim_script(**self._base_kwargs, write_yaml=True)
        assert "testRun   = True" in script
        assert "writeYaml = True" in script
        ast.parse(script)  # still valid python

    def test_script_sys_path_uses_sims_root(self):
        script = _build_sim_script(**self._base_kwargs)
        assert "/fake/METIS_Simulations" in script
        assert 'sys.path.insert(0, "python")' not in script

    def test_script_sets_scipy_datasets_dir(self):
        # metis_simulations.sources calls scipy.datasets.face() at import
        # time, which writes to ~/.cache/scipy-data. On read-only home
        # environments this raises PermissionError; the generated script
        # must redirect the cache to a writable temp location before the
        # metis_simulations import.
        script = _build_sim_script(**self._base_kwargs)
        assert "SCIPY_DATASETS_DIR" in script
        assert "setdefault" in script
        # Must precede the metis_simulations import or the env var has no
        # effect on the eager scipy.datasets.face() call.
        assert script.index("SCIPY_DATASETS_DIR") < script.index(
            "from metis_simulations"
        )

    # -- macOS spawn-safety guards ------------------------------------------

    def test_script_has_main_guard(self):
        """Generated script must wrap the simulation call in an
        ``if __name__ == "__main__":`` guard so that macOS spawn-mode
        multiprocessing workers do not re-execute the simulation."""
        script = _build_sim_script(**self._base_kwargs)
        assert 'if __name__ == "__main__":' in script

    def test_simulation_call_inside_main_guard(self):
        """runSimulationBlock() call (not the import) must appear only
        inside the __main__ guard, never at module level."""
        script = _build_sim_script(**self._base_kwargs)
        lines = script.splitlines()
        guard_line = next(
            i for i, l in enumerate(lines)
            if '__name__' in l and '__main__' in l
        )
        sim_call_lines = [
            i for i, l in enumerate(lines)
            if 'runSimulationBlock' in l and 'import' not in l
        ]
        for idx in sim_call_lines:
            assert idx > guard_line, (
                f"runSimulationBlock call at line {idx} is before the "
                f"__main__ guard at line {guard_line}"
            )
            assert lines[idx].startswith("    "), (
                f"runSimulationBlock call at line {idx} is not indented "
                f"under the __main__ guard"
            )

    def test_monkey_patch_outside_main_guard(self):
        """The skycalc_ipy monkey-patch must remain at module level so that
        spawn-mode workers execute it when they re-import __main__."""
        script = _build_sim_script(**self._base_kwargs)
        lines = script.splitlines()
        guard_line = next(
            i for i, l in enumerate(lines)
            if '__name__' in l and '__main__' in l
        )
        patch_lines = [
            i for i, l in enumerate(lines)
            if '_skc_safe_call' in l or '_skc_orig_call' in l
        ]
        assert patch_lines, "Monkey-patch lines not found in generated script"
        for idx in patch_lines:
            assert idx < guard_line, (
                f"Monkey-patch at line {idx} should be before the "
                f"__main__ guard at line {guard_line}"
            )

    def test_static_calibs_inside_main_guard(self):
        """Static calibration generation must also be inside the guard."""
        script = _build_sim_script(
            **{**self._base_kwargs, "do_static": True},
            static_calibs_dir="/output/static_calibs",
        )
        lines = script.splitlines()
        guard_line = next(
            i for i, l in enumerate(lines)
            if '__name__' in l and '__main__' in l
        )
        static_lines = [
            i for i, l in enumerate(lines)
            if 'generateStaticCalibs' in l
        ]
        for idx in static_lines:
            assert idx > guard_line
            assert lines[idx].startswith("    ")


# ---------------------------------------------------------------------------
# Spawn-mode safety (simulates macOS multiprocessing behavior on Linux)
# ---------------------------------------------------------------------------

class TestSpawnSafety:
    """Verify generated scripts survive multiprocessing spawn mode
    (the default on macOS since Python 3.8)."""

    @staticmethod
    def _mock_modules():
        """Build a dict of mocked modules for exec'ing the generated script.

        ``from metis_simulations import runSimulationBlock as rsb`` resolves
        *rsb* to ``sys.modules["metis_simulations"].runSimulationBlock``, so
        we wire the mock_rsb_module into both places.
        """
        from unittest.mock import MagicMock

        mock_rsb_module = MagicMock()
        mock_metis = MagicMock()
        mock_metis.runSimulationBlock = mock_rsb_module
        return {
            "scopesim": MagicMock(),
            "scopesim.rc": MagicMock(),
            "skycalc_ipy": MagicMock(),
            "skycalc_ipy.core": MagicMock(),
            "metis_simulations": mock_metis,
            "metis_simulations.runSimulationBlock": mock_rsb_module,
        }, mock_rsb_module

    _base_kwargs = dict(
        out_dir="/tmp/sim",
        do_calib=False,
        do_static=False,
        n_cores=4,
        input_list=["/data/obs.yaml"],
        sims_root="/fake/METIS_Simulations",
    )

    def test_spawn_worker_does_not_re_execute_simulation(self):
        """When a spawn-mode worker re-imports __main__, __name__ is set to
        '__mp_main__'.  The simulation call must NOT execute in that case."""
        from unittest.mock import patch

        script = _build_sim_script(**self._base_kwargs)
        code = compile(script, "<spawn_test>", "exec")
        mock_modules, mock_rsb = self._mock_modules()

        ns = {"__name__": "__mp_main__", "__builtins__": __builtins__}
        with patch.dict("sys.modules", mock_modules):
            exec(code, ns)

        mock_rsb.runSimulationBlock.assert_not_called()

    def test_main_process_does_execute_simulation(self):
        """When __name__ is "__main__" (the parent process), the simulation
        call must execute exactly once."""
        from unittest.mock import patch

        script = _build_sim_script(**self._base_kwargs)
        code = compile(script, "<main_test>", "exec")
        mock_modules, mock_rsb = self._mock_modules()

        ns = {"__name__": "__main__", "__builtins__": __builtins__}
        with patch.dict("sys.modules", mock_modules):
            exec(code, ns)

        mock_rsb.runSimulationBlock.assert_called_once()


# ---------------------------------------------------------------------------
# _edps_base_cmd, _check_default_env, _default_subprocess_env
# ---------------------------------------------------------------------------

class TestEdpsBaseCmd:
    def test_default_runner_calls_edps_directly(self):
        cmd = _edps_base_cmd("default", None, 4444)
        assert cmd == ["edps", "-P", "4444"]

    def test_native_runner_calls_edps_directly(self):
        cmd = _edps_base_cmd("native", None, 5000)
        assert cmd == ["edps", "-P", "5000"]

    def test_docker_runner_wraps_with_exec(self):
        cmd = _edps_base_cmd("docker", "my-container", 4444)
        assert cmd[:3] == ["docker", "exec", "my-container"]
        assert "edps" in cmd

    def test_podman_runner_wraps_with_exec(self):
        cmd = _edps_base_cmd("podman", "my-container", 4444)
        assert cmd[:3] == ["podman", "exec", "my-container"]
        assert "edps" in cmd


class TestCheckDefaultEnv:
    def test_raises_when_pipeline_clone_missing(self, tmp_path):
        missing = tmp_path / "nope"
        with patch("metis_test_runner.run_metis.paths.pipeline_dir",
                   return_value=missing):
            with pytest.raises(FileNotFoundError, match="Install tab"):
                _check_default_env("default")

    def test_returns_silently_when_pipeline_clone_present(self, tmp_path):
        clone = tmp_path / "METIS_Pipeline"
        (clone / ".git").mkdir(parents=True)
        with patch("metis_test_runner.run_metis.paths.pipeline_dir",
                   return_value=clone):
            _check_default_env("default")  # no exception

    def test_noop_for_non_default_runners(self, tmp_path):
        missing = tmp_path / "nope"
        with patch("metis_test_runner.run_metis.paths.pipeline_dir",
                   return_value=missing):
            for r in ("native", "docker", "podman"):
                _check_default_env(r)  # no exception even though clone is missing


class TestDefaultSubprocessEnv:
    def test_loads_dotenv_and_prepends_venv_bin(self, tmp_path):
        import os
        import sys
        env_path = tmp_path / ".env"
        env_path.write_text("FOO=bar\nMTR_TEST_KEY=value123\n")
        with patch("metis_test_runner.run_metis.paths.env_file", return_value=env_path):
            env = _default_subprocess_env()
        assert env["FOO"] == "bar"
        assert env["MTR_TEST_KEY"] == "value123"
        venv_bin = str(Path(sys.executable).parent)
        assert env["PATH"].split(os.pathsep)[0] == venv_bin

    def test_works_without_dotenv_file(self, tmp_path):
        import os
        import sys
        missing = tmp_path / "nope" / ".env"
        with patch("metis_test_runner.run_metis.paths.env_file", return_value=missing):
            env = _default_subprocess_env()
        venv_bin = str(Path(sys.executable).parent)
        assert env["PATH"].split(os.pathsep)[0] == venv_bin


# ---------------------------------------------------------------------------
# Lookup table completeness / consistency checks
# ---------------------------------------------------------------------------

class TestLookupTableConsistency:
    def test_all_tech_to_workflow_values_are_known_workflows(self):
        known = set(WORKFLOW_TASK_CHAIN)
        for tech, wf in TECH_TO_WORKFLOW.items():
            assert wf in known, f"TECH_TO_WORKFLOW[{tech!r}] = {wf!r} not in WORKFLOW_TASK_CHAIN"

    def test_all_mode_to_workflow_values_are_known_workflows(self):
        known = set(WORKFLOW_TASK_CHAIN)
        for mode, wf in MODE_TO_WORKFLOW.items():
            assert wf in known, f"MODE_TO_WORKFLOW[{mode!r}] = {wf!r} not in WORKFLOW_TASK_CHAIN"

    def test_each_workflow_task_chain_has_at_least_one_entry(self):
        for wf, chain in WORKFLOW_TASK_CHAIN.items():
            assert len(chain) >= 1, f"Empty task chain for {wf!r}"

    def test_workflow_task_chain_tuples_have_three_elements(self):
        for wf, chain in WORKFLOW_TASK_CHAIN.items():
            for entry in chain:
                assert len(entry) == 3, (
                    f"Task chain entry in {wf!r} does not have 3 elements: {entry!r}"
                )

    def test_meta_targets_only_valid_values(self):
        valid = {None, "qc1calib", "science"}
        for wf, chain in WORKFLOW_TASK_CHAIN.items():
            for _, _, meta in chain:
                assert meta in valid, (
                    f"Unknown meta_target {meta!r} in workflow {wf!r}"
                )

    def test_dpr_to_tag_keys_are_three_tuples(self):
        for key in DPR_TO_TAG:
            assert isinstance(key, tuple) and len(key) == 3, (
                f"DPR_TO_TAG key is not a 3-tuple: {key!r}"
            )

    def test_ifu_workflow_science_task_has_science_meta_target(self):
        """The IFU sci_reduce task must be gated by 'science'."""
        chain = dict(
            (name, meta)
            for name, _, meta in WORKFLOW_TASK_CHAIN["metis.metis_ifu_wkf"]
        )
        assert chain.get("metis_ifu_sci_reduce") == "science"

    def test_lm_img_workflow_calib_tasks_have_no_meta_target(self):
        """LM IMG calibration tasks (lingain, dark, flat, distortion) must have no meta-target."""
        calib_tasks = {
            "metis_lm_img_lingain",
            "metis_lm_img_dark",
            "metis_lm_img_flat",
            "metis_lm_img_distortion",
        }
        for name, _, meta in WORKFLOW_TASK_CHAIN["metis.metis_lm_img_wkf"]:
            if name in calib_tasks:
                assert meta is None, (
                    f"Expected no meta_target for {name!r}, got {meta!r}"
                )

    def test_lss_calib_tasks_gated_by_qc1calib(self):
        """All non-science tasks in LSS workflows must be qc1calib-gated."""
        for wf in ("metis.metis_lm_lss_wkf", "metis.metis_n_lss_wkf"):
            for name, _, meta in WORKFLOW_TASK_CHAIN[wf]:
                if meta != "science":
                    assert meta == "qc1calib", (
                        f"LSS task {name!r} in {wf!r} expected qc1calib, got {meta!r}"
                    )


# ---------------------------------------------------------------------------
# --pipeline-input  (multi-directory support)
# ---------------------------------------------------------------------------

class TestPipelineInputArg:
    """Verify that --pipeline-input accepts multiple directories via action='append'."""

    def test_single_pipeline_input(self):
        args = parse_args(["--no-sim", "--pipeline-input", "/tmp/a"])
        assert args.pipeline_input == ["/tmp/a"]

    def test_multiple_pipeline_inputs(self):
        args = parse_args([
            "--no-sim",
            "--pipeline-input", "/tmp/a",
            "--pipeline-input", "/tmp/b",
        ])
        assert args.pipeline_input == ["/tmp/a", "/tmp/b"]

    def test_no_pipeline_input_is_none(self):
        args = parse_args(["--no-sim"])
        assert args.pipeline_input is None

    def test_pipeline_input_without_no_sim(self):
        """--pipeline-input is accepted even without --no-sim (main() ignores it)."""
        args = parse_args(["--pipeline-input", "/tmp/a", "file.yaml"])
        assert args.pipeline_input == ["/tmp/a"]


# ---------------------------------------------------------------------------
# Input file positional argument
# ---------------------------------------------------------------------------

class TestInputFilesArg:
    def test_positional_attribute_is_input_files(self):
        args = parse_args(["foo.yaml", "bar.csv"])
        assert args.input_files == ["foo.yaml", "bar.csv"]

    def test_no_positionals_is_empty_list(self):
        args = parse_args(["--no-sim"])
        assert args.input_files == []

    def test_csv_path_accepted_as_positional(self):
        args = parse_args(["seq.csv"])
        assert args.input_files == ["seq.csv"]


class TestCsvToYamlFlag:
    def test_default_is_false(self):
        assert parse_args(["foo.csv"]).csv_to_yaml is False

    def test_flag_sets_attribute(self):
        assert parse_args(["--csv-to-yaml", "foo.csv"]).csv_to_yaml is True


# ---------------------------------------------------------------------------
# --csv-lines: parse + slice
# ---------------------------------------------------------------------------

# A minimal AIT-format CSV: column-name row + 3 metadata rows + 5 data rows.
_AIT_CSV = (
    "test_ID,step_number,DPR.TECH\n"      # line 1 (header)
    "component,component,component\n"      # line 2 (header)
    "description,description,description\n"  # line 3 (header)
    "type,type,type\n"                     # line 4 (header)
    "LM_IMG_DARK,1,IMAGE_LM\n"             # line 5 (data row 1)
    "LINGAIN,1,IMAGE_LM\n"                 # line 6 (data row 2)
    "LINGAIN,2,IMAGE_LM\n"                 # line 7 (data row 3)
    "LINGAIN,3,IMAGE_LM\n"                 # line 8 (data row 4)
    "LINGAIN,4,IMAGE_LM\n"                 # line 9 (data row 5)
)


class TestParseLineRange:
    def test_both_bounds(self):
        assert _parse_line_range("6:12") == (6, 12)

    def test_open_end(self):
        assert _parse_line_range("6:") == (6, None)

    def test_open_start(self):
        assert _parse_line_range(":12") == (None, 12)

    def test_both_open(self):
        assert _parse_line_range(":") == (None, None)

    def test_missing_colon_rejected(self):
        with pytest.raises(argparse.ArgumentTypeError):
            _parse_line_range("6")

    def test_non_positive_rejected(self):
        with pytest.raises(argparse.ArgumentTypeError):
            _parse_line_range("0:5")

    def test_non_integer_rejected(self):
        with pytest.raises(argparse.ArgumentTypeError):
            _parse_line_range("abc:5")

    def test_start_after_end_rejected(self):
        with pytest.raises(argparse.ArgumentTypeError):
            _parse_line_range("12:6")


class TestSliceCsv:
    def _write(self, tmp_path, content=_AIT_CSV):
        p = tmp_path / "seq.csv"
        p.write_text(content)
        return p

    def test_header_preserved_and_data_range_selected(self, tmp_path):
        src = self._write(tmp_path)
        out = _slice_csv(src, (6, 8), tmp_path / "sliced")
        lines = out.read_text().splitlines()
        # 4 header rows + file lines 6-8 (3 data rows); line 5 dropped.
        assert lines[:4] == [
            "test_ID,step_number,DPR.TECH",
            "component,component,component",
            "description,description,description",
            "type,type,type",
        ]
        assert lines[4:] == [
            "LINGAIN,1,IMAGE_LM",
            "LINGAIN,2,IMAGE_LM",
            "LINGAIN,3,IMAGE_LM",
        ]
        assert "LM_IMG_DARK,1,IMAGE_LM" not in lines  # line 5 excluded

    def test_output_written_next_to_out_dir_with_same_name(self, tmp_path):
        src = self._write(tmp_path)
        out = _slice_csv(src, (6, 8), tmp_path / "sliced")
        assert out == tmp_path / "sliced" / "seq.csv"
        assert out.exists()

    def test_open_end_keeps_through_eof(self, tmp_path):
        src = self._write(tmp_path)
        out = _slice_csv(src, (7, None), tmp_path / "sliced")
        lines = out.read_text().splitlines()
        assert lines[4:] == [
            "LINGAIN,2,IMAGE_LM",
            "LINGAIN,3,IMAGE_LM",
            "LINGAIN,4,IMAGE_LM",
        ]

    def test_open_start_counts_from_line_one(self, tmp_path):
        src = self._write(tmp_path)
        out = _slice_csv(src, (None, 6), tmp_path / "sliced")
        lines = out.read_text().splitlines()
        # lo defaults to 1, so data lines 5-6 are kept.
        assert lines[4:] == [
            "LM_IMG_DARK,1,IMAGE_LM",
            "LINGAIN,1,IMAGE_LM",
        ]

    def test_zero_data_rows_warns(self, tmp_path, capsys):
        src = self._write(tmp_path)
        out = _slice_csv(src, (100, 200), tmp_path / "sliced")
        lines = out.read_text().splitlines()
        assert len(lines) == 4  # header only
        assert "selected no data rows" in capsys.readouterr().err

    def test_fixed_four_row_header_kept_with_blank_metadata_cells(self, tmp_path):
        # Real AIT sheets leave test_ID/templateName/step_number blank in the
        # three metadata rows, so their first cell is empty. The fixed 4-row
        # header must still be preserved in full — otherwise csvParser's four
        # unconditional next(reader) calls run off the end (StopIteration).
        content = (
            "test_ID,step_number,DPR.TECH\n"    # line 1: column ids
            ",,Data product technique\n"         # line 2: descriptions (blank 1st)
            ",,keyword\n"                         # line 3: data types (blank 1st)
            ",,keyword\n"                         # line 4: (blank 1st)
            "ROW_A,1,IFU\n"                       # line 5: data
            "ROW_B,2,IFU\n"                       # line 6: data
            "ROW_C,3,IFU\n"                       # line 7: data
        )
        src = tmp_path / "real.csv"
        src.write_text(content)
        out = _slice_csv(src, (6, 6), tmp_path / "sliced")
        lines = out.read_text().splitlines()
        assert lines[:4] == [
            "test_ID,step_number,DPR.TECH",
            ",,Data product technique",
            ",,keyword",
            ",,keyword",
        ]
        assert lines[4:] == ["ROW_B,2,IFU"]  # only the selected data row


class TestCsvLinesFlag:
    def test_default_is_none(self):
        assert parse_args(["foo.csv"]).csv_lines is None

    def test_parsed_into_tuple(self):
        assert parse_args(["--csv-lines", "6:12", "foo.csv"]).csv_lines == (6, 12)

    def test_malformed_rejected(self):
        with pytest.raises(SystemExit):
            parse_args(["--csv-lines", "nope", "foo.csv"])
