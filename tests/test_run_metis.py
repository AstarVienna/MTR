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

from run_metis import (
    DPR_TO_TAG,
    MODE_TO_WORKFLOW,
    TECH_TO_WORKFLOW,
    WORKFLOW_TASK_CHAIN,
    _build_sim_script,
    _edps_base_cmd,
    _edps_cwd,
    classify_fits_file,
    collect_tags_from_fits,
    infer_edps_target,
    infer_workflow,
    parse_args,
    read_edps_port,
)


# ---------------------------------------------------------------------------
# read_edps_port
# ---------------------------------------------------------------------------

class TestReadEdpsPort:
    def test_returns_default_when_file_missing(self, tmp_path):
        """No application.properties → falls back to default."""
        with patch("run_metis.Path.home", return_value=tmp_path):
            assert read_edps_port(default=5000) == 5000

    def test_reads_port_from_properties_file(self, tmp_path):
        props_dir = tmp_path / ".edps"
        props_dir.mkdir()
        (props_dir / "application.properties").write_text("port=4444\n")
        with patch("run_metis.Path.home", return_value=tmp_path):
            assert read_edps_port() == 4444

    def test_ignores_malformed_port_value(self, tmp_path):
        props_dir = tmp_path / ".edps"
        props_dir.mkdir()
        (props_dir / "application.properties").write_text("port=not_a_number\n")
        with patch("run_metis.Path.home", return_value=tmp_path):
            assert read_edps_port(default=5000) == 5000

    def test_warns_on_malformed_port_value(self, tmp_path, capsys):
        # Silent fallback to default is confusing: user thinks EDPS is on
        # 5000 while the real config has a typo. We want a stderr breadcrumb.
        props_dir = tmp_path / ".edps"
        props_dir.mkdir()
        (props_dir / "application.properties").write_text("port=4444, 5555\n")
        with patch("run_metis.Path.home", return_value=tmp_path):
            assert read_edps_port(default=5000) == 5000
        err = capsys.readouterr().err
        assert "malformed" in err
        assert "4444, 5555" in err

    def test_picks_port_from_multiline_file(self, tmp_path):
        props_dir = tmp_path / ".edps"
        props_dir.mkdir()
        content = "server.host=localhost\nport=9999\nsome.other=value\n"
        (props_dir / "application.properties").write_text(content)
        with patch("run_metis.Path.home", return_value=tmp_path):
            assert read_edps_port() == 9999

    def test_custom_default_returned_when_no_file(self, tmp_path):
        with patch("run_metis.Path.home", return_value=tmp_path):
            assert read_edps_port(default=1234) == 1234


# ---------------------------------------------------------------------------
# infer_workflow
# ---------------------------------------------------------------------------

def _write_yaml(tmp_path, name, content):
    p = tmp_path / name
    p.write_text(textwrap.dedent(content))
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
        yaml_list=["/data/obs.yaml"],
        sims_root="/fake/METIS_Simulations",
    )

    def test_metapkg_runner_includes_inst_pkgs_override(self):
        script = _build_sim_script(
            **self._base_kwargs,
            inst_pkgs_path="/home/user/metis-meta-package/inst_pkgs",
        )
        assert "local_packages_path" in script
        assert "/home/user/metis-meta-package/inst_pkgs" in script

    def test_native_runner_omits_inst_pkgs_override(self):
        script = _build_sim_script(**self._base_kwargs, inst_pkgs_path=None)
        assert "local_packages_path" not in script

    def test_script_contains_output_dir(self):
        script = _build_sim_script(**self._base_kwargs)
        assert "/tmp/sim" in script

    def test_script_contains_yaml_list(self):
        script = _build_sim_script(**self._base_kwargs)
        assert "/data/obs.yaml" in script

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
        yaml_list=["/data/obs.yaml"],
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
# _edps_base_cmd and _edps_cwd
# ---------------------------------------------------------------------------

class TestEdpsBaseCmd:
    def test_metapkg_runner_uses_uv(self, tmp_path):
        meta_pkg = tmp_path / "meta"
        meta_pkg.mkdir()
        (meta_pkg / ".env").write_text("")
        cmd = _edps_base_cmd("metapkg", None, 4444, meta_pkg=meta_pkg)
        assert cmd[:3] == ["uv", "run", "--no-sync"]
        assert "edps" in cmd
        assert "-P" in cmd
        assert "4444" in cmd

    def test_native_runner_calls_edps_directly(self):
        cmd = _edps_base_cmd("native", None, 5000)
        assert cmd[0] == "edps"
        assert "-P" in cmd
        assert "5000" in cmd
        assert "uv" not in cmd

    def test_docker_runner_wraps_with_exec(self):
        cmd = _edps_base_cmd("docker", "my-container", 4444)
        assert cmd[:3] == ["docker", "exec", "my-container"]
        assert "edps" in cmd

    def test_podman_runner_wraps_with_exec(self):
        cmd = _edps_base_cmd("podman", "my-container", 4444)
        assert cmd[:3] == ["podman", "exec", "my-container"]
        assert "edps" in cmd

    def test_metapkg_runner_raises_when_env_file_missing(self, tmp_path):
        # uv's own error for a missing --env-file is correct but unhelpful;
        # we want a loud pre-flight pointing the user at the Install tab.
        meta_pkg = tmp_path / "meta"
        meta_pkg.mkdir()
        # No .env written
        with pytest.raises(FileNotFoundError, match="Install tab"):
            _edps_base_cmd("metapkg", None, 4444, meta_pkg=meta_pkg)


class TestEdpsCwd:
    def test_metapkg_runner_returns_meta_pkg_path(self, tmp_path):
        assert _edps_cwd("metapkg", meta_pkg=tmp_path) == str(tmp_path)

    def test_native_runner_returns_none(self):
        assert _edps_cwd("native") is None

    def test_docker_runner_returns_none(self):
        assert _edps_cwd("docker") is None

    def test_podman_runner_returns_none(self):
        assert _edps_cwd("podman") is None


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
