"""archive.py — METIS archive integration module

Provides Podman lifecycle management, archive data server client operations,
and master-calibration auto-download logic.  All archive operations that need
the MetisWISE / commonwise API execute Python code *inside* the running
archive container via ``podman exec``, so no host-side dependency on those
packages is required.

The archive stack is a minimal set of Podman containers (PostgreSQL +
dataserver) whose configuration files are generated into
``<repo_root>/archive_stack/``.
"""

from __future__ import annotations

import json
import os
import platform
import shutil
import subprocess
import textwrap
from pathlib import Path
from typing import Callable

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).parent.resolve()
ARCHIVE_COMPOSE_DIR = REPO_ROOT / "archive_stack"
ARCHIVE_IMAGE_NAME = "metis_archive"

# ---------------------------------------------------------------------------
# Section A — OS detection & Podman helpers
# ---------------------------------------------------------------------------


def detect_os() -> tuple[str, str]:
    """Return *(os_family, distro_id)*.

    *os_family* is ``'linux'``, ``'darwin'``, or ``'windows'``.
    *distro_id* is the lowercase ``ID`` field from ``/etc/os-release`` on
    Linux (e.g. ``'ubuntu'``, ``'fedora'``, ``'arch'``), ``'macos'`` on
    macOS, or ``'unknown'``.
    """
    system = platform.system().lower()
    if system == "darwin":
        return ("darwin", "macos")
    if system == "windows":
        return ("windows", "unknown")
    # Linux — read /etc/os-release
    os_release = Path("/etc/os-release")
    if os_release.exists():
        for line in os_release.read_text().splitlines():
            if line.startswith("ID="):
                distro = line.split("=", 1)[1].strip().strip('"').lower()
                return ("linux", distro)
    return ("linux", "unknown")


def podman_available() -> bool:
    """Return ``True`` if ``podman`` is on *PATH* and responds to ``--version``."""
    try:
        subprocess.run(
            ["podman", "--version"],
            capture_output=True, timeout=10,
        )
        return True
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def podman_compose_available() -> bool:
    """Return ``True`` if ``podman-compose`` is on *PATH*."""
    try:
        subprocess.run(
            ["podman-compose", "--version"],
            capture_output=True, timeout=10,
        )
        return True
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def install_podman_commands(distro_id: str) -> list[list[str]]:
    """Return the shell commands to install Podman for *distro_id*.

    Returns a list of command lists (steps) to execute in order.  For
    apt-based distros this includes an ``apt-get update`` step before
    the install.

    ``sudo`` is prepended when the current user is not root.  If neither
    root nor ``sudo`` is available, raises :class:`RuntimeError`.

    Raises :class:`ValueError` for unrecognised distributions.
    """
    if os.getuid() == 0:
        prefix: list[str] = []
    elif shutil.which("sudo") is not None:
        prefix = ["sudo"]
    else:
        raise RuntimeError(
            "Root privileges are required to install packages, but this "
            "process is not running as root and 'sudo' is not available.\n"
            "Either run as root or install sudo first:\n"
            "  apt-get install sudo   (as root)"
        )
    commands: dict[str, list[list[str]]] = {
        "debian": [
            prefix + ["apt-get", "update", "-q"],
            prefix + ["apt-get", "install", "-y", "podman", "podman-compose"],
        ],
        "ubuntu": [
            prefix + ["apt-get", "update", "-q"],
            prefix + ["apt-get", "install", "-y", "podman", "podman-compose"],
        ],
        "fedora":  [prefix + ["dnf", "install", "-y", "podman", "podman-compose"]],
        "centos":  [prefix + ["dnf", "install", "-y", "podman", "podman-compose"]],
        "rhel":    [prefix + ["dnf", "install", "-y", "podman", "podman-compose"]],
        "arch":    [prefix + ["pacman", "-Sy", "--noconfirm", "podman", "podman-compose"]],
        "manjaro": [prefix + ["pacman", "-Sy", "--noconfirm", "podman", "podman-compose"]],
        "opensuse-tumbleweed": [prefix + ["zypper", "install", "-y", "podman", "podman-compose"]],
        "opensuse-leap":       [prefix + ["zypper", "install", "-y", "podman", "podman-compose"]],
        "macos":   [["brew", "install", "podman", "podman-compose"]],
    }
    if distro_id not in commands:
        raise ValueError(
            f"Unsupported distribution: {distro_id!r}.  "
            f"Known: {', '.join(sorted(commands))}"
        )
    return commands[distro_id]


# ---------------------------------------------------------------------------
# Section B — Container lifecycle
# ---------------------------------------------------------------------------

_COMPOSE_YML = textwrap.dedent("""\
    services:
      postgres:
        image: docker.io/library/postgres:latest
        environment:
          - "POSTGRES_DB=wise"
          - "POSTGRES_USER=system"
          - "POSTGRES_PASSWORD=klmn"
        volumes:
          - ./space:/root/space
      dataserver:
        image: {image}:latest
        ports:
          - "127.0.0.1:8013:8013"
        entrypoint: /root/scripts/entrypoint_dataserver.sh
        volumes:
          - ./scripts:/root/scripts
          - ./space:/root/space
        depends_on:
          - postgres
""")

_CONTAINERFILE = textwrap.dedent("""\
    # Minimal METIS archive image — dataserver + MetisWISE only.
    # Build:
    #   export OMEGACEN_CREDENTIALS=username:password
    #   podman build --secret=id=OMEGACEN_CREDENTIALS,type=env \\
    #       -t {image} .
    FROM quay.io/condaforge/miniforge3:25.3.0-3

    RUN apt-get update && \\
        apt-get install -y --no-install-recommends \\
            media-types postgresql openssl curl && \\
        rm -rf /var/lib/apt/lists/*

    RUN --mount=type=secret,id=OMEGACEN_CREDENTIALS \\
        conda config --add channels conda-forge && \\
        conda config --add channels omegacen && \\
        conda config --add channels \\
            "https://$(cat /run/secrets/OMEGACEN_CREDENTIALS)@conda.astro-wise.org/" && \\
        conda install -y common dataserver psycopg2

    RUN --mount=type=secret,id=OMEGACEN_CREDENTIALS \\
        pip install --no-cache-dir \\
            --extra-index-url \\
            "https://$(cat /run/secrets/OMEGACEN_CREDENTIALS)@pip.entropynaut.com/packages/" \\
            metiswise

    RUN mkdir -p /root/space/dataserver /root/scripts /root/.awe
    COPY scripts/Environment.cfg /root/.awe/Environment.cfg

    ENV AWETARGET=metiswise
    WORKDIR /root
""")

_DS_CFG = textwrap.dedent("""\
    [global]
    directdataaccess        : False
    email                   : user@localhost
    hosts                   : localhost
    maxfiles                : 8192
    maxsearchthreads        : 10
    maxthreads              : 750
    ports                   : 18008
    query                   : True
    spacelimit              : 1
    secureports             : 8013
    certfile                : mylocalhost.pem
    servers                 : localhost
    title                   : METIS Data Server

    [localhost]
    hosts                   : localhost, dataserver
    workingdirectory        : /root/space/dataserver
    logfile                 : %(workingdirectory)s/data_server.log
    email                   : user@localhost
""")

_ENTRYPOINT_SH = textwrap.dedent("""\
    #!/usr/bin/env bash
    set -euo pipefail

    export DATADIR=/root/space/dataserver
    echo "Create ${DATADIR}"
    mkdir -p "${DATADIR}"
    mkdir -p "${DATADIR}/cdata"
    mkdir -p "${DATADIR}/idata"
    mkdir -p "${DATADIR}/sdata"
    mkdir -p "${DATADIR}/pdata"
    mkdir -p "${DATADIR}/xdata"
    mkdir -p "${DATADIR}/ydata"
    mkdir -p "${DATADIR}/ddata"
    mkdir -p "${DATADIR}/tdata"

    echo "Generate self-signed certificate"
    cd "${DATADIR}" || exit 1
    openssl genrsa -out mylocalhost.key 2048
    openssl req -key mylocalhost.key -new -out mylocalhost.csr -subj "/CN=localhost"
    openssl x509 -signkey mylocalhost.key -in mylocalhost.csr -req -days 365 -out mylocalhost.crt
    cat mylocalhost.key mylocalhost.crt > mylocalhost.pem

    echo "Starting dataserver"
    python -u -m dataserver.main --config /root/scripts/ds.cfg
""")

_ENVIRONMENT_CFG = textwrap.dedent("""\
    # MetisWISE Environment configuration (archive stack)

    [global]
    database_name: postgres/wise
    database_engine     : postgresql

    database_user       : AWANONYMOUS
    database_password   : anonymous
    password_awanonymous : ANONYMOUS
    password_awarobot    : AROBOT
    password_awworld     : WORLD

    query_not_all_mydb  : 1

    data_server         : dataserver
    data_port           : 8013
    data_protocol       : https

    data_server_gpgkey : MetisWISE DataServer Key

    project             : SIM

    use_n_chars_md5     : 8

    mockcommon          :
    use_find_existing   :

    use_python_logging : 1
    python_logging_level : INFO
    python_logging_format : %(asctime)s [%(levelname)s] %(name)s: %(message)s
    python_logging_format_date : %Y-%m-%d %H:%M:%S
""")


def ensure_archive_stack_files(
    compose_dir: Path | None = None,
    image_name: str = ARCHIVE_IMAGE_NAME,
) -> Path:
    """Create the archive stack directory with all config files.

    Files are only written when they do not already exist so that manual
    edits are preserved.  Returns *compose_dir*.
    """
    compose_dir = compose_dir or ARCHIVE_COMPOSE_DIR
    scripts_dir = compose_dir / "scripts"
    scripts_dir.mkdir(parents=True, exist_ok=True)
    (compose_dir / "space" / "inbox").mkdir(parents=True, exist_ok=True)
    (compose_dir / "space" / "outbox").mkdir(parents=True, exist_ok=True)

    files: dict[Path, str] = {
        compose_dir / "compose.yml":              _COMPOSE_YML.format(image=image_name),
        compose_dir / "Containerfile":             _CONTAINERFILE.format(image=image_name),
        scripts_dir / "ds.cfg":                    _DS_CFG,
        scripts_dir / "entrypoint_dataserver.sh":  _ENTRYPOINT_SH,
        scripts_dir / "Environment.cfg":           _ENVIRONMENT_CFG,
    }
    for path, content in files.items():
        if not path.exists():
            path.write_text(content)
            if path.suffix == ".sh":
                path.chmod(0o755)
    return compose_dir


def archive_image_exists(image_name: str = ARCHIVE_IMAGE_NAME) -> bool:
    """Return ``True`` if the archive container image is built."""
    try:
        result = subprocess.run(
            ["podman", "image", "exists", f"{image_name}:latest"],
            capture_output=True, timeout=10,
        )
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def build_archive_image(
    compose_dir: Path | None = None,
    image_name: str = ARCHIVE_IMAGE_NAME,
    credentials: str = "",
    on_output: Callable[[str], None] | None = None,
) -> int:
    """Build the minimal archive image.  Returns the exit code.

    *credentials* should be ``"username:password"`` for the OmegaCEN
    conda / pip channels.
    """
    compose_dir = compose_dir or ARCHIVE_COMPOSE_DIR
    ensure_archive_stack_files(compose_dir, image_name)

    env = os.environ.copy()
    if credentials:
        env["OMEGACEN_CREDENTIALS"] = credentials

    cmd = [
        "podman", "build",
        "--secret=id=OMEGACEN_CREDENTIALS,type=env",
        "-t", f"{image_name}:latest",
        str(compose_dir),
    ]
    proc = subprocess.Popen(
        cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
        text=True, env=env,
    )
    for line in iter(proc.stdout.readline, ""):
        if on_output:
            on_output(line)
    proc.wait()
    return proc.returncode


def archive_stack_up(compose_dir: Path | None = None) -> subprocess.CompletedProcess:
    """Run ``podman-compose up -d``."""
    compose_dir = compose_dir or ARCHIVE_COMPOSE_DIR
    ensure_archive_stack_files(compose_dir)
    return subprocess.run(
        ["podman-compose", "up", "-d"],
        cwd=str(compose_dir),
        capture_output=True, text=True, timeout=120,
    )


def archive_stack_down(compose_dir: Path | None = None) -> subprocess.CompletedProcess:
    """Run ``podman-compose down``."""
    compose_dir = compose_dir or ARCHIVE_COMPOSE_DIR
    return subprocess.run(
        ["podman-compose", "down"],
        cwd=str(compose_dir),
        capture_output=True, text=True, timeout=60,
    )


def archive_stack_status(compose_dir: Path | None = None) -> dict[str, str]:
    """Return ``{service_name: status_string}`` for the archive stack."""
    compose_dir = compose_dir or ARCHIVE_COMPOSE_DIR
    try:
        result = subprocess.run(
            ["podman-compose", "ps", "--format", "json"],
            cwd=str(compose_dir),
            capture_output=True, text=True, timeout=15,
        )
        if result.returncode != 0:
            return {}
        containers = json.loads(result.stdout) if result.stdout.strip() else []
        if isinstance(containers, list):
            return {
                c.get("Service", c.get("Names", "unknown")):
                c.get("State", c.get("Status", "unknown"))
                for c in containers
            }
    except (FileNotFoundError, subprocess.TimeoutExpired, json.JSONDecodeError):
        pass
    return {}


# ---------------------------------------------------------------------------
# Section C — Archive client (exec into container)
# ---------------------------------------------------------------------------


def _compose_exec(
    python_code: str,
    compose_dir: Path | None = None,
    service: str = "dataserver",
    timeout: int = 120,
) -> tuple[int, str, str]:
    """Execute *python_code* inside the archive container.

    Returns *(returncode, stdout, stderr)*.
    """
    compose_dir = compose_dir or ARCHIVE_COMPOSE_DIR
    result = subprocess.run(
        ["podman-compose", "exec", "-T", service, "python", "-c", python_code],
        cwd=str(compose_dir),
        capture_output=True, text=True, timeout=timeout,
    )
    return result.returncode, result.stdout, result.stderr


def upload_files(
    filepaths: list[Path],
    compose_dir: Path | None = None,
    on_log: Callable[[str], None] | None = None,
) -> list[str]:
    """Upload FITS files to the archive via the container.

    Files are first copied to the shared ``space/inbox/`` volume, then
    ingested inside the container using the MetisWISE ingest logic.

    Returns the list of successfully ingested filenames.
    """
    compose_dir = compose_dir or ARCHIVE_COMPOSE_DIR
    inbox = compose_dir / "space" / "inbox"
    inbox.mkdir(parents=True, exist_ok=True)

    ingested: list[str] = []
    for i, fp in enumerate(filepaths, 1):
        dest = inbox / fp.name
        if on_log:
            on_log(f"[{i}/{len(filepaths)}] Copying {fp.name} to inbox…")
        shutil.copy2(fp, dest)

        ingest_code = textwrap.dedent(f"""\
            import json, sys
            from pathlib import Path
            try:
                from astropy.io import fits
            except ImportError:
                fits = None

            filename = "/root/space/inbox/{fp.name}"
            p = Path(filename)
            if not p.exists():
                print(json.dumps({{"error": "file not found"}}))
                sys.exit(1)

            try:
                from metiswise.main.raw import Raw
                from metiswise.main.pro import Pro

                hdus = fits.open(filename)
                if "ESO DPR CATG" in hdus[0].header:
                    di = Raw(filename)
                elif "ESO PRO CATG" in hdus[0].header:
                    di = Pro(filename)
                else:
                    print(json.dumps({{"error": "no DPR.CATG or PRO.CATG header"}}))
                    sys.exit(1)

                di.store()
                di.commit()
                print(json.dumps({{"ok": True, "filename": p.name}}))
            except Exception as exc:
                print(json.dumps({{"error": str(exc)}}))
                sys.exit(1)
        """)

        if on_log:
            on_log(f"[{i}/{len(filepaths)}] Ingesting {fp.name}…")
        rc, stdout, stderr = _compose_exec(ingest_code, compose_dir)
        if rc == 0:
            ingested.append(fp.name)
            if on_log:
                on_log(f"[{i}/{len(filepaths)}] {fp.name} ingested successfully")
        else:
            if on_log:
                msg = stdout.strip() or stderr.strip() or f"exit code {rc}"
                on_log(f"[{i}/{len(filepaths)}] Failed to ingest {fp.name}: {msg}")
    return ingested


def query_archive(
    pro_catg: str | None = None,
    compose_dir: Path | None = None,
    on_log: Callable[[str], None] | None = None,
) -> list[dict]:
    """Query the archive database for available files.

    If *pro_catg* is given, filters by ``PRO.CATG`` value (master product
    category).  Returns a list of dicts with ``filename``, ``pro_catg``,
    and ``class_name`` keys.
    """
    compose_dir = compose_dir or ARCHIVE_COMPOSE_DIR

    if pro_catg:
        query_code = textwrap.dedent(f"""\
            import json, sys
            try:
                from metiswise.main.aweimports import *
                from metiswise.main.pro import Pro
                results = (Pro.pro_catg == "{pro_catg}")
                items = []
                for r in results:
                    items.append({{
                        "filename": r.filename,
                        "pro_catg": getattr(r, "pro_catg", ""),
                        "class_name": type(r).__name__,
                    }})
                print(json.dumps(items))
            except Exception as exc:
                print(json.dumps({{"error": str(exc)}}))
                sys.exit(1)
        """)
    else:
        query_code = textwrap.dedent("""\
            import json, sys
            try:
                from metiswise.main.dataitem import DataItem
                results = DataItem.select_all()
                items = []
                for r in results:
                    items.append({
                        "filename": r.filename,
                        "pro_catg": getattr(r, "pro_catg", ""),
                        "class_name": type(r).__name__,
                    })
                print(json.dumps(items))
            except Exception as exc:
                print(json.dumps({"error": str(exc)}))
                sys.exit(1)
        """)

    if on_log:
        on_log("Querying archive…")
    rc, stdout, stderr = _compose_exec(query_code, compose_dir)

    if rc != 0:
        if on_log:
            on_log(f"Query failed: {stderr.strip() or stdout.strip()}")
        return []

    try:
        data = json.loads(stdout)
        if isinstance(data, dict) and "error" in data:
            if on_log:
                on_log(f"Query error: {data['error']}")
            return []
        return data if isinstance(data, list) else []
    except json.JSONDecodeError:
        if on_log:
            on_log(f"Failed to parse query result: {stdout[:200]}")
        return []


def download_file(
    filename: str,
    dest_dir: Path,
    compose_dir: Path | None = None,
    on_log: Callable[[str], None] | None = None,
) -> Path | None:
    """Download a file from the archive to *dest_dir*.

    The file is retrieved inside the container to the shared ``space/outbox/``
    volume, then copied to *dest_dir* on the host.

    Returns the host path to the downloaded file, or ``None`` on failure.
    """
    compose_dir = compose_dir or ARCHIVE_COMPOSE_DIR
    outbox = compose_dir / "space" / "outbox"
    outbox.mkdir(parents=True, exist_ok=True)

    retrieve_code = textwrap.dedent(f"""\
        import json, shutil, sys
        from pathlib import Path
        try:
            from metiswise.main.dataitem import DataItem
            results = (DataItem.filename == "{filename}")
            if len(results) == 0:
                print(json.dumps({{"error": "not found"}}))
                sys.exit(1)
            di = results[0]
            di.retrieve()
            src = Path(di.pathname) / di.filename
            if not src.exists():
                src = Path(di.filename)
            dest = Path("/root/space/outbox/{filename}")
            shutil.copy2(str(src), str(dest))
            print(json.dumps({{"ok": True, "filename": di.filename}}))
        except Exception as exc:
            print(json.dumps({{"error": str(exc)}}))
            sys.exit(1)
    """)

    if on_log:
        on_log(f"Retrieving {filename} from archive…")
    rc, stdout, stderr = _compose_exec(retrieve_code, compose_dir)

    if rc != 0:
        if on_log:
            on_log(f"Download failed: {stderr.strip() or stdout.strip()}")
        return None

    container_file = outbox / filename
    if not container_file.exists():
        if on_log:
            on_log(f"File not found in outbox after retrieve: {filename}")
        return None

    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / filename
    shutil.move(str(container_file), str(dest))
    if on_log:
        on_log(f"Downloaded {filename} → {dest}")
    return dest


def list_available_masters(
    workflow: str,
    compose_dir: Path | None = None,
    on_log: Callable[[str], None] | None = None,
) -> dict[str, list[str]]:
    """Query the archive for master calibration files relevant to *workflow*.

    Returns ``{pro_catg: [filename, …]}`` for each master product type
    available in the archive.
    """
    from archive import TASK_TO_MASTER_PROCATG  # avoid circular at module level

    chain = _get_task_chain(workflow)
    result: dict[str, list[str]] = {}
    for task_name, _tag, _meta in chain:
        pro_catg = TASK_TO_MASTER_PROCATG.get(task_name)
        if not pro_catg:
            continue
        items = query_archive(pro_catg=pro_catg, compose_dir=compose_dir,
                              on_log=on_log)
        if items:
            result[pro_catg] = [it["filename"] for it in items]
    return result


# ---------------------------------------------------------------------------
# Section D — Master calibration auto-download
# ---------------------------------------------------------------------------

# Maps pipeline task names to the PRO.CATG of the master product they create.
TASK_TO_MASTER_PROCATG: dict[str, str] = {
    # LM IMG
    "metis_lm_img_lingain":          "LINEARITY_2RG",
    "metis_lm_img_dark":             "MASTER_DARK_2RG",
    "metis_lm_img_flat":             "MASTER_IMG_FLAT_LAMP_LM",
    "metis_lm_img_distortion":       "LM_DISTORTION_TABLE",
    # N IMG
    "metis_n_img_lingain":           "LINEARITY_GEO",
    "metis_n_img_dark":              "MASTER_DARK_GEO",
    "metis_n_img_flat":              "MASTER_IMG_FLAT_LAMP_N",
    "metis_n_img_distortion":        "N_DISTORTION_TABLE",
    # IFU
    "metis_ifu_lingain":             "LINEARITY_IFU",
    "metis_ifu_dark":                "MASTER_DARK_IFU",
    "metis_ifu_distortion":          "IFU_DISTORTION_TABLE",
    "metis_ifu_wavecal":             "IFU_WAVECAL",
    "metis_ifu_rsrf":                "MASTER_IFU_RSRF",
    # LM LSS
    "metis_lm_lss_lingain":          "LINEARITY_2RG",
    "metis_lm_lss_dark":             "MASTER_DARK_2RG",
    "metis_lm_lss_rsrf":             "LM_LSS_MASTER_RSRF",
    "metis_lm_lss_trace":            "LM_LSS_TRACE_TABLE",
    "metis_lm_lss_wave":             "LM_LSS_WAVECAL",
    # N LSS
    "metis_n_lss_lingain":           "LINEARITY_GEO",
    "metis_n_lss_dark":              "MASTER_DARK_GEO",
    "metis_n_lss_rsrf":              "N_LSS_MASTER_RSRF",
    "metis_n_lss_trace":             "N_LSS_TRACE_TABLE",
    "metis_n_lss_wave":              "N_LSS_WAVECAL",
}


def _get_task_chain(workflow: str) -> list[tuple[str, str, str | None]]:
    """Import and return the task chain for *workflow* from ``run_metis``."""
    # Late import to avoid circular dependency with run_metis.py.
    from run_metis import WORKFLOW_TASK_CHAIN
    return WORKFLOW_TASK_CHAIN.get(workflow, [])


def identify_missing_calibrations(
    workflow: str,
    data_tags: set[str],
    has_science: bool,
) -> list[tuple[str, str]]:
    """Identify master calibrations needed but not available as raw data.

    Walks the ``WORKFLOW_TASK_CHAIN`` for *workflow*.  For each non-science
    task whose raw-input classification tag is **not** in *data_tags* but
    which is upstream of a task that **is**, its master product category is
    returned.

    Returns a list of ``(task_name, master_pro_catg)`` pairs.
    """
    chain = _get_task_chain(workflow)
    if not chain:
        return []

    # Find the deepest task whose raw tag IS present (the target).
    deepest_present_idx = -1
    for idx, (_task, tag, meta) in enumerate(chain):
        if meta == "science":
            continue
        if tag in data_tags:
            deepest_present_idx = idx

    if deepest_present_idx < 0:
        return []

    # All tasks upstream of (and including) the deepest present task
    # whose raw tag is NOT present need their master products.
    missing: list[tuple[str, str]] = []
    for idx, (task_name, tag, meta) in enumerate(chain):
        if idx > deepest_present_idx:
            break
        if meta == "science":
            continue
        if tag not in data_tags:
            pro_catg = TASK_TO_MASTER_PROCATG.get(task_name)
            if pro_catg:
                missing.append((task_name, pro_catg))
    return missing


def fetch_missing_calibrations(
    workflow: str,
    data_tags: set[str],
    has_science: bool,
    dest_dir: Path,
    compose_dir: Path | None = None,
    on_log: Callable[[str], None] | None = None,
) -> list[Path]:
    """Download missing master calibration files from the archive.

    1. Calls :func:`identify_missing_calibrations` to find gaps.
    2. Queries the archive for each missing master ``PRO.CATG``.
    3. Downloads matching files into *dest_dir*.

    Returns a list of downloaded file paths.  Gracefully handles the case
    where no master files are available yet (returns an empty list).
    """
    compose_dir = compose_dir or ARCHIVE_COMPOSE_DIR
    missing = identify_missing_calibrations(workflow, data_tags, has_science)

    if not missing:
        if on_log:
            on_log("No missing calibrations identified")
        return []

    if on_log:
        on_log(f"Missing calibrations: {', '.join(pc for _, pc in missing)}")

    downloaded: list[Path] = []
    for task_name, pro_catg in missing:
        if on_log:
            on_log(f"Searching archive for {pro_catg}…")
        items = query_archive(pro_catg=pro_catg, compose_dir=compose_dir,
                              on_log=on_log)
        if not items:
            if on_log:
                on_log(f"  No {pro_catg} found in archive — skipping")
            continue

        # Take the most recent file (last in list).
        target = items[-1]["filename"]
        path = download_file(target, dest_dir, compose_dir=compose_dir,
                             on_log=on_log)
        if path:
            downloaded.append(path)
    return downloaded
