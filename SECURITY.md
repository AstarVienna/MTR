# Security Policy

## Reporting a vulnerability

Please report security issues privately via
[GitHub's private vulnerability reporting](https://github.com/AstarVienna/MTR/security/advisories/new)
rather than opening a public issue.

## Scope

MTR handles two kinds of credential, and both are worth understanding if you
are reviewing or extending the code.

### Archive database credentials

Stored in the **OS keyring** (`keyring`, one JSON entry under the service name
`metis-test-runner` / `archive-db`), never in `QSettings` and never written to
disk by MTR.

`archive.apply_db_credentials()` injects them into `os.environ` because that is
how commonwise reads its configuration. To keep them from leaking further,
`env.resolve_runtime_env()` — the single seam every subprocess environment
flows through — **strips** the keys in `env.SECRET_ENV_KEYS` before handing an
environment to any child process. Without that filter the database password
would be inherited by `edps`, `pyesorex`, ScopeSim and the interactive
`mtr-shell`.

**If you add a new way to spawn a subprocess, route its environment through
`env.resolve_runtime_env()`.** Do not use a bare `os.environ.copy()`.

### OmegaCEN pip credentials

Used only to install MetisWISE from the credentialed package index. They travel
in `PIP_EXTRA_INDEX_URL` and **never in `argv`**: a command line is readable by
any local user via `/proc/<pid>/cmdline` and gets echoed into the GUI log,
whereas `/proc/<pid>/environ` is owner-only. `tests/test_archive.py` enforces
this.

They are percent-encoded by `archive.encode_pip_credentials()` before being
interpolated into the index URL. This matters: an unescaped `@` in a password
re-splits the URL authority, which would send the remaining credential material
to a host taken from the password itself.

### Legacy `~/.awe/Environment.cfg`

MTR still *reads* this Astro-WISE file as a fallback and can migrate away from
it, but the GUI no longer writes credentials there. Any write goes through
`paths.write_text_atomic(..., mode=0o600)`, and values containing newlines are
rejected — an embedded newline would inject additional configuration lines,
which could for example downgrade the archive transport to cleartext.

## Non-goals

MTR is a developer/test tool that runs pipeline code and installs packages from
configured indexes on the user's behalf. It trusts:

- the ESO and OmegaCEN package indexes it is configured with,
- the `METIS_Pipeline` / `METIS_Simulations` repositories it clones,
- the archive it is pointed at.

Compromise of any of those is out of scope for this project.
