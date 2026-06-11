"""OS-keyring storage for archive credentials.

Two secrets live under the ``metis-test-runner`` keyring service:

- ``omegacen-pip`` — the OmegaCEN pip-channel credentials as a single
  ``"username:password"`` string (used to build the credentialed
  entropynaut index URL for installing MetisWISE).
- ``archive-db`` — a JSON object holding the five database fields that
  commonwise needs (``database_user``, ``database_password``, ``project``,
  ``database_tablespacename``, ``database_name``).  One entry instead of
  five keeps it to a single unlock/ACL prompt and makes saves atomic.

``keyring`` is imported lazily inside the helpers, never at module scope:
merely importing this module (e.g. via ``archive.py``) must not initialize
a keyring backend, so users who never touch the archive are never prompted
to unlock anything.
"""

from __future__ import annotations

import json

SERVICE_NAME = "metis-test-runner"
PIP_ENTRY = "omegacen-pip"
DB_ENTRY = "archive-db"


class CredentialsUnavailable(RuntimeError):
    """No usable keyring backend, or the backend failed.

    Raised on headless machines without a Secret Service daemon, inside
    containers, or when the backend errors out (D-Bus failures surface as
    assorted exception types).  Callers should degrade to session-only
    operation or the legacy ``~/.awe/Environment.cfg`` fallback.
    """


def _get_secret(entry: str) -> str | None:
    import keyring

    try:
        return keyring.get_password(SERVICE_NAME, entry)
    except Exception as exc:
        raise CredentialsUnavailable(
            f"Could not read '{entry}' from the OS keyring: {exc}"
        ) from exc


def _set_secret(entry: str, value: str) -> None:
    import keyring

    try:
        keyring.set_password(SERVICE_NAME, entry, value)
    except Exception as exc:
        raise CredentialsUnavailable(
            f"Could not store '{entry}' in the OS keyring: {exc}"
        ) from exc


def _delete_secret(entry: str) -> None:
    import keyring

    try:
        keyring.delete_password(SERVICE_NAME, entry)
    except Exception as exc:
        # Distinguish "nothing to delete" (fine) from backend failure.
        try:
            import keyring.errors
            if isinstance(exc, keyring.errors.PasswordDeleteError):
                return
        except ImportError:
            pass
        raise CredentialsUnavailable(
            f"Could not delete '{entry}' from the OS keyring: {exc}"
        ) from exc


# ── OmegaCEN pip credentials ("username:password") ──────────────────────────

def get_pip_credentials() -> str | None:
    """Return the stored ``user:pass`` string, or None if not stored."""
    return _get_secret(PIP_ENTRY)


def set_pip_credentials(credentials: str) -> None:
    _set_secret(PIP_ENTRY, credentials)


def delete_pip_credentials() -> None:
    _delete_secret(PIP_ENTRY)


# ── Archive DB fields (JSON blob of the five Environment.cfg keys) ──────────

def get_db_credentials() -> dict[str, str] | None:
    """Return the stored DB fields, or None if absent or malformed.

    A malformed blob (e.g. from a corrupted entry) is treated as absent
    rather than an error — it self-heals on the next successful save.
    """
    raw = _get_secret(DB_ENTRY)
    if raw is None:
        return None
    try:
        fields = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    if not isinstance(fields, dict):
        return None
    return {str(k): str(v) for k, v in fields.items()}


def set_db_credentials(fields: dict[str, str]) -> None:
    _set_secret(DB_ENTRY, json.dumps(fields))


def delete_db_credentials() -> None:
    _delete_secret(DB_ENTRY)
