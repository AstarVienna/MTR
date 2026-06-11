"""
Unit tests for credentials.py (OS-keyring wrapper).

The real ``keyring`` package is never exercised: every test patches a mock
``keyring`` module into sys.modules (mirroring the _DB_MOCKS pattern in
test_archive.py), so no backend is initialized and no unlock prompt can
fire on a developer machine.
"""

import json
import sys
from unittest.mock import MagicMock, patch

import pytest

from metis_test_runner import credentials


def _keyring_mocks(store: dict | None = None) -> dict:
    """Return sys.modules patches for a dict-backed fake keyring."""
    store = store if store is not None else {}

    kr = MagicMock()
    kr.get_password.side_effect = (
        lambda service, entry: store.get((service, entry))
    )
    kr.set_password.side_effect = (
        lambda service, entry, value: store.__setitem__((service, entry), value)
    )
    kr.delete_password.side_effect = (
        lambda service, entry: store.pop((service, entry))
    )

    errors = MagicMock()
    errors.PasswordDeleteError = KeyError
    kr.errors = errors
    return {"keyring": kr, "keyring.errors": errors}


class TestLazyImport:
    def test_no_module_scope_keyring_import(self):
        # The hard UX requirement: importing the module must never touch a
        # keyring backend. Guard against a top-level `import keyring`.
        assert "keyring" not in vars(credentials)


class TestPipCredentials:
    def test_round_trip(self):
        with patch.dict(sys.modules, _keyring_mocks()):
            credentials.set_pip_credentials("alice:p4ss")
            assert credentials.get_pip_credentials() == "alice:p4ss"

    def test_absent_returns_none(self):
        with patch.dict(sys.modules, _keyring_mocks()):
            assert credentials.get_pip_credentials() is None

    def test_delete(self):
        with patch.dict(sys.modules, _keyring_mocks()):
            credentials.set_pip_credentials("alice:p4ss")
            credentials.delete_pip_credentials()
            assert credentials.get_pip_credentials() is None

    def test_delete_absent_is_noop(self):
        with patch.dict(sys.modules, _keyring_mocks()):
            credentials.delete_pip_credentials()

    def test_uses_expected_service_and_entry(self):
        store = {}
        with patch.dict(sys.modules, _keyring_mocks(store)):
            credentials.set_pip_credentials("u:p")
        assert store == {("metis-test-runner", "omegacen-pip"): "u:p"}


class TestDbCredentials:
    _FIELDS = {
        "database_user":           "AWTEST",
        "database_password":       "lmno",
        "project":                 "SIM (3)",
        "database_tablespacename": "metis_data",
        "database_name":           "metis.example.com:5436/pgmetis",
    }

    def test_round_trip(self):
        with patch.dict(sys.modules, _keyring_mocks()):
            credentials.set_db_credentials(self._FIELDS)
            assert credentials.get_db_credentials() == self._FIELDS

    def test_absent_returns_none(self):
        with patch.dict(sys.modules, _keyring_mocks()):
            assert credentials.get_db_credentials() is None

    def test_stored_as_single_json_entry(self):
        store = {}
        with patch.dict(sys.modules, _keyring_mocks(store)):
            credentials.set_db_credentials(self._FIELDS)
        assert list(store) == [("metis-test-runner", "archive-db")]
        assert json.loads(store[("metis-test-runner", "archive-db")]) \
            == self._FIELDS

    def test_malformed_json_returns_none(self):
        store = {("metis-test-runner", "archive-db"): "{not json"}
        with patch.dict(sys.modules, _keyring_mocks(store)):
            assert credentials.get_db_credentials() is None

    def test_non_dict_json_returns_none(self):
        store = {("metis-test-runner", "archive-db"): '["a", "b"]'}
        with patch.dict(sys.modules, _keyring_mocks(store)):
            assert credentials.get_db_credentials() is None

    def test_delete(self):
        with patch.dict(sys.modules, _keyring_mocks()):
            credentials.set_db_credentials(self._FIELDS)
            credentials.delete_db_credentials()
            assert credentials.get_db_credentials() is None


class TestBackendFailures:
    def _broken_keyring(self) -> dict:
        kr = MagicMock()
        kr.get_password.side_effect = RuntimeError("no D-Bus session")
        kr.set_password.side_effect = RuntimeError("no D-Bus session")
        kr.delete_password.side_effect = RuntimeError("no D-Bus session")
        errors = MagicMock()
        errors.PasswordDeleteError = KeyError
        kr.errors = errors
        return {"keyring": kr, "keyring.errors": errors}

    def test_get_raises_credentials_unavailable(self):
        with patch.dict(sys.modules, self._broken_keyring()):
            with pytest.raises(credentials.CredentialsUnavailable):
                credentials.get_pip_credentials()
            with pytest.raises(credentials.CredentialsUnavailable):
                credentials.get_db_credentials()

    def test_set_raises_credentials_unavailable(self):
        with patch.dict(sys.modules, self._broken_keyring()):
            with pytest.raises(credentials.CredentialsUnavailable):
                credentials.set_pip_credentials("u:p")
            with pytest.raises(credentials.CredentialsUnavailable):
                credentials.set_db_credentials({"database_user": "x"})

    def test_delete_raises_credentials_unavailable(self):
        with patch.dict(sys.modules, self._broken_keyring()):
            with pytest.raises(credentials.CredentialsUnavailable):
                credentials.delete_pip_credentials()
