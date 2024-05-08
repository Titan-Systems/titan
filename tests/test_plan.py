import pytest

from titan.blueprint import Action, Blueprint, ResourceChange
from titan.parse import parse_URN


@pytest.fixture(scope="session")
def remote_state(removed_db):
    state = {
        "urn::XYZ123:database/EXISTING_DB": {
            "name": "EXISTING_DB",
            "transient": False,
            "owner": "SYSADMIN",
        },
    }
    state.update(removed_db)
    return state


@pytest.fixture(scope="session")
def new_db():
    return {
        "urn::XYZ123:database/NEW_DB": {
            "name": "NEW_DB",
            "transient": False,
            "owner": "SYSADMIN",
        }
    }


@pytest.fixture(scope="session")
def changed_db():
    return {
        "urn::XYZ123:database/EXISTING_DB": {
            "name": "EXISTING_DB",
            "transient": False,
            "owner": "SYSADMIN",
            "default_ddl_collation": "UTF8",
        }
    }


@pytest.fixture(scope="session")
def removed_db():
    return {
        "urn::XYZ123:database/REMOVED_DB": {
            "name": "REMOVED_DB",
            "transient": False,
            "owner": "SYSADMIN",
        }
    }


@pytest.fixture(scope="session")
def manifest(new_db, changed_db):
    manifest = {
        "_urns": [
            "urn::XYZ123:database/EXISTING_DB",
            "urn::XYZ123:database/NEW_DB",
        ],
        "_refs": [],
    }
    manifest.update(new_db)
    manifest.update(changed_db)
    return manifest


def test_plan_add_action(remote_state, manifest, new_db):
    bp = Blueprint()
    changes = bp._plan(remote_state, manifest)
    urn_str, after = new_db.popitem()
    expected = ResourceChange(Action.ADD, parse_URN(urn_str), {}, after, after)
    assert expected in changes


def test_plan_change_action(remote_state, manifest, changed_db):
    bp = Blueprint()
    changes = bp._plan(remote_state, manifest)
    urn_str, data = changed_db.popitem()
    delta = {"default_ddl_collation": "UTF8"}
    expected = ResourceChange(Action.CHANGE, parse_URN(urn_str), remote_state[urn_str], data, delta)
    assert expected in changes


def test_plan_remove_action(remote_state, manifest, removed_db):
    bp = Blueprint()
    changes = bp._plan(remote_state, manifest)
    urn_str, _ = removed_db.popitem()
    expected = ResourceChange(Action.REMOVE, parse_URN(urn_str), remote_state[urn_str], {}, {})
    assert expected in changes
