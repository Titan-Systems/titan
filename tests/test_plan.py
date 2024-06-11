import pytest

from titan.blueprint import Action, Blueprint, ResourceChange
from titan.parse import parse_URN


@pytest.fixture(scope="session")
def remote_state(removed_db):
    state = {
        parse_URN("urn::XYZ123:database/EXISTING_DB"): {
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
        parse_URN("urn::XYZ123:database/NEW_DB"): {
            "name": "NEW_DB",
            "transient": False,
            "owner": "SYSADMIN",
        }
    }


@pytest.fixture(scope="session")
def changed_db():
    return {
        parse_URN("urn::XYZ123:database/EXISTING_DB"): {
            "name": "EXISTING_DB",
            "transient": False,
            "owner": "SYSADMIN",
            "default_ddl_collation": "UTF8",
        }
    }


@pytest.fixture(scope="session")
def removed_db():
    return {
        parse_URN("urn::XYZ123:database/REMOVED_DB"): {
            "name": "REMOVED_DB",
            "transient": False,
            "owner": "SYSADMIN",
        }
    }


@pytest.fixture(scope="session")
def manifest(new_db, changed_db):
    manifest = {
        "_urns": [
            parse_URN("urn::XYZ123:database/EXISTING_DB"),
            parse_URN("urn::XYZ123:database/NEW_DB"),
        ],
        "_refs": [],
    }
    manifest.update(new_db)
    manifest.update(changed_db)
    return manifest


def test_plan_add_action(remote_state, manifest, new_db):
    bp = Blueprint()
    changes = bp._plan(remote_state, manifest)
    urn, after = new_db.popitem()
    expected = ResourceChange(Action.ADD, urn, {}, after, after)
    assert expected in changes


def test_plan_change_action(remote_state, manifest, changed_db):
    bp = Blueprint()
    changes = bp._plan(remote_state, manifest)
    urn, data = changed_db.popitem()
    delta = {"default_ddl_collation": "UTF8"}
    expected = ResourceChange(Action.CHANGE, urn, remote_state[urn], data, delta)
    assert expected in changes


def test_plan_remove_action(remote_state, manifest, removed_db):
    bp = Blueprint()
    changes = bp._plan(remote_state, manifest)
    urn, _ = removed_db.popitem()
    expected = ResourceChange(Action.REMOVE, urn, remote_state[urn], {}, {})
    assert expected in changes
