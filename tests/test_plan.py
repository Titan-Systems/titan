import pytest

from titan import resources as res
from titan.blueprint import Blueprint, CreateResource, DropResource, NonConformingPlanException, RunMode, UpdateResource
from titan.enums import ResourceType
from titan.identifiers import parse_URN


@pytest.fixture
def session_ctx() -> dict:
    return {
        "account": "SOMEACCT",
        "account_locator": "ABCD123",
        "role": "SYSADMIN",
        "available_roles": ["SYSADMIN", "USERADMIN"],
    }


@pytest.fixture
def remote_state() -> dict:
    return {
        parse_URN("urn::ABCD123:account/SOMEACCT"): {},
    }


def test_plan_add_action(session_ctx, remote_state):
    bp = Blueprint(resources=[res.Database(name="NEW_DATABASE")])
    manifest = bp.generate_manifest(session_ctx)
    plan = bp._plan(remote_state, manifest)
    assert len(plan) == 1
    change = plan[0]
    assert isinstance(change, CreateResource)
    assert change.urn == parse_URN("urn::ABCD123:database/NEW_DATABASE")
    assert "name" in change.after
    assert change.after["name"] == "NEW_DATABASE"


def test_plan_change_action(session_ctx, remote_state):
    remote_state[parse_URN("urn::ABCD123:role/EXISTING_ROLE")] = {
        "name": "EXISTING_ROLE",
        "comment": "old comment",
        "owner": "USERADMIN",
    }
    bp = Blueprint(
        resources=[
            res.Role(
                name="EXISTING_ROLE",
                comment="new comment",
            )
        ]
    )
    manifest = bp.generate_manifest(session_ctx)
    plan = bp._plan(remote_state, manifest)
    assert len(plan) == 1
    change = plan[0]
    assert isinstance(change, UpdateResource)
    assert change.urn == parse_URN("urn::ABCD123:role/EXISTING_ROLE")
    assert "comment" in change.before
    assert change.before["comment"] == "old comment"
    assert "comment" in change.after
    assert change.after["comment"] == "new comment"


def test_plan_remove_action(session_ctx, remote_state):
    remote_state[parse_URN("urn::ABCD123:role/REMOVED_ROLE")] = {
        "name": "REMOVED_ROLE",
        "comment": "old comment",
        "owner": "USERADMIN",
    }
    bp = Blueprint(run_mode=RunMode.SYNC, allowlist=[ResourceType.ROLE])
    manifest = bp.generate_manifest(session_ctx)
    plan = bp._plan(remote_state, manifest)
    assert len(plan) == 1
    change = plan[0]
    assert isinstance(change, DropResource)
    assert change.urn == parse_URN("urn::ABCD123:role/REMOVED_ROLE")


def test_plan_no_removes_in_run_mode_create_or_update(session_ctx, remote_state):
    remote_state[parse_URN("urn::ABCD123:role/REMOVED_ROLE")] = {
        "name": "REMOVED_ROLE",
        "comment": "old comment",
        "owner": "USERADMIN",
    }
    bp = Blueprint(run_mode=RunMode.CREATE_OR_UPDATE)
    manifest = bp.generate_manifest(session_ctx)
    plan = bp._plan(remote_state, manifest)
    assert len(plan) == 1
    change = plan[0]
    assert isinstance(change, DropResource)
    assert change.urn == parse_URN("urn::ABCD123:role/REMOVED_ROLE")
    with pytest.raises(NonConformingPlanException):
        bp._raise_for_nonconforming_plan(plan)
