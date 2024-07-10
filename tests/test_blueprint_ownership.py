import pytest

from titan import resources as res
from titan.blueprint import Blueprint, Action, InvalidOwnerException, MissingPrivilegeException, compile_plan_to_sql
from titan.identifiers import parse_URN
from titan.resource_name import ResourceName
from titan.privs import GrantedPrivilege, AccountPriv


@pytest.fixture
def session_ctx() -> dict:
    return {
        "account": "SOMEACCT",
        "account_locator": "ABCD123",
        "role": "SYSADMIN",
        "available_roles": [
            "SYSADMIN",
            "USERADMIN",
            "ACCOUNTADMIN",
            "SECURITYADMIN",
            "PUBLIC",
        ],
        "role_privileges": {},
    }


@pytest.fixture
def remote_state() -> dict:
    return {
        parse_URN("urn::ABCD123:account/SOMEACCT"): {},
    }


def test_default_owner(session_ctx, remote_state):
    warehouse = res.Warehouse(name="test_warehouse")
    assert warehouse._data.owner.name == "SYSADMIN"
    blueprint = Blueprint(resources=[warehouse])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = blueprint._plan(remote_state, manifest)
    assert len(plan) == 1
    assert plan[0].action == Action.ADD
    assert plan[0].after["owner"] == "SYSADMIN"


def test_non_default_owner(session_ctx, remote_state):
    warehouse = res.Warehouse(name="test_warehouse", owner="ACCOUNTADMIN")
    assert warehouse._data.owner.name == "ACCOUNTADMIN"
    blueprint = Blueprint(resources=[warehouse])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = blueprint._plan(remote_state, manifest)
    assert len(plan) == 1
    assert plan[0].action == Action.ADD
    assert plan[0].after["owner"] == "ACCOUNTADMIN"


def test_custom_role_owner(session_ctx, remote_state):
    role = res.Role(name="CUSTOMROLE")
    grant = res.RoleGrant(role=role, to_role="SYSADMIN")
    warehouse = res.Warehouse(name="test_warehouse", owner=role)
    assert warehouse._data.owner.name == "CUSTOMROLE"
    blueprint = Blueprint(resources=[role, grant, warehouse])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = blueprint._plan(remote_state, manifest)
    assert len(plan) == 3
    assert plan[0].action == Action.ADD
    assert plan[0].urn == parse_URN("urn::ABCD123:role/CUSTOMROLE")
    assert plan[1].action == Action.ADD
    assert plan[1].urn == parse_URN("urn::ABCD123:role_grant/CUSTOMROLE?role=SYSADMIN")
    assert plan[2].action == Action.ADD
    assert plan[2].urn == parse_URN("urn::ABCD123:warehouse/test_warehouse")
    assert plan[2].after["owner"] == "CUSTOMROLE"


def test_invalid_custom_role_owner(session_ctx):
    role = res.Role(name="INVALIDROLE")
    warehouse = res.Warehouse(name="test_warehouse", owner=role)
    blueprint = Blueprint(resources=[role, warehouse])
    with pytest.raises(InvalidOwnerException):
        blueprint.generate_manifest(session_ctx)


def test_transfer_ownership(session_ctx, remote_state):
    remote_state = remote_state.copy()
    remote_state[parse_URN("urn::ABCD123:role/test_role")] = {
        "name": "test_role",
        "owner": "ACCOUNTADMIN",
        "comment": None,
    }

    role = res.Role(name="test_role")
    blueprint = Blueprint(resources=[role])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = blueprint._plan(remote_state, manifest)
    assert len(plan) == 1
    assert plan[0].action == Action.TRANSFER
    assert plan[0].after["owner"] == "USERADMIN"


def test_transfer_ownership_with_changes(session_ctx, remote_state):
    remote_state = remote_state.copy()
    remote_state[parse_URN("urn::ABCD123:role/test_role")] = {
        "name": "test_role",
        "owner": "ACCOUNTADMIN",
        "comment": None,
    }

    role = res.Role(name="test_role", comment="This comment has been added")
    blueprint = Blueprint(resources=[role])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = blueprint._plan(remote_state, manifest)
    assert len(plan) == 2
    assert plan[0].action == Action.CHANGE
    assert plan[0].after["comment"] == "This comment has been added"
    assert plan[1].action == Action.TRANSFER
    assert plan[1].after["owner"] == "USERADMIN"


def test_resource_has_custom_role_owner_with_create_priv(session_ctx, remote_state):
    session_ctx = session_ctx.copy()
    session_ctx["available_roles"].append(ResourceName("test_role"))
    session_ctx["role_privileges"] = {
        "TEST_ROLE": [
            GrantedPrivilege(privilege=AccountPriv.CREATE_WAREHOUSE, on="ABCD123"),
        ]
    }

    warehouse = res.Warehouse(name="test_warehouse", owner="test_role")
    blueprint = Blueprint(resources=[warehouse])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = blueprint._plan(remote_state, manifest)
    assert len(plan) == 1
    assert plan[0].action == Action.ADD
    assert plan[0].urn == parse_URN("urn::ABCD123:warehouse/test_warehouse")

    sql_commands = compile_plan_to_sql(session_ctx, plan)
    assert sql_commands[0] == "USE SECONDARY ROLES ALL"
    assert sql_commands[1] == "USE ROLE TEST_ROLE"
    assert sql_commands[2].startswith("CREATE WAREHOUSE TEST_WAREHOUSE")


def test_resource_has_custom_role_owner_without_create_priv(session_ctx, remote_state):
    session_ctx = session_ctx.copy()
    session_ctx["available_roles"].append(ResourceName("test_role"))

    warehouse = res.Warehouse(name="test_warehouse", owner="test_role")
    blueprint = Blueprint(resources=[warehouse])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = blueprint._plan(remote_state, manifest)
    assert len(plan) == 1
    assert plan[0].action == Action.ADD
    assert plan[0].urn == parse_URN("urn::ABCD123:warehouse/test_warehouse")

    with pytest.raises(MissingPrivilegeException):
        compile_plan_to_sql(session_ctx, plan)
