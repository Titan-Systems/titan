import pytest

from titan import resources as res
from titan.blueprint import (
    Blueprint,
    CreateResource,
    MissingPrivilegeException,
    TransferOwnership,
    UpdateResource,
    compile_plan_to_sql,
)
from titan.identifiers import parse_URN
from titan.privs import AccountPriv, DatabasePriv, GrantedPrivilege
from titan.resource_name import ResourceName


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
    assert isinstance(plan[0], CreateResource)
    assert plan[0].after["owner"] == "SYSADMIN"


def test_non_default_owner(session_ctx, remote_state):
    warehouse = res.Warehouse(name="test_warehouse", owner="ACCOUNTADMIN")
    assert warehouse._data.owner.name == "ACCOUNTADMIN"
    blueprint = Blueprint(resources=[warehouse])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = blueprint._plan(remote_state, manifest)
    assert len(plan) == 1
    assert isinstance(plan[0], CreateResource)
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
    assert isinstance(plan[0], CreateResource)
    assert plan[0].urn == parse_URN("urn::ABCD123:role/CUSTOMROLE")
    assert isinstance(plan[1], CreateResource)
    assert plan[1].urn == parse_URN("urn::ABCD123:role_grant/CUSTOMROLE?role=SYSADMIN")
    assert isinstance(plan[2], CreateResource)
    assert plan[2].urn == parse_URN("urn::ABCD123:warehouse/test_warehouse")
    assert plan[2].after["owner"] == "CUSTOMROLE"


# def test_invalid_custom_role_owner(session_ctx):
#     role = res.Role(name="INVALIDROLE")
#     warehouse = res.Warehouse(name="test_warehouse", owner=role)
#     blueprint = Blueprint(resources=[role, warehouse])
#     with pytest.raises(InvalidOwnerException):
#         blueprint.generate_manifest(session_ctx)


def test_transfer_ownership(session_ctx, remote_state):
    remote_state = remote_state.copy()
    remote_state[parse_URN("urn::ABCD123:role/test_role")] = {
        "name": "test_role",
        "owner": "ACCOUNTADMIN",
        "comment": None,
    }

    role = res.Role(name="test_role", owner="USERADMIN")
    blueprint = Blueprint(resources=[role])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = blueprint._plan(remote_state, manifest)
    assert len(plan) == 1
    assert isinstance(plan[0], TransferOwnership)
    assert plan[0].from_owner == "ACCOUNTADMIN"
    assert plan[0].to_owner == "USERADMIN"
    sql_commands = compile_plan_to_sql(session_ctx, plan)
    assert sql_commands[0] == "USE SECONDARY ROLES ALL"
    assert sql_commands[1] == "USE ROLE ACCOUNTADMIN"
    assert sql_commands[2] == "GRANT OWNERSHIP ON ROLE TEST_ROLE TO ROLE USERADMIN COPY CURRENT GRANTS"


def test_transfer_ownership_with_changes(session_ctx, remote_state):
    remote_state = remote_state.copy()
    remote_state[parse_URN("urn::ABCD123:role/test_role")] = {
        "name": "test_role",
        "owner": "ACCOUNTADMIN",
        "comment": None,
    }

    role = res.Role(name="test_role", comment="This comment has been added", owner="USERADMIN")
    blueprint = Blueprint(resources=[role])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = blueprint._plan(remote_state, manifest)
    assert len(plan) == 2
    assert isinstance(plan[0], UpdateResource)
    assert plan[0].after["comment"] == "This comment has been added"
    assert isinstance(plan[1], TransferOwnership)
    assert plan[1].from_owner == "ACCOUNTADMIN"
    assert plan[1].to_owner == "USERADMIN"
    sql_commands = compile_plan_to_sql(session_ctx, plan)
    assert sql_commands[0] == "USE SECONDARY ROLES ALL"
    assert sql_commands[1] == "USE ROLE ACCOUNTADMIN"
    assert sql_commands[2] == "ALTER ROLE TEST_ROLE SET COMMENT = $$This comment has been added$$"
    assert sql_commands[3] == "USE ROLE ACCOUNTADMIN"
    assert sql_commands[4] == "GRANT OWNERSHIP ON ROLE TEST_ROLE TO ROLE USERADMIN COPY CURRENT GRANTS"


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
    assert isinstance(plan[0], CreateResource)
    assert plan[0].urn == parse_URN("urn::ABCD123:warehouse/test_warehouse")

    sql_commands = compile_plan_to_sql(session_ctx, plan)
    assert sql_commands[0] == "USE SECONDARY ROLES ALL"
    assert sql_commands[1] == "USE ROLE TEST_ROLE"
    assert sql_commands[2].startswith("CREATE WAREHOUSE TEST_WAREHOUSE")


def test_resource_is_transferred_to_custom_role_owner(session_ctx, remote_state):
    session_ctx = session_ctx.copy()
    session_ctx["available_roles"].append(ResourceName("test_role"))

    warehouse = res.Warehouse(name="test_warehouse", owner="test_role")
    blueprint = Blueprint(resources=[warehouse])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = blueprint._plan(remote_state, manifest)
    assert len(plan) == 1
    assert isinstance(plan[0], CreateResource)
    assert plan[0].urn == parse_URN("urn::ABCD123:warehouse/test_warehouse")

    sql_commands = compile_plan_to_sql(session_ctx, plan)
    assert sql_commands[0] == "USE SECONDARY ROLES ALL"
    assert sql_commands[1] == "USE ROLE ACCOUNTADMIN"
    assert sql_commands[2].startswith("CREATE WAREHOUSE TEST_WAREHOUSE")
    assert sql_commands[3] == "GRANT OWNERSHIP ON WAREHOUSE TEST_WAREHOUSE TO ROLE TEST_ROLE COPY CURRENT GRANTS"


def test_resource_cant_be_created(remote_state):
    session_ctx = {
        "account": "SOMEACCT",
        "account_locator": "ABCD123",
        "role": "TEST_ROLE",
        "available_roles": [
            "TEST_ROLE",
        ],
        "role_privileges": {},
    }
    warehouse = res.Warehouse(name="test_warehouse", owner="test_role")
    blueprint = Blueprint(resources=[warehouse])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = blueprint._plan(remote_state, manifest)
    assert len(plan) == 1
    assert isinstance(plan[0], CreateResource)
    assert plan[0].urn == parse_URN("urn::ABCD123:warehouse/test_warehouse")

    with pytest.raises(MissingPrivilegeException):
        compile_plan_to_sql(session_ctx, plan)


def test_grant_with_grant_admin_custom_role(remote_state):
    session_ctx = {
        "account": "SOMEACCT",
        "account_locator": "ABCD123",
        "role": "GRANT_ADMIN",
        "available_roles": [
            "GRANT_ADMIN",
        ],
        "role_privileges": {
            "GRANT_ADMIN": [
                GrantedPrivilege(privilege=AccountPriv.MANAGE_GRANTS, on="ABCD123"),
            ]
        },
    }

    grant = res.RoleGrant(role="GRANT_ADMIN", to_role="SYSADMIN")
    blueprint = Blueprint(resources=[grant])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = blueprint._plan(remote_state, manifest)
    assert len(plan) == 1
    assert isinstance(plan[0], CreateResource)
    assert plan[0].urn == parse_URN("urn::ABCD123:role_grant/GRANT_ADMIN?role=SYSADMIN")
    compile_plan_to_sql(session_ctx, plan)


def test_tag_reference_with_tag_admin_custom_role():
    session_ctx = {
        "account": "SOMEACCT",
        "account_locator": "ABCD123",
        "role": "TAG_ADMIN",
        "available_roles": [
            "TAG_ADMIN",
        ],
        "role_privileges": {
            "TAG_ADMIN": [
                GrantedPrivilege(privilege=AccountPriv.APPLY_TAG, on="ABCD123"),
            ]
        },
        "tags": ["tags.tags.cost_center"],
    }

    remote_state = {
        parse_URN("urn::ABCD123:account/SOMEACCT"): {},
    }

    tag_reference = res.TagReference(
        object_name="SOME_ROLE",
        object_domain="ROLE",
        tags={"tags.tags.cost_center": "finance"},
    )
    blueprint = Blueprint(resources=[tag_reference])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = blueprint._plan(remote_state, manifest)
    assert len(plan) == 1
    assert isinstance(plan[0], CreateResource)
    assert plan[0].urn == parse_URN("urn::ABCD123:tag_reference/SOME_ROLE?domain=ROLE")
    sql_commands = compile_plan_to_sql(session_ctx, plan)
    assert len(sql_commands) == 3
    assert sql_commands[0] == "USE SECONDARY ROLES ALL"
    assert sql_commands[1] == "USE ROLE TAG_ADMIN"
    assert sql_commands[2] == "ALTER ROLE SOME_ROLE SET TAG tags.tags.cost_center='finance'"


def test_owner_is_database_role(session_ctx):
    remote_state = {
        parse_URN("urn::ABCD123:account/SOMEACCT"): {},
        parse_URN("urn::ABCD123:database/SOME_DATABASE"): {},
        parse_URN("urn::ABCD123:schema/SOME_DATABASE.PUBLIC"): {},
    }

    def _plan_for_resources(resources):
        blueprint = Blueprint(resources=resources)
        manifest = blueprint.generate_manifest(session_ctx)
        return blueprint._plan(remote_state, manifest)

    # Specify owner as a string
    database_role = res.DatabaseRole(
        name="SOME_DATABASE_ROLE",
        database="SOME_DATABASE",
    )
    schema = res.Schema(
        name="SOME_SCHEMA",
        database="SOME_DATABASE",
        owner="SOME_DATABASE.SOME_DATABASE_ROLE",
    )
    plan = _plan_for_resources([database_role, schema])
    assert len(plan) == 2

    # Specify owner as a resource
    database_role = res.DatabaseRole(
        name="SOME_DATABASE_ROLE",
        database="SOME_DATABASE",
    )
    schema = res.Schema(
        name="SOME_SCHEMA",
        database="SOME_DATABASE",
        owner=database_role,
    )
    plan = _plan_for_resources([database_role, schema])
    assert len(plan) == 2


def test_blueprint_create_resource_with_database_role_owner(monkeypatch, session_ctx, remote_state):
    def role_can_execute_change(role, change, role_privileges):
        if role in ["SYSADMIN", "USERADMIN"]:
            return True
        return False

    # Patch the deep function
    monkeypatch.setattr("titan.blueprint.role_can_execute_change", role_can_execute_change)

    database = res.Database(name="SOME_DATABASE")
    database_role = res.DatabaseRole(
        name="SOME_DATABASE_ROLE",
        database=database,
    )
    schema = res.Schema(name="test_schema", database=database, owner=database_role)
    blueprint = Blueprint(resources=[database, database_role, schema])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = blueprint._plan(remote_state, manifest)
    assert len(plan) == 3
    sql_commands = compile_plan_to_sql(session_ctx, plan)
    assert len(sql_commands) == 8
    assert sql_commands[0] == "USE SECONDARY ROLES ALL"
    assert sql_commands[1] == "USE ROLE SYSADMIN"
    assert sql_commands[2].startswith("CREATE DATABASE SOME_DATABASE")
    assert sql_commands[3] == "USE ROLE USERADMIN"
    assert sql_commands[4] == "CREATE DATABASE ROLE SOME_DATABASE.SOME_DATABASE_ROLE"
    assert sql_commands[5] == "USE ROLE SYSADMIN"
    assert sql_commands[6].startswith("CREATE SCHEMA SOME_DATABASE.TEST_SCHEMA")
    assert (
        sql_commands[7]
        == "GRANT OWNERSHIP ON SCHEMA SOME_DATABASE.TEST_SCHEMA TO DATABASE ROLE SOME_DATABASE.SOME_DATABASE_ROLE COPY CURRENT GRANTS"
    )


def test_database_with_custom_owner_modifies_public_schema_owner(monkeypatch, session_ctx, remote_state):
    def role_can_execute_change(role, change, role_privileges):
        if role in ["ACCOUNTADMIN", "USERADMIN"]:
            return True
        return False

    monkeypatch.setattr("titan.blueprint.role_can_execute_change", role_can_execute_change)
    role = res.Role(name="CUSTOM_ROLE")
    role_grant = res.RoleGrant(role=role, to_role="SYSADMIN")
    database = res.Database(name="SOME_DATABASE", owner=role)
    blueprint = Blueprint(resources=[database, role, role_grant])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = blueprint._plan(remote_state, manifest)
    assert len(plan) == 3
    sql_commands = compile_plan_to_sql(session_ctx, plan)
    assert len(sql_commands) == 9
    assert sql_commands[0] == "USE SECONDARY ROLES ALL"
    assert sql_commands[1] == "USE ROLE USERADMIN"
    assert sql_commands[2] == "CREATE ROLE CUSTOM_ROLE"
    assert sql_commands[3] == "USE ROLE SECURITYADMIN"
    assert sql_commands[4] == "GRANT ROLE CUSTOM_ROLE TO ROLE SYSADMIN"
    assert sql_commands[5] == "USE ROLE USERADMIN"
    assert sql_commands[6].startswith("CREATE DATABASE SOME_DATABASE")
    assert sql_commands[7] == "GRANT OWNERSHIP ON DATABASE SOME_DATABASE TO ROLE CUSTOM_ROLE COPY CURRENT GRANTS"
    assert sql_commands[8] == "GRANT OWNERSHIP ON SCHEMA SOME_DATABASE.PUBLIC TO ROLE CUSTOM_ROLE COPY CURRENT GRANTS"
