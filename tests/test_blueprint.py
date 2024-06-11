import pytest

from titan import (
    Blueprint,
    Database,
    Role,
    Grant,
    PythonUDF,
    Schema,
    Table,
    View,
)
from titan.blueprint import Action
from titan.enums import ResourceType
from titan.identifiers import URN, FQN
from titan.parse import parse_URN


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
    return {parse_URN("urn::ABCD123:account/SOMEACCT"): {}}


def test_blueprint_with_resources():
    session_ctx = {"account": "SOMEACCT", "account_locator": "ABCD123"}
    db = Database(name="DB")
    schema = Schema(name="SCHEMA", database=db)
    table = Table(name="TABLE", columns=[{"name": "ID", "data_type": "INT"}])
    schema.add(table)
    view = View(name="VIEW", schema=schema, as_="SELECT 1")
    udf = PythonUDF(
        name="SOMEUDF",
        returns="VARCHAR",
        args=[],
        runtime_version="3.9",
        handler="main",
        comment="This is a UDF comment",
    )
    blueprint = Blueprint(name="blueprint", resources=[db, table, schema, view, udf])
    manifest = blueprint.generate_manifest(session_ctx)

    db_urn = parse_URN("urn::ABCD123:database/DB")
    assert db_urn in manifest
    assert manifest[db_urn] == {
        "name": "DB",
        "owner": "SYSADMIN",
        "comment": None,
        "data_retention_time_in_days": 1,
        "default_ddl_collation": None,
        "max_data_extension_time_in_days": 14,
        "tags": None,
        "transient": False,
    }

    schema_urn = parse_URN("urn::ABCD123:schema/DB.SCHEMA")
    assert schema_urn in manifest
    assert manifest[schema_urn] == {
        "comment": None,
        "data_retention_time_in_days": 1,
        "default_ddl_collation": None,
        "managed_access": False,
        "max_data_extension_time_in_days": 14,
        "name": "SCHEMA",
        "owner": "SYSADMIN",
        "tags": None,
        "transient": False,
    }
    view_urn = parse_URN("urn::ABCD123:view/DB.SCHEMA.VIEW")
    assert view_urn in manifest
    assert manifest[view_urn] == {
        "as_": "SELECT 1",
        "change_tracking": None,
        "columns": None,
        "comment": None,
        "copy_grants": None,
        "name": "VIEW",
        "owner": "SYSADMIN",
        "recursive": None,
        "secure": None,
        "tags": None,
        "volatile": None,
    }
    table_urn = parse_URN("urn::ABCD123:table/DB.SCHEMA.TABLE")
    assert table_urn in manifest
    assert manifest[table_urn] == {
        "name": "TABLE",
        "owner": "SYSADMIN",
        "columns": [{"name": "ID", "data_type": "INT"}],
        "constraints": None,
        "volatile": False,
        "transient": False,
        "cluster_by": None,
        "enable_schema_evolution": False,
        "data_retention_time_in_days": None,
        "max_data_extension_time_in_days": None,
        "change_tracking": False,
        "default_ddl_collation": None,
        "copy_grants": False,
        "row_access_policy": None,
        "tags": None,
        "comment": None,
    }
    # parse URN is incorrectly stripping the parens. Not sure what the correct behavior should be
    # udf_urn = parse_URN("urn::ABCD123:function/DB.PUBLIC.SOMEUDF()")
    udf_urn = URN(
        resource_type=ResourceType.FUNCTION,
        fqn=FQN(
            database="DB",
            schema="PUBLIC",
            name="SOMEUDF()",
        ),
        account_locator="ABCD123",
    )
    assert udf_urn in manifest
    assert manifest[udf_urn] == {
        "name": "SOMEUDF",
        "owner": "SYSADMIN",
        "returns": "VARCHAR",
        "handler": "main",
        "runtime_version": "3.9",
        "comment": "This is a UDF comment",
        "args": [],
        "as_": None,
        "copy_grants": False,
        "language": "PYTHON",
        "external_access_integrations": None,
        "imports": None,
        "null_handling": None,
        "packages": None,
        "secrets": None,
        "secure": None,
        "volatility": None,
    }


def test_blueprint_resource_owned_by_plan_role(session_ctx, remote_state):
    role = Role("SOME_ROLE")
    db = Database("DB", owner=role)
    blueprint = Blueprint(name="blueprint", resources=[db, role])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = blueprint._plan(remote_state, manifest)

    assert len(plan) == 3
    assert plan[0].action == Action.ADD
    assert plan[0].urn == parse_URN("urn::ABCD123:role/SOME_ROLE")
    assert plan[1].action == Action.ADD
    assert plan[1].urn == parse_URN("urn::ABCD123:database/DB")
    assert plan[2].action == Action.ADD
    assert plan[2].urn == parse_URN("urn::ABCD123:grant/SOME_ROLE?priv=OWNERSHIP&on=database/DB")

    changes = blueprint._compile_plan_to_sql(session_ctx, plan)
    assert len(changes) == 7
    assert changes[0] == "USE SECONDARY ROLES ALL"
    assert changes[1] == "USE ROLE USERADMIN"
    assert changes[2] == "CREATE ROLE SOME_ROLE"
    assert changes[3] == "USE ROLE SYSADMIN"
    assert changes[4] == "CREATE DATABASE DB DATA_RETENTION_TIME_IN_DAYS = 1 MAX_DATA_EXTENSION_TIME_IN_DAYS = 14"
    assert changes[5] == "USE ROLE SYSADMIN"
    assert changes[6] == "GRANT OWNERSHIP ON DATABASE DB TO SOME_ROLE"


def test_blueprint_duplicate_resources(session_ctx, remote_state):
    blueprint = Blueprint(name="blueprint", resources=[Database("DB"), Database("DB")])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = blueprint._plan(remote_state, manifest)
    assert len(plan) == 1
    assert plan[0].action == Action.ADD
    assert plan[0].urn == parse_URN("urn::ABCD123:database/DB")

    blueprint = Blueprint(
        name="blueprint",
        resources=[
            Grant(priv="OWNERSHIP", on_database="DB", to="SOME_ROLE"),
            Grant(priv="OWNERSHIP", on_database="DB", to="SOME_ROLE"),
        ],
    )
    manifest = blueprint.generate_manifest(session_ctx)
    plan = blueprint._plan(remote_state, manifest)
    assert len(plan) == 1
    assert plan[0].action == Action.ADD
    assert plan[0].urn == parse_URN("urn::ABCD123:grant/SOME_ROLE?priv=OWNERSHIP&on=database/DB")
