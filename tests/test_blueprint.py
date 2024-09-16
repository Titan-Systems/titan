import json

import pytest

from titan import resources as res
from titan import var
from titan.blueprint import (
    Blueprint,
    CreateResource,
    DuplicateResourceException,
    _merge_pointers,
    compile_plan_to_sql,
    dump_plan,
)
from titan.blueprint_config import BlueprintConfig
from titan.enums import ResourceType, RunMode
from titan.exceptions import InvalidResourceException, MissingVarException, DuplicateResourceException
from titan.identifiers import FQN, URN, parse_URN
from titan.privs import AccountPriv, GrantedPrivilege
from titan.resource_name import ResourceName
from titan.resources.resource import ResourcePointer
from titan.var import VarString


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
        "role_privileges": {
            "USERADMIN": [
                GrantedPrivilege(privilege=AccountPriv.CREATE_ROLE, on="ABCD123"),
                GrantedPrivilege(privilege=AccountPriv.CREATE_USER, on="ABCD123"),
            ],
            "SYSADMIN": [
                GrantedPrivilege(privilege=AccountPriv.CREATE_DATABASE, on="ABCD123"),
                GrantedPrivilege(privilege=AccountPriv.CREATE_WAREHOUSE, on="ABCD123"),
            ],
        },
    }


@pytest.fixture
def remote_state() -> dict:
    return {
        parse_URN("urn::ABCD123:account/SOMEACCT"): {},
    }


@pytest.fixture
def resource_manifest():
    session_ctx = {
        "account": "SOMEACCT",
        "account_locator": "ABCD123",
        "current_role": "SYSADMIN",
        "available_roles": ["SYSADMIN", "USERADMIN"],
    }
    db = res.Database(name="DB")
    schema = res.Schema(name="SCHEMA", database=db)
    table = res.Table(name="TABLE", columns=[{"name": "ID", "data_type": "INT"}])
    schema.add(table)
    view = res.View(name="VIEW", schema=schema, as_="SELECT 1")
    udf = res.PythonUDF(
        name="SOMEUDF",
        returns="VARCHAR",
        args=[],
        runtime_version="3.9",
        handler="main",
        comment="This is a UDF comment",
    )
    schema.add(udf)
    blueprint = Blueprint(name="blueprint", resources=[db, table, schema, view, udf])
    manifest = blueprint.generate_manifest(session_ctx)
    return manifest


def test_blueprint_with_database(resource_manifest):

    db_urn = parse_URN("urn::ABCD123:database/DB")
    assert db_urn in resource_manifest
    assert resource_manifest[db_urn].to_dict() == {
        "name": "DB",
        "owner": "SYSADMIN",
        "comment": None,
        "catalog": None,
        "external_volume": None,
        "data_retention_time_in_days": 1,
        "default_ddl_collation": None,
        "max_data_extension_time_in_days": 14,
        "transient": False,
    }


def test_blueprint_with_schema(resource_manifest):
    schema_urn = parse_URN("urn::ABCD123:schema/DB.SCHEMA")
    assert schema_urn in resource_manifest
    assert resource_manifest[schema_urn].to_dict() == {
        "comment": None,
        "data_retention_time_in_days": 1,
        "default_ddl_collation": None,
        "managed_access": False,
        "max_data_extension_time_in_days": 14,
        "name": "SCHEMA",
        "owner": "SYSADMIN",
        "transient": False,
    }


def test_blueprint_with_view(resource_manifest):
    view_urn = parse_URN("urn::ABCD123:view/DB.SCHEMA.VIEW")
    assert view_urn in resource_manifest
    assert resource_manifest[view_urn].to_dict() == {
        "as_": "SELECT 1",
        "change_tracking": False,
        "columns": None,
        "comment": None,
        "copy_grants": False,
        "name": "VIEW",
        "owner": "SYSADMIN",
        "recursive": None,
        "secure": False,
        "volatile": None,
    }


def test_blueprint_with_table(resource_manifest):
    table_urn = parse_URN("urn::ABCD123:table/DB.SCHEMA.TABLE")
    assert table_urn in resource_manifest
    assert resource_manifest[table_urn].to_dict() == {
        "name": "TABLE",
        "owner": "SYSADMIN",
        "columns": [
            {
                "name": "ID",
                "data_type": "NUMBER(38,0)",
                "collate": None,
                "comment": None,
                "constraint": None,
                "not_null": False,
                "default": None,
                "tags": None,
            }
        ],
        "constraints": None,
        "transient": False,
        "cluster_by": None,
        "enable_schema_evolution": False,
        "data_retention_time_in_days": None,
        "max_data_extension_time_in_days": None,
        "change_tracking": False,
        "default_ddl_collation": None,
        "copy_grants": None,
        "row_access_policy": None,
        "comment": None,
    }


def test_blueprint_with_udf(resource_manifest):
    # parse URN is incorrectly stripping the parens. Not sure what the correct behavior should be
    # udf_urn = parse_URN("urn::ABCD123:function/DB.PUBLIC.SOMEUDF()")
    udf_urn = URN(
        resource_type=ResourceType.FUNCTION,
        fqn=FQN(
            database=ResourceName("DB"),
            schema=ResourceName("SCHEMA"),
            name=ResourceName("SOMEUDF"),
            arg_types=[],
        ),
        account_locator="ABCD123",
    )
    assert udf_urn in resource_manifest
    assert resource_manifest[udf_urn].to_dict() == {
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
    role = res.Role("SOME_ROLE")
    wh = res.Warehouse("WH", owner=role)
    grant = res.RoleGrant(role=role, to_role="SYSADMIN")
    blueprint = Blueprint(name="blueprint", resources=[wh, role, grant])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = blueprint._plan(remote_state, manifest)

    plan_urns = [change.urn for change in plan]
    assert plan_urns == [
        parse_URN("urn::ABCD123:role/SOME_ROLE"),
        parse_URN("urn::ABCD123:role_grant/SOME_ROLE?role=SYSADMIN"),
        parse_URN("urn::ABCD123:warehouse/WH"),
    ]

    changes = compile_plan_to_sql(session_ctx, plan)
    assert len(changes) == 8
    assert changes[0] == "USE SECONDARY ROLES ALL"
    assert changes[1] == "USE ROLE USERADMIN"
    assert changes[2] == "CREATE ROLE SOME_ROLE"
    assert changes[3] == "USE ROLE SECURITYADMIN"
    assert changes[4] == "GRANT ROLE SOME_ROLE TO ROLE SYSADMIN"
    assert changes[5] == f"USE ROLE {session_ctx['role']}"
    assert changes[6].startswith("CREATE WAREHOUSE WH")
    assert changes[7] == "GRANT OWNERSHIP ON WAREHOUSE WH TO ROLE SOME_ROLE COPY CURRENT GRANTS"


def test_blueprint_deduplicate_resources(session_ctx, remote_state):
    blueprint = Blueprint(
        name="blueprint",
        resources=[
            res.Database("DB"),
            ResourcePointer(name="DB", resource_type=ResourceType.DATABASE),
        ],
    )
    manifest = blueprint.generate_manifest(session_ctx)
    plan = blueprint._plan(remote_state, manifest)
    assert len(plan) == 1
    assert isinstance(plan[0], CreateResource)
    assert plan[0].urn == parse_URN("urn::ABCD123:database/DB")

    blueprint = Blueprint(
        name="blueprint",
        resources=[
            res.Database("DB"),
            res.Database("DB", comment="This is a comment"),
        ],
    )
    with pytest.raises(DuplicateResourceException):
        blueprint.generate_manifest(session_ctx)

    blueprint = Blueprint(
        name="blueprint",
        resources=[
            res.Grant(priv="USAGE", on_database="DB", to="SOME_ROLE"),
            res.Grant(priv="USAGE", on_database="DB", to="SOME_ROLE"),
        ],
    )
    with pytest.raises(DuplicateResourceException):
        blueprint.generate_manifest(session_ctx)


def test_blueprint_dont_add_public_schema(session_ctx, remote_state):
    db = res.Database("DB")
    public = ResourcePointer(name="PUBLIC", resource_type=ResourceType.SCHEMA)
    blueprint = Blueprint(
        name="blueprint",
        resources=[db, public],
    )
    manifest = blueprint.generate_manifest(session_ctx)
    plan = blueprint._plan(remote_state, manifest)
    assert len(plan) == 1
    assert isinstance(plan[0], CreateResource)
    assert plan[0].urn == parse_URN("urn::ABCD123:database/DB")


def test_blueprint_implied_container_tree(session_ctx, remote_state):
    remote_state[parse_URN("urn::ABCD123:database/STATIC_DB")] = {}
    remote_state[parse_URN("urn::ABCD123:schema/STATIC_DB.PUBLIC")] = {}
    func = res.JavascriptUDF(
        name="func", args=[], returns="INT", as_="return 1;", database="STATIC_DB", schema="public"
    )
    blueprint = Blueprint(name="blueprint", resources=[func])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = blueprint._plan(remote_state, manifest)
    assert len(plan) == 1
    assert isinstance(plan[0], CreateResource)
    assert plan[0].urn.fqn.name == "FUNC"


def test_blueprint_chained_ownership(session_ctx, remote_state):
    role = res.Role("SOME_ROLE")
    role_grant = res.RoleGrant(role=role, to_role="SYSADMIN")
    db = res.Database("DB", owner=role)
    schema = res.Schema("SCHEMA", database=db, owner=role)
    blueprint = Blueprint(name="blueprint", resources=[db, schema, role_grant, role])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = blueprint._plan(remote_state, manifest)
    assert len(plan) == 4
    assert isinstance(plan[0], CreateResource)
    assert plan[0].urn == parse_URN("urn::ABCD123:role/SOME_ROLE")
    assert isinstance(plan[1], CreateResource)
    assert plan[1].urn == parse_URN("urn::ABCD123:role_grant/SOME_ROLE?role=SYSADMIN")
    assert isinstance(plan[2], CreateResource)
    assert plan[2].urn == parse_URN("urn::ABCD123:database/DB")
    assert isinstance(plan[3], CreateResource)
    assert plan[3].urn == parse_URN("urn::ABCD123:schema/DB.SCHEMA")


def test_blueprint_polymorphic_resource_resolution(session_ctx, remote_state):

    role = res.Role(name="DEMO_ROLE")
    sysad_grant = res.RoleGrant(role=role, to_role="SYSADMIN")
    test_db = res.Database(name="TEST_TITAN", transient=False, data_retention_time_in_days=1, comment="Test Titan")
    schema = res.Schema(name="TEST_SCHEMA", database=test_db, transient=False, comment="Test Titan Schema")
    warehouse = res.Warehouse(name="FAKER_LOADER", auto_suspend=60)

    future_schema_grant = res.FutureGrant(priv="usage", on_future_schemas_in=test_db, to=role)
    post_grant = [future_schema_grant]

    grants = [
        res.Grant(priv="usage", to=role, on=warehouse),
        res.Grant(priv="operate", to=role, on=warehouse),
        res.Grant(priv="usage", to=role, on=test_db),
        # future_schema_grant,
        # x
        # Grant(priv="usage", to=role, on=schema)
    ]

    sales_table = res.Table(
        name="faker_data",
        schema=schema,
        columns=[
            res.Column(name="NAME", data_type="VARCHAR(16777216)"),
            res.Column(name="EMAIL", data_type="VARCHAR(16777216)"),
            res.Column(name="ADDRESS", data_type="VARCHAR(16777216)"),
            res.Column(name="ORDERED_AT_UTC", data_type="NUMBER(38,0)"),
            res.Column(name="EXTRACTED_AT_UTC", data_type="NUMBER(38,0)"),
            res.Column(name="SALES_ORDER_ID", data_type="VARCHAR(16777216)"),
        ],
        comment="Test Table",
    )
    blueprint = Blueprint(
        name="blueprint",
        resources=[
            role,
            sysad_grant,
            # user_grant,
            test_db,
            # *pre_grant,
            schema,
            sales_table,
            # pipe,
            warehouse,
            *grants,
        ],
    )
    manifest = blueprint.generate_manifest(session_ctx)
    plan = blueprint._plan(remote_state, manifest)
    assert len(plan) == 9


def test_blueprint_scope_sorting(session_ctx, remote_state):
    db = res.Database(name="DB")
    schema = res.Schema(name="SCHEMA", database=db)
    view = res.View(name="SOME_VIEW", schema=schema, as_="SELECT 1")
    blueprint = Blueprint(name="blueprint", resources=[view, schema, db])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = blueprint._plan(remote_state, manifest)
    assert len(plan) == 3
    assert isinstance(plan[0], CreateResource)
    assert plan[0].urn == parse_URN("urn::ABCD123:database/DB")
    assert isinstance(plan[1], CreateResource)
    assert plan[1].urn == parse_URN("urn::ABCD123:schema/DB.SCHEMA")
    assert isinstance(plan[2], CreateResource)
    assert plan[2].urn == parse_URN("urn::ABCD123:view/DB.SCHEMA.SOME_VIEW")


def test_blueprint_reference_sorting(session_ctx, remote_state):
    db1 = res.Database(name="DB1")
    db2 = res.Database(name="DB2")
    db2.requires(db1)
    db3 = res.Database(name="DB3")
    db3.requires(db2)
    blueprint = Blueprint(resources=[db3, db1, db2])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = blueprint._plan(remote_state, manifest)
    assert len(plan) == 3
    assert isinstance(plan[0], CreateResource)
    assert plan[0].urn == parse_URN("urn::ABCD123:database/DB1")
    assert isinstance(plan[1], CreateResource)
    assert plan[1].urn == parse_URN("urn::ABCD123:database/DB2")
    assert isinstance(plan[2], CreateResource)
    assert plan[2].urn == parse_URN("urn::ABCD123:database/DB3")


def test_blueprint_ownership_sorting(session_ctx, remote_state):

    role = res.Role(name="SOME_ROLE")
    role_grant = res.RoleGrant(role=role, to_role="SYSADMIN")
    wh = res.Warehouse(name="WH", owner=role)

    blueprint = Blueprint(resources=[wh, role_grant, role])
    manifest = blueprint.generate_manifest(session_ctx)

    plan = blueprint._plan(remote_state, manifest)
    assert len(plan) == 3
    assert isinstance(plan[0], CreateResource)
    assert plan[0].urn == parse_URN("urn::ABCD123:role/SOME_ROLE")
    assert isinstance(plan[1], CreateResource)
    assert plan[1].urn == parse_URN("urn::ABCD123:role_grant/SOME_ROLE?role=SYSADMIN")
    assert isinstance(plan[2], CreateResource)
    assert plan[2].urn == parse_URN("urn::ABCD123:warehouse/WH")

    sql = compile_plan_to_sql(session_ctx, plan)
    assert len(sql) == 8
    assert sql[0] == "USE SECONDARY ROLES ALL"
    assert sql[1] == "USE ROLE USERADMIN"
    assert sql[2] == "CREATE ROLE SOME_ROLE"
    assert sql[3] == "USE ROLE SECURITYADMIN"
    assert sql[4] == "GRANT ROLE SOME_ROLE TO ROLE SYSADMIN"
    assert sql[5] == f"USE ROLE {session_ctx['role']}"
    assert sql[6].startswith("CREATE WAREHOUSE WH")
    assert sql[7] == "GRANT OWNERSHIP ON WAREHOUSE WH TO ROLE SOME_ROLE COPY CURRENT GRANTS"


def test_blueprint_dump_plan_create(session_ctx, remote_state):
    blueprint = Blueprint(resources=[res.Role("role1")])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = blueprint._plan(remote_state, manifest)
    plan_json_str = dump_plan(plan, format="json")
    assert json.loads(plan_json_str) == [
        {
            "action": "CREATE",
            "urn": "urn::ABCD123:role/ROLE1",
            "after": {"name": "ROLE1", "owner": "USERADMIN", "comment": None},
        }
    ]
    plan_str = dump_plan(plan, format="text")
    assert (
        plan_str
        == """
» titan core
» Plan: 1 to create, 0 to update, 0 to transfer, 0 to drop.

+ urn::ABCD123:role/ROLE1 {
  + name    = "ROLE1"
  + owner   = "USERADMIN"
  + comment = None
}

"""
    )


def test_blueprint_dump_plan_update(session_ctx):
    remote_state = {
        parse_URN("urn::ABCD123:account/SOMEACCT"): {},
        parse_URN("urn::ABCD123:role/ROLE1"): {
            "name": "ROLE1",
            "owner": "USERADMIN",
            "comment": "old",
        },
    }
    blueprint = Blueprint(resources=[res.Role("role1", comment="new")])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = blueprint._plan(remote_state, manifest)
    plan_json_str = dump_plan(plan, format="json")
    assert json.loads(plan_json_str) == [
        {
            "action": "UPDATE",
            "urn": "urn::ABCD123:role/ROLE1",
            "before": {"name": "ROLE1", "owner": "USERADMIN", "comment": "old"},
            "after": {"name": "ROLE1", "owner": "USERADMIN", "comment": "new"},
            "delta": {"comment": "new"},
        }
    ]
    plan_str = dump_plan(plan, format="text")
    assert (
        plan_str
        == """
» titan core
» Plan: 0 to create, 1 to update, 0 to transfer, 0 to drop.

~ urn::ABCD123:role/ROLE1 {
  ~ comment = "old" -> "new"
}

"""
    )


def test_blueprint_dump_plan_transfer(session_ctx):
    remote_state = {
        parse_URN("urn::ABCD123:account/SOMEACCT"): {},
        parse_URN("urn::ABCD123:role/ROLE1"): {
            "name": "ROLE1",
            "owner": "ACCOUNTADMIN",
            "comment": None,
        },
    }
    blueprint = Blueprint(resources=[res.Role("role1", owner="USERADMIN")])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = blueprint._plan(remote_state, manifest)
    plan_json_str = dump_plan(plan, format="json")
    assert json.loads(plan_json_str) == [
        {
            "action": "TRANSFER",
            "urn": "urn::ABCD123:role/ROLE1",
            "from_owner": "ACCOUNTADMIN",
            "to_owner": "USERADMIN",
        }
    ]
    plan_str = dump_plan(plan, format="text")
    assert (
        plan_str
        == """
» titan core
» Plan: 0 to create, 0 to update, 1 to transfer, 0 to drop.

~ urn::ABCD123:role/ROLE1 {
  ~ owner = "ACCOUNTADMIN" -> "USERADMIN"
}

"""
    )


def test_blueprint_dump_plan_drop(session_ctx):
    remote_state = {
        parse_URN("urn::ABCD123:account/SOMEACCT"): {},
        parse_URN("urn::ABCD123:role/ROLE1"): {
            "name": "ROLE1",
            "owner": "ACCOUNTADMIN",
            "comment": None,
        },
    }
    blueprint = Blueprint(resources=[], run_mode="SYNC", allowlist=[ResourceType.ROLE])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = blueprint._plan(remote_state, manifest)
    plan_json_str = dump_plan(plan, format="json")
    assert json.loads(plan_json_str) == [
        {
            "action": "DROP",
            "urn": "urn::ABCD123:role/ROLE1",
            "before": {"name": "ROLE1", "owner": "ACCOUNTADMIN", "comment": None},
        }
    ]
    plan_str = dump_plan(plan, format="text")
    assert (
        plan_str
        == """
» titan core
» Plan: 0 to create, 0 to update, 0 to transfer, 1 to drop.

- urn::ABCD123:role/ROLE1

"""
    )


def test_blueprint_vars(session_ctx):
    blueprint = Blueprint(
        resources=[res.Role(name="role", comment=var.role_comment)],
        vars={"role_comment": "var role comment"},
    )
    manifest = blueprint.generate_manifest(session_ctx)
    assert manifest.resources[1]._data.comment == "var role comment"

    role = res.Role(name="role", comment="some comment {{ var.suffix }}")
    assert isinstance(role._data.comment, VarString)
    blueprint = Blueprint(
        resources=[role],
        vars={"suffix": "1234"},
    )
    manifest = blueprint.generate_manifest(session_ctx)
    assert manifest.resources[1]._data.comment == "some comment 1234"

    role = res.Role(name=var.role_name)
    assert isinstance(role.name, VarString)
    blueprint = Blueprint(
        resources=[role],
        vars={"role_name": "role123"},
    )
    manifest = blueprint.generate_manifest(session_ctx)
    assert manifest.resources[1].name == "role123"

    role = res.Role(name="role_{{ var.suffix }}")
    assert isinstance(role.name, VarString)
    blueprint = Blueprint(
        resources=[role],
        vars={"suffix": "5678"},
    )
    manifest = blueprint.generate_manifest(session_ctx)
    assert manifest.resources[1].name == "role_5678"


def test_blueprint_vars_spec(session_ctx):
    blueprint = Blueprint(
        resources=[res.Role(name="role", comment=var.role_comment)],
        vars_spec=[
            {
                "name": "role_comment",
                "type": "string",
                "default": "var role comment",
            }
        ],
    )
    assert blueprint._config.vars == {"role_comment": "var role comment"}
    manifest = blueprint.generate_manifest(session_ctx)
    assert manifest.resources[1]._data.comment == "var role comment"

    with pytest.raises(MissingVarException):
        blueprint = Blueprint(
            resources=[res.Role(name="role", comment=var.role_comment)],
            vars_spec=[{"name": "role_comment", "type": "string"}],
        )

    blueprint = Blueprint(resources=[res.Role(name="role", comment=var.role_comment)])
    with pytest.raises(MissingVarException):
        blueprint.generate_manifest(session_ctx)


def test_blueprint_allowlist(session_ctx, remote_state):
    blueprint = Blueprint(
        resources=[res.Role(name="role1")],
        allowlist=[ResourceType.ROLE],
    )
    manifest = blueprint.generate_manifest(session_ctx)
    plan = blueprint._plan(remote_state, manifest)
    assert len(plan) == 1

    blueprint = Blueprint(allowlist=["ROLE"])
    assert blueprint._config.allowlist == [ResourceType.ROLE]
    with pytest.raises(InvalidResourceException):
        blueprint.add(res.Database(name="db1"))

    with pytest.raises(InvalidResourceException):
        blueprint = Blueprint(
            resources=[res.Role(name="role1")],
            allowlist=[ResourceType.DATABASE],
        )


def test_blueprint_config_validation():
    with pytest.raises(ValueError):
        BlueprintConfig(run_mode=None)
    with pytest.raises(ValueError):
        BlueprintConfig(run_mode="non-existent-mode")
    with pytest.raises(ValueError):
        BlueprintConfig(run_mode="sync")
    with pytest.raises(ValueError):
        BlueprintConfig(allowlist=[])

    bp = Blueprint(run_mode="SYNC", allowlist=["ROLE"])
    assert bp._config.run_mode == RunMode.SYNC
    bp = Blueprint(run_mode="CREATE-OR-UPDATE")
    assert bp._config.run_mode == RunMode.CREATE_OR_UPDATE

    bp = Blueprint(allowlist=["ROLE"])
    assert bp._config.allowlist == [ResourceType.ROLE]

    with pytest.raises(ValueError):
        Blueprint(allowlist=["non-existent-resource-type"])


def test_merge_account_scoped_resources():
    resources = [
        res.Database(name="DB1"),
        ResourcePointer(name="DB1", resource_type=ResourceType.DATABASE),
    ]
    merged = _merge_pointers(resources)
    assert len(merged) == 1
    assert isinstance(merged[0], res.Database)
    assert merged[0].name == "DB1"

    resources = [
        res.Database(name="DB1"),
        res.Database(name="DB2"),
    ]
    merged = _merge_pointers(resources)
    assert len(merged) == 2


def test_merge_account_scoped_resources_fail():
    resources = [
        res.Database(name="DB1"),
        res.Database(name="DB1", comment="namespace conflict"),
    ]
    with pytest.raises(DuplicateResourceException):
        _merge_pointers(resources)
