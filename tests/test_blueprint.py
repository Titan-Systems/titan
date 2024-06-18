import pytest


from titan import resources as res
from titan.blueprint import Action, Blueprint, MarkedForReplacementException
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
    return {
        parse_URN("urn::ABCD123:account/SOMEACCT"): {},
    }


def test_blueprint_with_resources():
    session_ctx = {"account": "SOMEACCT", "account_locator": "ABCD123"}
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
        "columns": [
            {
                "name": "ID",
                "data_type": "INT",
                "collate": None,
                "comment": None,
                "constraint": None,
                "not_null": False,
                "default": None,
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
    role = res.Role("SOME_ROLE")
    db = res.Database("DB", owner=role)
    blueprint = Blueprint(name="blueprint", resources=[db, role])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = blueprint._plan(remote_state, manifest)

    assert len(plan) == 2
    assert plan[0].action == Action.ADD
    assert plan[0].urn == parse_URN("urn::ABCD123:role/SOME_ROLE")
    assert plan[1].action == Action.ADD
    assert plan[1].urn == parse_URN("urn::ABCD123:database/DB")

    changes = blueprint._compile_plan_to_sql(session_ctx, plan)
    assert len(changes) == 6
    assert changes[0] == "USE SECONDARY ROLES ALL"
    assert changes[1] == "USE ROLE USERADMIN"
    assert changes[2] == "CREATE ROLE SOME_ROLE"
    assert changes[3] == "USE ROLE SYSADMIN"
    assert changes[4] == "CREATE DATABASE DB DATA_RETENTION_TIME_IN_DAYS = 1 MAX_DATA_EXTENSION_TIME_IN_DAYS = 14"
    assert changes[5] == "GRANT OWNERSHIP ON DATABASE DB TO SOME_ROLE"


def test_blueprint_deduplicate_resources(session_ctx, remote_state):
    blueprint = Blueprint(name="blueprint", resources=[res.Database("DB"), res.Database("DB")])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = blueprint._plan(remote_state, manifest)
    assert len(plan) == 1
    assert plan[0].action == Action.ADD
    assert plan[0].urn == parse_URN("urn::ABCD123:database/DB")

    blueprint = Blueprint(
        name="blueprint",
        resources=[
            res.Grant(priv="OWNERSHIP", on_database="DB", to="SOME_ROLE"),
            res.Grant(priv="OWNERSHIP", on_database="DB", to="SOME_ROLE"),
        ],
    )
    manifest = blueprint.generate_manifest(session_ctx)
    plan = blueprint._plan(remote_state, manifest)
    assert len(plan) == 1
    assert plan[0].action == Action.ADD
    assert plan[0].urn == parse_URN("urn::ABCD123:grant/SOME_ROLE?priv=OWNERSHIP&on=database/DB")


def test_blueprint_dont_add_public_schema(session_ctx, remote_state):
    db = res.Database("DB")
    public = res.Schema(name="PUBLIC", database=db, comment="this is ignored")
    blueprint = Blueprint(
        name="blueprint",
        resources=[db, public],
    )
    manifest = blueprint.generate_manifest(session_ctx)
    plan = blueprint._plan(remote_state, manifest)
    assert len(plan) == 1
    assert plan[0].action == Action.ADD
    assert plan[0].urn == parse_URN("urn::ABCD123:database/DB")


def test_blueprint_implied_container_tree(session_ctx, remote_state):
    remote_state[parse_URN("urn::ABCD123:database/STATIC_DB")] = {}
    remote_state[parse_URN("urn::ABCD123:schema/STATIC_DB.PUBLIC")] = {}
    func = res.JavascriptUDF(name="func", returns="INT", as_="return 1;", database="STATIC_DB", schema="public")
    blueprint = Blueprint(name="blueprint", resources=[func])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = blueprint._plan(remote_state, manifest)
    assert len(plan) == 1
    assert plan[0].action == Action.ADD
    assert plan[0].urn.fqn.name == "func"


def test_blueprint_chained_ownership(session_ctx, remote_state):
    role = res.Role("SOME_ROLE")
    db = res.Database("DB", owner=role)
    schema = res.Schema("SCHEMA", database=db, owner=role)
    blueprint = Blueprint(name="blueprint", resources=[db, schema])
    manifest = blueprint.generate_manifest(session_ctx)
    plan = blueprint._plan(remote_state, manifest)
    # assert len(plan) == 1
    # assert plan[0].action == Action.ADD
    # assert plan[0].urn.fqn.name == "func"


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
