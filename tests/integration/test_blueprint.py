import os

import pytest

from tests.helpers import safe_fetch
from titan import data_provider
from titan import resources as res
from titan.blueprint import (
    Blueprint,
    CreateResource,
    DropResource,
    MissingResourceException,
    UpdateResource,
    compile_plan_to_sql,
)
from titan.client import reset_cache
from titan.enums import ResourceType

TEST_ROLE = os.environ.get("TEST_SNOWFLAKE_ROLE")

pytestmark = pytest.mark.requires_snowflake


@pytest.fixture(autouse=True)
def clear_cache():
    reset_cache()
    yield


@pytest.fixture(scope="session")
def user(suffix, cursor, marked_for_cleanup):
    user = res.User(name=f"TEST_USER_{suffix}".upper(), owner="ACCOUNTADMIN")
    cursor.execute(user.create_sql())
    marked_for_cleanup.append(user)
    return user


@pytest.fixture(scope="session")
def role(suffix, cursor, marked_for_cleanup):
    role = res.Role(name=f"TEST_ROLE_{suffix}".upper(), owner="ACCOUNTADMIN")
    cursor.execute(role.create_sql())
    marked_for_cleanup.append(role)
    return role


@pytest.fixture(scope="session")
def noprivs_role(cursor, test_db, marked_for_cleanup):
    role = res.Role(name="NOPRIVS")
    cursor.execute(role.create_sql(if_not_exists=True))
    cursor.execute(f"GRANT ROLE NOPRIVS TO USER {cursor.connection.user}")
    cursor.execute(f"GRANT USAGE ON DATABASE {test_db} TO ROLE NOPRIVS")
    cursor.execute(f"GRANT USAGE ON SCHEMA {test_db}.PUBLIC TO ROLE NOPRIVS")
    marked_for_cleanup.append(role)
    return role.name


def test_plan(cursor, user, role):
    session = cursor.connection
    blueprint = Blueprint(name="test")
    role_grant = res.RoleGrant(role=role, to_user=user)
    blueprint.add(role_grant)
    changes = blueprint.plan(session)
    assert len(changes) == 1
    blueprint.apply(session, changes)
    role_grant_remote = data_provider.fetch_role_grant(session, role_grant.fqn)
    assert role_grant_remote


def test_blueprint_plan_no_changes(cursor, user, role):
    session = cursor.connection

    def _blueprint():
        blueprint = Blueprint(name="test_no_changes")
        # Assuming role_grant already exists in the setup for this test
        role_grant = res.RoleGrant(role=role, to_user=user)
        blueprint.add(role_grant)
        return blueprint

    bp = _blueprint()
    # Apply the initial blueprint to ensure the state is as expected
    initial_changes = bp.plan(session)
    bp.apply(session, initial_changes)

    # Plan again to verify no changes are detected
    bp = _blueprint()
    subsequent_changes = bp.plan(session)
    assert len(subsequent_changes) == 0, "Expected no changes in the blueprint plan but found some."


def test_blueprint_zero_drift_after_apply(cursor, test_db, suffix, marked_for_cleanup):
    session = cursor.connection
    blueprint = Blueprint(name="test_zero_drift_after_apply")
    schema = res.Schema(name=f"zero_drift_schema_{suffix}", database=test_db, owner=TEST_ROLE)
    tbl = res.Table(
        name=f"zero_drift_table_{suffix}",
        database=test_db,
        schema=schema,
        columns=[res.Column(name="ID", data_type="NUMBER(38,0)")],
        owner=TEST_ROLE,
    )
    marked_for_cleanup.append(schema)
    blueprint.add(schema, tbl)
    initial_plan = blueprint.plan(session)
    assert len(initial_plan) == 2
    blueprint.apply(session, initial_plan)

    # Plan again to verify no changes are detected
    reset_cache()
    blueprint = Blueprint(name="test_zero_drift_after_apply")
    schema = res.Schema(name=f"zero_drift_schema_{suffix}", database=test_db, owner=TEST_ROLE)
    tbl = res.Table(
        name=f"zero_drift_table_{suffix}",
        database=test_db,
        schema=schema,
        columns=[res.Column(name="ID", data_type="NUMBER(38,0)")],
        owner=TEST_ROLE,
    )
    blueprint.add(schema, tbl)
    subsequent_changes = blueprint.plan(session)
    assert len(subsequent_changes) == 0, "Expected no changes in the blueprint plan but found some."


def test_blueprint_modify_resource(cursor, suffix, marked_for_cleanup):
    cursor.execute(f"CREATE WAREHOUSE modify_me_{suffix}")
    session = cursor.connection
    blueprint = Blueprint(name="test_remove_resource")
    warehouse = res.Warehouse(
        name=f"modify_me_{suffix}",
        auto_suspend=60,
        owner=TEST_ROLE,
    )
    marked_for_cleanup.append(warehouse)
    blueprint.add(warehouse)
    plan = blueprint.plan(session)
    assert len(plan) == 1
    assert isinstance(plan[0], UpdateResource)
    assert plan[0].urn.fqn.name == f"MODIFY_ME_{suffix}"
    assert plan[0].delta == {"auto_suspend": 60}

    sql_commands = blueprint.apply(session, plan)
    assert sql_commands == [
        "USE SECONDARY ROLES ALL",
        f"USE ROLE {TEST_ROLE}",
        f"ALTER WAREHOUSE MODIFY_ME_{suffix} SET AUTO_SUSPEND = 60",
    ]


def test_blueprint_crossreferenced_database(cursor):
    session = cursor.connection
    bp = Blueprint(name="failing-reference")
    schema = res.Schema(name="MY_SCHEMA", database="some_db")
    bp.add(
        res.FutureGrant(priv="SELECT", on_future_views_in=schema, to="MY_ROLE"),
        res.Role(name="MY_ROLE"),
        res.Database(name="SOME_DB"),
        schema,
    )
    plan = bp.plan(session)
    assert len(plan) == 4


def test_blueprint_name_equivalence_drift(cursor, suffix, marked_for_cleanup):

    # Create user
    user_name = f"TEST_USER_{suffix}_NAME_EQUIVALENCE".upper()
    user = res.User(name=user_name, login_name=user_name, owner="ACCOUNTADMIN")
    cursor.execute(user.create_sql(if_not_exists=True))
    marked_for_cleanup.append(user)

    session = cursor.connection
    blueprint = Blueprint(name="test_name_equivalence_drift")
    blueprint.add(res.User(name=user_name, login_name=user_name.lower(), owner="ACCOUNTADMIN"))
    plan = blueprint.plan(session)

    assert len(plan) == 0, "Expected no changes in the blueprint plan but found some."


def test_blueprint_plan_sql(cursor, user):
    session = cursor.connection

    blueprint = Blueprint(name="test_add_database")
    somedb = res.Database(name="this_database_does_not_exist")
    blueprint.add(somedb)
    plan = blueprint.plan(session)

    session_ctx = data_provider.fetch_session(session)

    sql_commands = compile_plan_to_sql(session_ctx, plan)

    assert sql_commands == [
        "USE SECONDARY ROLES ALL",
        "USE ROLE SYSADMIN",
        "CREATE DATABASE THIS_DATABASE_DOES_NOT_EXIST DATA_RETENTION_TIME_IN_DAYS = 1 MAX_DATA_EXTENSION_TIME_IN_DAYS = 14",
    ]

    blueprint = Blueprint(name="test_modify_user")
    modified_user = res.User(name=user.name, owner=user.owner, display_name="new_display_name")
    blueprint.add(modified_user)
    plan = blueprint.plan(session)

    sql_commands = compile_plan_to_sql(session_ctx, plan)

    assert sql_commands == [
        "USE SECONDARY ROLES ALL",
        "USE ROLE ACCOUNTADMIN",
        f"ALTER USER {user.name} SET DISPLAY_NAME = $$new_display_name$$",
    ]


def test_blueprint_missing_resource_pointer(cursor):
    session = cursor.connection
    grant = res.Grant.from_sql("GRANT ALL ON WAREHOUSE missing_wh TO ROLE SOMEROLE")
    blueprint = Blueprint(name="blueprint", resources=[grant])
    with pytest.raises(MissingResourceException):
        blueprint.plan(session)


def test_blueprint_present_resource_pointer(cursor):
    session = cursor.connection
    grant = res.Grant.from_sql("GRANT AUDIT ON ACCOUNT TO ROLE THISROLEDOESNTEXIST")
    role = res.Role(name="THISROLEDOESNTEXIST")
    blueprint = Blueprint(name="blueprint", resources=[grant, role])
    plan = blueprint.plan(session)
    assert len(plan) == 2


def test_blueprint_missing_database_inferred_from_session_context(cursor):
    session = cursor.connection
    func = res.JavascriptUDF(name="func", args=[], returns="INT", as_="return 1;", schema="public")
    blueprint = Blueprint(name="blueprint", resources=[func])
    blueprint.plan(session)


def test_blueprint_all_grant_forces_add(cursor, test_db, role):
    cursor.execute(f"GRANT USAGE ON DATABASE {test_db} TO ROLE {role.name}")
    session = cursor.connection
    all_grant = res.Grant(priv="ALL", on_database=test_db, to=role, owner=TEST_ROLE)
    blueprint = Blueprint(name="blueprint", resources=[all_grant])
    plan = blueprint.plan(session)
    assert len(plan) == 1
    assert isinstance(plan[0], CreateResource)


def test_blueprint_sync_dont_remove_system_schemas(cursor, test_db):
    session = cursor.connection
    blueprint = Blueprint(
        name="blueprint",
        resources=[
            res.Schema(name="INFORMATION_SCHEMA", database=test_db),
        ],
        run_mode="sync",
        allowlist=[ResourceType.SCHEMA],
    )
    plan = blueprint.plan(session)
    assert len(plan) == 0


def test_blueprint_sync_resource_missing_from_remote_state(cursor, test_db):
    session = cursor.connection
    blueprint = Blueprint(
        name="blueprint",
        resources=[
            res.Schema(name="ABSENT", database=test_db),
            res.Schema(name="INFORMATION_SCHEMA", database=test_db),
        ],
        run_mode="sync",
        allowlist=[ResourceType.SCHEMA],
    )
    plan = blueprint.plan(session)
    assert len(plan) == 1
    assert isinstance(plan[0], CreateResource)
    assert plan[0].urn.fqn.name == "ABSENT"


def test_blueprint_sync_plan_matches_remote_state(cursor, test_db):
    session = cursor.connection
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {test_db}.PRESENT")
    blueprint = Blueprint(
        name="blueprint",
        resources=[
            res.Schema(name="PRESENT", database=test_db, owner=TEST_ROLE),
            res.Schema(name="INFORMATION_SCHEMA", database=test_db),
        ],
        run_mode="sync",
        allowlist=[ResourceType.SCHEMA],
    )
    plan = blueprint.plan(session)
    assert len(plan) == 0


def test_blueprint_sync_remote_state_contains_extra_resource(cursor, test_db):
    session = cursor.connection
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {test_db}.PRESENT")
    blueprint = Blueprint(
        name="blueprint",
        resources=[res.Schema(name="INFORMATION_SCHEMA", database=test_db)],
        run_mode="sync",
        allowlist=[ResourceType.SCHEMA],
    )
    plan = blueprint.plan(session)
    assert len(plan) == 1
    assert isinstance(plan[0], DropResource)
    assert plan[0].urn.fqn.name == "PRESENT"


def test_blueprint_quoted_references(cursor):
    session = cursor.connection

    cursor.execute('CREATE USER IF NOT EXISTS "info@applytitan.com"')
    cursor.execute('GRANT ROLE STATIC_ROLE TO USER "info@applytitan.com"')

    blueprint = Blueprint(
        name="test_quoted_references",
        resources=[res.RoleGrant(role="STATIC_ROLE", to_user="info@applytitan.com")],
    )
    plan = blueprint.plan(session)
    cursor.execute('DROP USER IF EXISTS "info@applytitan.com"')

    assert len(plan) == 0


def test_blueprint_grant_with_lowercase_priv_drift(cursor, suffix, marked_for_cleanup):
    session = cursor.connection

    def _blueprint():
        blueprint = Blueprint()
        role = res.Role(name=f"TITAN_TEST_ROLE_{suffix}")
        warehouse = res.Warehouse(
            name=f"TITAN_TEST_WAREHOUSE_{suffix}",
            warehouse_size="xsmall",
            auto_suspend=60,
        )
        grant = res.Grant(priv="usage", to=role, on=warehouse)
        marked_for_cleanup.append(role)
        marked_for_cleanup.append(warehouse)
        blueprint.add(role, warehouse, grant)
        return blueprint

    bp = _blueprint()
    plan = bp.plan(session)
    assert len(plan) == 3

    bp = _blueprint()
    bp.apply(session, plan)
    plan = bp.plan(session)
    assert len(plan) == 0


def test_blueprint_with_nested_database(cursor):
    session = cursor.connection
    bp = Blueprint(name="failing-reference")
    schema = res.Schema(name="static_database.static_schema")
    bp.add(res.FutureGrant(priv="SELECT", on_future_views_in=schema, to="STATIC_ROLE"))
    plan = bp.plan(session)
    assert len(plan) == 1


def test_blueprint_quoted_identifier_drift(cursor, test_db, suffix):
    session = cursor.connection

    cursor.execute(f'CREATE SCHEMA {test_db}."multiCaseString_{suffix}"')

    blueprint = Blueprint(
        resources=[res.Schema(name=f'"multiCaseString_{suffix}"', database=test_db, owner=TEST_ROLE)],
    )
    plan = blueprint.plan(session)
    cursor.execute(f'DROP SCHEMA {test_db}."multiCaseString_{suffix}"')

    assert len(plan) == 0


def test_blueprint_grant_role_to_public(cursor, suffix, marked_for_cleanup):
    session = cursor.connection

    role_name = f"role{suffix}_grant_role_to_public"
    role = res.Role(name=role_name)
    marked_for_cleanup.append(role)
    grant = res.RoleGrant(role=role, to_role="PUBLIC")
    blueprint = Blueprint(resources=[role, grant])
    blueprint.apply(session)
    role_data = safe_fetch(cursor, role.urn)
    assert role_data is not None
    assert role_data["name"] == role.name

    grant_data = safe_fetch(cursor, grant.urn)
    assert grant_data is not None
    assert grant_data["role"] == role_name
    assert grant_data["to_role"] == "PUBLIC"


def test_blueprint_account_grants(cursor, suffix, marked_for_cleanup):
    session = cursor.connection

    role_name = f"ROLE{suffix}_ACCOUNT_GRANTS"
    role = res.Role(name=role_name)
    marked_for_cleanup.append(role)
    grant = res.Grant(priv="CREATE DATABASE", on="ACCOUNT", to=role)
    blueprint = Blueprint(resources=[role, grant])
    blueprint.apply(session)
    role_data = safe_fetch(cursor, role.urn)
    assert role_data is not None
    assert role_data["name"] == role.name

    grant_data = safe_fetch(cursor, grant.urn)
    assert grant_data is not None
    assert grant_data["to"] == role_name
    assert grant_data["priv"] == "CREATE DATABASE"
    assert grant_data["on"] == "ACCOUNT"
    assert grant_data["on_type"] == "ACCOUNT"


def test_blueprint_create_resource_with_database_role_owner(cursor, suffix, test_db):
    session = cursor.connection

    database_role = res.DatabaseRole(
        name=f"TEST_BLUEPRINT_CREATE_RESOURCE_WITH_DATABASE_ROLE_OWNER_{suffix}",
        database=test_db,
        owner=TEST_ROLE,
    )
    schema = res.Schema(
        name="test_schema",
        database=test_db,
        owner=database_role,
    )
    blueprint = Blueprint(resources=[database_role, schema])
    plan = blueprint.plan(session)
    assert len(plan) == 2

    blueprint.apply(session, plan)

    schema_data = safe_fetch(cursor, schema.urn)
    assert schema_data is not None
    assert schema_data["name"] == schema.name
    assert schema_data["owner"] == str(database_role.fqn)


def test_blueprint_database_params_passed_to_public_schema(cursor, suffix):
    session = cursor.connection

    def _database():
        return res.Database(
            name=f"test_db_params_passed_to_public_schema_{suffix}",
            data_retention_time_in_days=2,
            max_data_extension_time_in_days=2,
            default_ddl_collation="en_US",
        )

    database = _database()
    blueprint = Blueprint(resources=[database])
    plan = blueprint.plan(session)
    assert len(plan) == 1
    blueprint.apply(session, plan)
    database_data = safe_fetch(cursor, database.urn)
    assert database_data is not None
    assert database_data["data_retention_time_in_days"] == 2
    assert database_data["max_data_extension_time_in_days"] == 2
    assert database_data["default_ddl_collation"] == "en_US"
    database = _database()
    blueprint = Blueprint(resources=[database])
    plan = blueprint.plan(session)
    assert len(plan) == 0
