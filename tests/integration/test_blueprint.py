import pytest

from titan import data_provider
from titan.blueprint import Action, Blueprint, MissingResourceException, plan_sql
from titan.client import reset_cache
from titan.enums import ResourceType
from titan.resources import (
    FutureGrant,
    Database,
    Grant,
    JavascriptUDF,
    User,
    Role,
    RoleGrant,
    Schema,
)

from tests.helpers import get_json_fixtures

JSON_FIXTURES = list(get_json_fixtures())


@pytest.fixture(autouse=True)
def clear_cache():
    reset_cache()
    yield


@pytest.fixture(
    params=JSON_FIXTURES,
    ids=[resource_cls.__name__ for resource_cls, _ in JSON_FIXTURES],
    scope="function",
)
def resource(request):
    resource_cls, data = request.param
    yield resource_cls, data


@pytest.fixture(scope="session")
def user(suffix, cursor, marked_for_cleanup):
    user = User(name=f"TEST_USER_{suffix}".upper(), owner="ACCOUNTADMIN")
    cursor.execute(user.create_sql())
    marked_for_cleanup.append(user)
    return user


@pytest.fixture(scope="session")
def role(suffix, cursor, marked_for_cleanup):
    role = Role(name=f"TEST_ROLE_{suffix}".upper(), owner="ACCOUNTADMIN")
    cursor.execute(role.create_sql())
    marked_for_cleanup.append(role)
    return role


@pytest.fixture(scope="session")
def noprivs_role(cursor, test_db, marked_for_cleanup):
    role = Role(name="NOPRIVS")
    cursor.execute(role.create_sql(if_not_exists=True))
    cursor.execute(f"GRANT ROLE NOPRIVS TO USER {cursor.connection.user}")
    cursor.execute(f"GRANT USAGE ON DATABASE {test_db} TO ROLE NOPRIVS")
    cursor.execute(f"GRANT USAGE ON SCHEMA {test_db}.PUBLIC TO ROLE NOPRIVS")
    marked_for_cleanup.append(role)
    return role.name


@pytest.mark.requires_snowflake
def test_plan(cursor, user, role):
    session = cursor.connection
    blueprint = Blueprint(name="test")
    role_grant = RoleGrant(role=role, to_user=user)
    blueprint.add(role_grant)
    changes = blueprint.plan(session)
    assert len(changes) == 1
    blueprint.apply(session, changes)
    role_grant_remote = data_provider.fetch_role_grant(session, role_grant.fqn)
    assert role_grant_remote


@pytest.mark.requires_snowflake
def test_blueprint_plan_no_changes(cursor, user, role):
    session = cursor.connection
    blueprint = Blueprint(name="test_no_changes")
    # Assuming role_grant already exists in the setup for this test
    role_grant = RoleGrant(role=role, to_user=user)
    blueprint.add(role_grant)
    # Apply the initial blueprint to ensure the state is as expected
    initial_changes = blueprint.plan(session)
    blueprint.apply(session, initial_changes)
    # Plan again to verify no changes are detected
    subsequent_changes = blueprint.plan(session)
    assert len(subsequent_changes) == 0, "Expected no changes in the blueprint plan but found some."


def test_blueprint_crossreferenced_database(cursor):
    session = cursor.connection
    bp = Blueprint(name="failing-reference")
    schema = Schema(name="MY_SCHEMA", database="some_db")
    bp.add(
        FutureGrant(priv="SELECT", on_future_views_in=schema, to="MY_ROLE"),
        Role(name="MY_ROLE"),
        Database(name="SOME_DB"),
        schema,
    )
    plan = bp.plan(session)
    assert len(plan) == 4


# noprivs_role is causing issues and breaking other integration tests
# @pytest.mark.requires_snowflake
# def test_privilege_scanning(resource, noprivs_role, cursor, marked_for_cleanup):
#     resource_cls, data = resource
#     cursor.execute(f"USE ROLE {noprivs_role}")
#     bp = Blueprint(name="test", allow_role_switching=False)
#     res = resource_cls(**data)
#     bp.add(res)
#     marked_for_cleanup.append(res)
#     with pytest.raises(MissingPrivilegeException):
#         bp.apply(cursor.connection)


@pytest.mark.requires_snowflake
def test_name_equivalence_drift(cursor, suffix, marked_for_cleanup):

    # Create user
    user_name = f"TEST_USER_{suffix}_NAME_EQUIVALENCE".upper()
    user = User(name=user_name, login_name="ALL_UPPERCASE", owner="ACCOUNTADMIN")
    cursor.execute(user.create_sql())
    marked_for_cleanup.append(user)

    session = cursor.connection
    blueprint = Blueprint(name="test_name_equivalence_drift")
    blueprint.add(User(name=user_name, login_name="all_uppercase", owner="ACCOUNTADMIN"))
    plan = blueprint.plan(session)

    assert len(plan) == 0, "Expected no changes in the blueprint plan but found some."


@pytest.mark.requires_snowflake
def test_blueprint_plan_sql(cursor, user):
    session = cursor.connection

    blueprint = Blueprint(name="test_add_database")
    somedb = Database(name="this_database_does_not_exist")
    blueprint.add(somedb)
    plan = blueprint.plan(session)

    assert plan_sql(plan) == [
        "CREATE DATABASE THIS_DATABASE_DOES_NOT_EXIST DATA_RETENTION_TIME_IN_DAYS = 1 MAX_DATA_EXTENSION_TIME_IN_DAYS = 14"
    ]

    blueprint = Blueprint(name="test_modify_user")
    modified_user = User(name=user.name, owner=user.owner, display_name="new_display_name")
    blueprint.add(modified_user)
    plan = blueprint.plan(session)

    assert plan_sql(plan) == [f"ALTER USER {user.name} SET display_name = 'new_display_name'"]


@pytest.mark.requires_snowflake
def test_blueprint_missing_resource_pointer(cursor):
    session = cursor.connection
    grant = Grant.from_sql("GRANT ALL ON WAREHOUSE missing_wh TO ROLE SOMEROLE")
    blueprint = Blueprint(name="blueprint", resources=[grant])
    with pytest.raises(MissingResourceException):
        blueprint.plan(session)


@pytest.mark.requires_snowflake
def test_blueprint_present_resource_pointer(cursor):
    session = cursor.connection
    grant = Grant.from_sql("GRANT AUDIT ON ACCOUNT TO ROLE THISROLEDOESNTEXIST")
    role = Role(name="THISROLEDOESNTEXIST")
    blueprint = Blueprint(name="blueprint", resources=[grant, role])
    plan = blueprint.plan(session)
    assert len(plan) == 2


@pytest.mark.requires_snowflake
def test_blueprint_missing_database(cursor):
    session = cursor.connection
    func = JavascriptUDF(name="func", returns="INT", as_="return 1;", schema="public")
    blueprint = Blueprint(name="blueprint", resources=[func])
    with pytest.raises(Exception):
        blueprint.plan(session)


@pytest.mark.requires_snowflake
def test_blueprint_all_grant_forces_add(cursor, test_db, role):
    cursor.execute(f"GRANT USAGE ON DATABASE {test_db} TO ROLE {role.name}")
    session = cursor.connection
    all_grant = Grant(priv="ALL", on_database=test_db, to=role)
    blueprint = Blueprint(name="blueprint", resources=[all_grant])
    plan = blueprint.plan(session)
    assert len(plan) == 1
    assert plan[0].action == Action.ADD

    # TODO: This test is failing
    # @pytest.mark.requires_snowflake
    # def test_blueprint_fully_managed_dont_remove_information_schema(cursor, test_db):
    #     session = cursor.connection
    #     blueprint = Blueprint(
    #         name="blueprint",
    #         resources=[
    #             Schema(name="INFORMATION_SCHEMA", database=test_db),
    #         ],
    #         run_mode="fully-managed",
    #         valid_resource_types=[ResourceType.SCHEMA],
    #     )
    #     plan = blueprint.plan(session)
    #     assert len(plan) == 0

    #     blueprint = Blueprint(
    #         name="blueprint",
    #         resources=[
    #             Schema(name="ABSENT", database=test_db),
    #             Schema(name="INFORMATION_SCHEMA", database=test_db),
    #         ],
    #         run_mode="fully-managed",
    #         valid_resource_types=[ResourceType.SCHEMA],
    #     )
    #     plan = blueprint.plan(session)
    #     assert len(plan) == 1
    #     assert plan[0].action == Action.ADD
    #     assert plan[0].urn.fqn.name == "ABSENT"
    # cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {test_db}.PRESENT")
    # blueprint = Blueprint(
    #     name="blueprint",
    #     resources=[
    #         Schema(name="PRESENT", database=test_db),
    #         Schema(name="INFORMATION_SCHEMA", database=test_db),
    #     ],
    #     run_mode="fully-managed",
    #     valid_resource_types=[ResourceType.SCHEMA],
    # )
    # plan = blueprint.plan(session)
    # assert len(plan) == 0

    # blueprint = Blueprint(
    #     name="blueprint",
    #     resources=[Schema(name="INFORMATION_SCHEMA", database=test_db)],
    #     run_mode="fully-managed",
    #     valid_resource_types=[ResourceType.SCHEMA],
    # )
    # plan = blueprint.plan(session)
    # assert len(plan) == 1
    # assert plan[0].action == Action.REMOVE
    # assert plan[0].urn.fqn.name == "PRESENT"


@pytest.mark.requires_snowflake
def test_blueprint_quoted_references(cursor):
    session = cursor.connection

    cursor.execute(f"CREATE ROLE IF NOT EXISTS STATIC_ROLE")
    cursor.execute(f'CREATE USER IF NOT EXISTS "info@applytitan.com"')
    cursor.execute(f'GRANT ROLE STATIC_ROLE TO USER "info@applytitan.com"')

    blueprint = Blueprint(
        name="test_quoted_references",
        resources=[RoleGrant(role="STATIC_ROLE", to_user="info@applytitan.com")],
    )
    plan = blueprint.plan(session)

    assert len(plan) == 0
