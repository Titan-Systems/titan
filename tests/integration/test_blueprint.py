import pytest

from titan import Blueprint, Database, Grant, User, Role, RoleGrant, data_provider
from titan.diff import DiffAction
from titan.blueprint import plan_sql
from tests.helpers import get_json_fixtures

JSON_FIXTURES = list(get_json_fixtures())


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
    somedb = Database(name="somedb")
    blueprint.add(somedb)
    plan = blueprint.plan(session)

    assert plan_sql(plan) == [
        "CREATE DATABASE SOMEDB DATA_RETENTION_TIME_IN_DAYS = 1 MAX_DATA_EXTENSION_TIME_IN_DAYS = 14"
    ]

    blueprint = Blueprint(name="test_modify_user")
    modified_user = User(name=user.name, owner=user.owner, display_name="new_display_name")
    blueprint.add(modified_user)
    plan = blueprint.plan(session)

    assert plan_sql(plan) == [f"ALTER USER {user.name} SET display_name = 'new_display_name'"]


@pytest.mark.requires_snowflake
def test_blueprint_with_string_grants(cursor):
    session = cursor.connection
    blueprint = Blueprint()
    blueprint.add(
        Grant.from_sql("GRANT ALL ON DATABASE titan_db_test TO ROLE titan_app_admin_role_test"),
        Grant.from_sql("GRANT ALL ON SCHEMA titan_db_test.titan_app_test TO ROLE titan_app_admin_role_test"),
        Grant.from_sql("GRANT ALL ON WAREHOUSE titan_app_warehouse_test TO ROLE titan_app_admin_role_test"),
        Grant.from_sql("GRANT USAGE ON COMPUTE POOL titan_app_compute_pool_test TO ROLE titan_app_admin_role_test"),
        Grant.from_sql("GRANT MONITOR ON COMPUTE POOL titan_app_compute_pool_test TO ROLE titan_app_admin_role_test"),
        Grant.from_sql("GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO ROLE titan_app_admin_role_test"),
    )
    plan = blueprint.plan(session)
    assert len(plan) == 6
    assert all(change.action == DiffAction.ADD for change in plan)
    assert all("on_type" in change.after for change in plan)
