import pytest

from titan import Blueprint, User, Role, RoleGrant, data_provider
from titan.blueprint import MissingPrivilegeException
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
    bp = Blueprint(name="test")
    role_grant = RoleGrant(role=role, to_user=user)
    bp.add(role_grant)
    changes = bp.plan(session)
    assert len(changes) == 1
    bp.apply(session, changes)
    role_grant_remote = data_provider.fetch_role_grant(session, role_grant.fqn)
    assert role_grant_remote


@pytest.mark.requires_snowflake
def test_privilege_scanning(resource, noprivs_role, cursor, marked_for_cleanup):
    resource_cls, data = resource
    cursor.execute(f"USE ROLE {noprivs_role}")
    bp = Blueprint(name="test", allow_role_switching=False)
    res = resource_cls(**data)
    bp.add(res)
    marked_for_cleanup.append(res)
    with pytest.raises(MissingPrivilegeException):
        bp.apply(cursor.connection)
