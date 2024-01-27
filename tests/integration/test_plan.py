import pytest

from titan import Blueprint, User, Resource, Role, RoleGrant, data_provider


@pytest.fixture(scope="session")
def user(suffix, cursor, marked_for_cleanup):
    user = User(name=f"TEST_USER_{suffix}".upper(), owner="ACCOUNTADMIN")
    cursor.execute(user.create_sql())
    marked_for_cleanup.append(user)
    return user


@pytest.fixture(scope="session")
def role(suffix, cursor, marked_for_cleanup: list[Resource]):
    role = Role(name=f"TEST_ROLE_{suffix}".upper(), owner="ACCOUNTADMIN")
    cursor.execute(role.create_sql())
    marked_for_cleanup.append(role)
    return role


@pytest.mark.requires_snowflake
def test_blueprint_plan(cursor, user, role):
    session = cursor.connection
    bp = Blueprint(name="test")
    role_grant = RoleGrant(role=role, to_user=user)
    bp.add(role_grant)
    changes = bp.plan(session)
    assert len(changes) == 1
    bp.apply(session, changes)
    role_grant_remote = data_provider.fetch_role_grant(session, role_grant.fqn)
    assert role_grant_remote
