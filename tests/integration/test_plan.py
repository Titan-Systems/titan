import pytest

from titan import Blueprint, User, Resource, Role, RoleGrant


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


@pytest.mark.requires_snowflake
def test_role_permissions(cursor, user, role):
    session = cursor.connection
    user_grant = RoleGrant(role=role, to_user=user)
    sysadmin_grant = RoleGrant(role=role, to_role="SYSADMIN")

    bp = Blueprint(name="test")
    bp.add(
        role,
        user_grant,
        sysadmin_grant,
    )
    changes = bp.plan(session)
    assert len(changes) == 2
