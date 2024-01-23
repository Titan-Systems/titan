import os
import uuid

import pytest

from titan import Blueprint, User, Resource, Role, RoleGrant
from titan.client import get_session


@pytest.fixture(scope="session")
def suffix():
    return str(uuid.uuid4())[:8].upper()


@pytest.fixture(scope="session")
def marked_for_cleanup():
    """List to keep track of resources created during tests."""
    return []


@pytest.fixture(scope="session")
def cursor(suffix, marked_for_cleanup: list[Resource]):
    session = get_session()
    with session.cursor() as cur:
        cur.execute(f"ALTER SESSION set query_tag='titan_package:test::{suffix}'")
        cur.execute("USE ROLE ACCOUNTADMIN")
        yield cur
        cur.execute("USE ROLE ACCOUNTADMIN")
        for res in marked_for_cleanup:
            cur.execute(res.drop_sql(if_exists=True))


@pytest.fixture(scope="session")
def user(suffix, cursor, marked_for_cleanup: list[Resource]):
    user = User(name=f"TEST_USER_{suffix}", owner="ACCOUNTADMIN")
    cursor.execute(user.create_sql())
    marked_for_cleanup.append(user)
    return user


@pytest.fixture(scope="session")
def role(suffix, cursor, marked_for_cleanup: list[Resource]):
    role = Role(name=f"TEST_ROLE_{suffix}", owner="ACCOUNTADMIN")
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


# @pytest.mark.requires_snowflake
# def test_role_permissions(cursor, user, role):
#     session = cursor.connection
#     user_grant = RoleGrant(role=role, to_user=user, owner="ACCOUNTADMIN")
#     sysadmin_grant = RoleGrant(role=role, to_role="SYSADMIN", owner="ACCOUNTADMIN")

#     bp = Blueprint(name="test", account=os.environ["SNOWFLAKE_ACCOUNT"])
#     bp.add(
#         role,
#         user_grant,
#         sysadmin_grant,
#     )
#     changes = bp.plan(session)
#     assert len(changes) == 2
#     bp.apply(session, changes)
#     drift = bp.plan(session)
#     assert len(drift) == 0
