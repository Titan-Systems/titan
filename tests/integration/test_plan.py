import os
import uuid

import pytest

from titan import Blueprint, User, Role, RoleGrant
from titan.client import get_session


@pytest.fixture(scope="session")
def seed():
    yield str(uuid.uuid4())[:8]


@pytest.fixture(scope="module")
def module_setup():
    session = get_session()
    seed = str(uuid.uuid4())[:8]
    user = User(name=f"TEST_USER_{seed}")
    role = Role(name=f"TEST_ROLE_{seed}")
    session.execute_string(f"CREATE USER {user.name}")
    yield user, role
    session.execute_string(f"DROP USER {user.name}")
    session.execute_string(f"DROP ROLE IF EXISTS {role.name}")


def test_role_permissions(module_setup):
    user, role = module_setup
    user_grant = RoleGrant(role=role, to_user=user)
    sysadmin_grant = RoleGrant(role=role, to_role="SYSADMIN")

    bp = Blueprint(name="test", account=os.environ["SNOWFLAKE_ACCOUNT"])
    bp.add(
        role,
        user_grant,
        sysadmin_grant,
    )
    session = get_session()
    plan = bp.plan(session)
    assert len(plan) == 2
    bp.apply(session, plan)
    drift = bp.plan(session)
    assert len(drift) == 0
    bp.destroy(session)
    plan = bp.plan(session)
    assert len(plan) == 2
