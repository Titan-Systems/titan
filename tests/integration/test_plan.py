import os
import unittest

from random import randint

from titan import Blueprint, User, Role, RoleGrant, DataProvider
from titan.client import get_session
from titan.identifiers import URN


class TestPlan(unittest.TestCase):
    def setUp(self):
        self.seed = randint(100000, 1000000)
        self.user = User(name=f"TEST_USER_{self.seed}")
        self.session = get_session()
        self.user_urn = URN.from_resource(account=self.session.account, resource=self.user)
        provider = DataProvider(self.session)
        provider.create_resource(self.user_urn, self.user.model_dump(exclude_none=True))

    def test_role_permissions(self):
        role = Role(name=f"TEST_ROLE_{self.seed}")
        user_grant = RoleGrant(role=role, to_user=self.user)
        sysadmin_grant = RoleGrant(role=role, to_role="SYSADMIN")

        bp = Blueprint(name=f"test-{self.seed}", account=os.environ["SNOWFLAKE_ACCOUNT"])
        bp.add(
            role,
            user_grant,
            sysadmin_grant,
        )
        plan = bp.plan(self.session)
        self.assertEqual(len(plan.changes), 2)
        bp.apply(self.session, plan)
        drift = bp.plan(self.session)
        self.assertEqual(len(drift.changes), 0)
        bp.destroy(self.session)

    def tearDown(self):
        provider = DataProvider(self.session)
        provider.drop_resource(self.user_urn, self.user.model_dump(exclude_none=True))
