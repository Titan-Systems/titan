import unittest

from pydantic import ValidationError

from titan.resources import RoleGrant


class TestGrants(unittest.TestCase):
    def test_role_grants(self):
        py = RoleGrant(role="ANALYZER", to_user="someuser")
        sql = RoleGrant.from_sql("GRANT ROLE ANALYZER TO USER someuser")
        self.assertDictEqual(py.model_dump(), sql.model_dump())
