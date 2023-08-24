import unittest

from titan.blueprint import _plan


class TestPlan(unittest.TestCase):
    def setUp(self):
        self.remote_state = {
            "urn:XYZ123:database/RAW": {
                "name": "RAW",
                "transient": False,
                "owner": "SYSADMIN",
                "data_retention_time_in_days": 1,
                "max_data_extension_time_in_days": 14,
            },
            "urn:XYZ123:database/ANALYTICS": {
                "name": "ANALYTICS",
                "transient": False,
                "owner": "SYSADMIN",
                "data_retention_time_in_days": 1,
                "max_data_extension_time_in_days": 14,
            },
            "urn:XYZ123:warehouse/LOADING": {
                "name": "LOADING",
                "owner": "SYSADMIN",
                "warehouse_type": "STANDARD",
                "auto_suspend": 600,
                "auto_resume": True,
                "max_concurrency_level": 8,
                "statement_queued_timeout_in_seconds": 0,
                "statement_timeout_in_seconds": 172800,
            },
            "urn:XYZ123:warehouse/TRANSFORMING": {
                "name": "TRANSFORMING",
                "owner": "SYSADMIN",
                "warehouse_type": "STANDARD",
                "warehouse_size": "LARGE",
                "auto_suspend": 600,
                "auto_resume": True,
                "max_concurrency_level": 8,
                "statement_queued_timeout_in_seconds": 0,
                "statement_timeout_in_seconds": 172800,
            },
            "urn:XYZ123:warehouse/REPORTING": {
                "name": "REPORTING",
                "owner": "SYSADMIN",
                "warehouse_type": "STANDARD",
                "warehouse_size": "SMALL",
                "auto_suspend": 60,
                "auto_resume": True,
                "max_concurrency_level": 8,
                "statement_queued_timeout_in_seconds": 0,
                "statement_timeout_in_seconds": 300,
            },
            "urn:XYZ123:role/LOADER": {"name": "LOADER", "owner": "SYSADMIN"},
            "urn:XYZ123:role/TRANSFORMER": {"name": "TRANSFORMER", "owner": "SYSADMIN"},
            "urn:XYZ123:role/REPORTER": {"name": "REPORTER", "owner": "SYSADMIN"},
            "urn:XYZ123:user/TEEJ": {
                "name": "TEEJ",
                "owner": "USERADMIN",
                "must_change_password": False,
                "disabled": False,
                "default_role": "REPORTER",
            },
            "urn:XYZ123:role_grant/REPORTER": {
                "to_role": [{"role": "REPORTER", "to_role": "SYSADMIN", "owner": "SYSADMIN"}],
                "to_user": [{"role": "REPORTER", "to_user": "TEEJ", "owner": "SYSADMIN"}],
            },
            "urn:XYZ123:role_grant/LOADER": {
                "to_role": [{"role": "LOADER", "to_role": "SYSADMIN", "owner": "SYSADMIN"}],
                "to_user": [],
            },
            "urn:XYZ123:role_grant/TRANSFORMER": {
                "to_role": [{"role": "TRANSFORMER", "to_role": "SYSADMIN", "owner": "SYSADMIN"}],
                "to_user": [],
            },
        }

        self.manifest = {
            "urn:XYZ123:database/RAW": {
                "name": "RAW",
                "data_retention_time_in_days": 1,
                "transient": False,
                "owner": "SYSADMIN",
                "max_data_extension_time_in_days": 14,
            },
            "urn:XYZ123:warehouse/LOADING": {
                "name": "LOADING",
                "owner": "SYSADMIN",
                "warehouse_type": "STANDARD",
                "warehouse_size": "X-SMALL",
                "auto_suspend": 600,
                "auto_resume": True,
                "max_concurrency_level": 8,
                "statement_queued_timeout_in_seconds": 0,
                "statement_timeout_in_seconds": 172800,
            },
            "urn:XYZ123:warehouse/TRANSFORMING": {
                "name": "TRANSFORMING",
                "owner": "SYSADMIN",
                "warehouse_type": "STANDARD",
                "warehouse_size": "LARGE",
                "auto_suspend": 600,
                "auto_resume": True,
                "max_concurrency_level": 8,
                "statement_queued_timeout_in_seconds": 0,
                "statement_timeout_in_seconds": 99999,
            },
            "urn:XYZ123:warehouse/REPORTING": {
                "name": "REPORTING",
                "owner": "SYSADMIN",
                "warehouse_type": "STANDARD",
                "warehouse_size": "SMALL",
                "auto_suspend": 60,
                "auto_resume": True,
                "max_concurrency_level": 8,
                "statement_queued_timeout_in_seconds": 0,
                "statement_timeout_in_seconds": 172800,
            },
            "urn:XYZ123:role/TRANSFORMER": {"name": "TRANSFORMER", "owner": "SYSADMIN"},
            "urn:XYZ123:user/TEEJ": {
                "name": "TEEJ",
                "login_name": "TEEJ",
                "display_name": "TEEJ",
                "disabled": False,
                "must_change_password": False,
                "default_role": "REPORTER",
                "ext_authn_duo": False,
                "owner": "USERADMIN",
                "has_password": False,
                "has_rsa_public_key": False,
            },
            "urn:XYZ123:role_grant/TRANSFORMER": {
                "to_role": [{"role": "TRANSFORMER", "owner": "SYSADMIN", "to_role": "SYSADMIN"}],
                "to_user": [],
            },
        }

    def test_plan_add_action(self):
        plan = _plan(self.remote_state, self.manifest)
        self.assertIn(("create", "urn:XYZ123:database/ANALYTICS", {...}), plan.changes)

    def test_plan_change_action(self):
        plan = _plan(self.remote_state, self.manifest)
        self.assertIn(
            ("update", "urn:XYZ123:warehouse/REPORTING", {"statement_timeout_in_seconds": 300}),
            plan.changes,
        )

    def test_plan_remove_action(self):
        plan = _plan(self.remote_state, self.manifest)
        self.assertIn(("delete", "urn:XYZ123:role_grant/TRANSFORMER", {...}), plan.changes)

    def test_plan_unexpected_action(self):
        with self.assertRaises(Exception) as context:
            # Here, you'd need to modify either remote_state or manifest or the diff function to return an unexpected action
            plan = _plan(self.remote_state, self.manifest)

        self.assertEqual(str(context.exception), "Unexpected action some_unexpected_action in diff")
