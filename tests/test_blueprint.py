import unittest

from titan import Blueprint, Database, Schema, Table, View


class TestBlueprint(unittest.TestCase):
    def test_blueprint_with_resources(self):
        self.maxDiff = None

        db = Database(name="DB")
        schema = Schema(name="SCHEMA", database=db)
        table = Table(name="TABLE", schema=schema)
        view = View(name="VIEW", schema=schema)
        blueprint = Blueprint(name="blueprint", account="ABCD123", resources=[db, schema, table, view])

        self.assertEqual(
            blueprint.manifest,
            {
                "urn:ABCD123:database/DB": {
                    "name": "DB",
                    "data_retention_time_in_days": 1,
                    "max_data_extension_time_in_days": 14,
                    "owner": "SYSADMIN",
                    "transient": False,
                },
                "urn:ABCD123:schema/DB.SCHEMA": {
                    "name": "SCHEMA",
                    "transient": False,
                    "with_managed_access": False,
                },
                "urn:ABCD123:table/DB.SCHEMA.TABLE": {
                    "name": "TABLE",
                    "columns": [],
                    "volatile": False,
                    "transient": False,
                    "cluster_by": [],
                    "enable_schema_evolution": False,
                    "change_tracking": False,
                    "copy_grants": False,
                },
                "urn:ABCD123:view/DB.SCHEMA.VIEW": {
                    "name": "VIEW",
                    "columns": [],
                    "volatile": False,
                    "recursive": False,
                    "copy_grants": False,
                },
            },
        )

        # provider = Provider()
        # state = provider.fetch_from_manifest(blueprint.manifest)

        # # Calculate the diff
        # diff = blueprint.compare(state)

        # # Now, make assertions based on the diff.
        # # For example, assuming diff is a dictionary with resource names as keys:
        # assert "res1" in diff
        # assert "res2" in diff

        # Or, you can make specific assertions based on the diff's content:
        # assert diff['res1'] == { ... expected difference ... }
        # assert diff['res2'] == { ... expected difference ... }

        # Add more assertions as needed, based on your diff calculation logic.
