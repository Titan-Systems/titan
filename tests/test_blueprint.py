import unittest

from titan import Blueprint, Database, Schema, Table, View, Adapter


class TestBlueprint(unittest.TestCase):
    def test_blueprint_with_resources(self):
        self.maxDiff = None

        db = Database(name="DB")
        schema = Schema(name="SCHEMA", database=db)
        # table = Table(name="TABLE", schema=schema)
        view = View(name="VIEW", schema=schema)
        blueprint = Blueprint(name="blueprint", account="ABCD123", resources=[db, schema, view])
        manifest = blueprint.generate_manifest()

        blueprint.plan(Adapter())

        self.assertIn("urn:ABCD123:database/DB", manifest)
        self.assertEqual(
            manifest["urn:ABCD123:database/DB"],
            {
                "name": "DB",
                "owner": "SYSADMIN",
                "transient": False,
                "data_retention_time_in_days": 1,
                "max_data_extension_time_in_days": 14,
            },
        )

        self.assertIn("urn:ABCD123:schema/DB.SCHEMA", manifest)
        self.assertEqual(
            manifest["urn:ABCD123:schema/DB.SCHEMA"],
            {
                "name": "SCHEMA",
                "owner": "SYSADMIN",
                "transient": False,
            },
        )
        self.assertIn("urn:ABCD123:view/DB.SCHEMA.VIEW", manifest)
        self.assertEqual(
            manifest["urn:ABCD123:view/DB.SCHEMA.VIEW"],
            {
                "name": "VIEW",
                "owner": "SYSADMIN",
                "secure": False,
                "volatile": False,
                "recursive": False,
                "columns": [],
            },
        )
        # self.assertIn("urn:ABCD123:table/DB.SCHEMA.TABLE", manifest)
        #         ,
        #         "urn:ABCD123:table/DB.SCHEMA.TABLE": {
        #             "name": "TABLE",
        #             "columns": [],
        #             "volatile": False,
        #             "transient": False,
        #             "cluster_by": [],
        #             "enable_schema_evolution": False,
        #             "change_tracking": False,
        #             "copy_grants": False,
        #         },

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
