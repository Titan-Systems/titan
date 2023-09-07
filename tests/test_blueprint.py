import unittest

from titan import Blueprint, Database, Schema, Table, View


class TestBlueprint(unittest.TestCase):
    def test_blueprint_with_resources(self):
        self.maxDiff = None

        db = Database(name="DB")
        schema = Schema(name="SCHEMA", database=db)
        table = Table(name="TABLE", columns=["id INT"])
        table.schema = schema
        view = View(name="VIEW", schema_=schema, as_="SELECT 1")
        blueprint = Blueprint(name="blueprint", account="ABCD123", resources=[db, table, schema, view])
        manifest = blueprint.generate_manifest()

        self.assertIn("urn::ABCD123:database/DB", manifest)
        self.assertDictEqual(
            manifest["urn::ABCD123:database/DB"],
            {
                "name": "DB",
                "owner": "SYSADMIN",
                "transient": False,
                "data_retention_time_in_days": 1,
                "max_data_extension_time_in_days": 14,
            },
        )

        self.assertIn("urn::ABCD123:schema/DB.SCHEMA", manifest)
        self.assertDictEqual(
            manifest["urn::ABCD123:schema/DB.SCHEMA"],
            {
                "name": "SCHEMA",
                "owner": "SYSADMIN",
                "transient": False,
            },
        )
        self.assertIn("urn::ABCD123:view/DB.SCHEMA.VIEW", manifest)
        self.assertDictEqual(
            manifest["urn::ABCD123:view/DB.SCHEMA.VIEW"],
            {"name": "VIEW", "owner": "SYSADMIN", "as_": "SELECT 1"},
        )
        self.assertIn("urn::ABCD123:table/DB.SCHEMA.TABLE", manifest)
        self.assertDictEqual(
            manifest["urn::ABCD123:table/DB.SCHEMA.TABLE"],
            {
                "name": "TABLE",
                "owner": "SYSADMIN",
                "columns": [{"name": "ID", "data_type": "INT"}],
                "volatile": False,
                "transient": False,
                "cluster_by": [],
                "enable_schema_evolution": False,
                "change_tracking": False,
                "copy_grants": False,
            },
        )
