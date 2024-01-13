import unittest

from titan import (
    Blueprint,
    Database,
    PythonUDF,
    Schema,
    Table,
    View,
)


class TestBlueprint(unittest.TestCase):
    def test_blueprint_with_resources(self):
        self.maxDiff = None

        db = Database(name="DB")
        # FIXME: database=db is not setting a ref from schema to db, causing schema to not get added to the manifest
        schema = Schema(name="SCHEMA", database=db)
        table = Table(name="TABLE", columns=[{"name": "ID", "data_type": "INT"}])
        schema.add(table)
        view = View(name="VIEW", schema=schema, as_="SELECT 1")
        udf = PythonUDF(
            name="SOMEUDF",
            returns="VARCHAR",
            runtime_version="3.9",
            handler="main",
            comment="This is a UDF comment",
        )
        blueprint = Blueprint(name="blueprint", resources=[db, table, schema, view, udf])
        manifest = blueprint.generate_manifest({"account": "SOMEACCT", "account_locator": "ABCD123"})

        self.assertIn("urn::ABCD123:database/DB", manifest)
        self.assertDictEqual(
            manifest["urn::ABCD123:database/DB"],
            {
                "name": "DB",
                "owner": "SYSADMIN",
            },
        )

        self.assertIn("urn::ABCD123:schema/DB.SCHEMA", manifest)
        self.assertDictEqual(
            manifest["urn::ABCD123:schema/DB.SCHEMA"],
            {
                "name": "SCHEMA",
                "owner": "SYSADMIN",
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
            },
        )
        self.assertIn("urn::ABCD123:function/DB.PUBLIC.SOMEUDF", manifest)
        self.assertDictEqual(
            manifest["urn::ABCD123:function/DB.PUBLIC.SOMEUDF"],
            {
                "name": "SOMEUDF",
                "owner": "SYSADMIN",
                "returns": "VARCHAR",
                "handler": "main",
                "runtime_version": "3.9",
                "comment": "This is a UDF comment",
            },
        )
