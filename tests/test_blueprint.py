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
                "comment": None,
                "data_retention_time_in_days": 1,
                "default_ddl_collation": None,
                "max_data_extension_time_in_days": 14,
                "tags": None,
                "transient": False,
            },
        )

        self.assertIn("urn::ABCD123:schema/DB.SCHEMA", manifest)
        self.assertDictEqual(
            manifest["urn::ABCD123:schema/DB.SCHEMA"],
            {
                "comment": None,
                "data_retention_time_in_days": None,
                "default_ddl_collation": None,
                "managed_access": False,
                "max_data_extension_time_in_days": 14,
                "name": "SCHEMA",
                "owner": "SYSADMIN",
                "tags": None,
                "transient": False,
            },
        )
        self.assertIn("urn::ABCD123:view/DB.SCHEMA.VIEW", manifest)
        self.assertDictEqual(
            manifest["urn::ABCD123:view/DB.SCHEMA.VIEW"],
            {
                "as_": "SELECT 1",
                "change_tracking": None,
                "columns": None,
                "comment": None,
                "copy_grants": None,
                "name": "VIEW",
                "owner": "SYSADMIN",
                "recursive": None,
                "secure": None,
                "tags": None,
                "volatile": None,
            },
        )
        self.assertIn("urn::ABCD123:table/DB.SCHEMA.TABLE", manifest)
        self.assertDictEqual(
            manifest["urn::ABCD123:table/DB.SCHEMA.TABLE"],
            {
                "name": "TABLE",
                "owner": "SYSADMIN",
                "columns": [{"name": "ID", "data_type": "INT"}],
                "constraints": None,
                "volatile": False,
                "transient": False,
                "cluster_by": None,
                "enable_schema_evolution": False,
                "data_retention_time_in_days": None,
                "max_data_extension_time_in_days": None,
                "change_tracking": False,
                "default_ddl_collation": None,
                "copy_grants": False,
                "row_access_policy": None,
                "tags": None,
                "comment": None,
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
                "args": None,
                "as_": None,
                "copy_grants": False,
                "language": "PYTHON",
                "external_access_integrations": None,
                "imports": None,
                "null_handling": None,
                "packages": None,
                "secrets": None,
                "secure": None,
                "volatility": None,
            },
        )
