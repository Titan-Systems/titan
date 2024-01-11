import unittest

from pyparsing import ParseException

from tests.helpers import load_sql_fixtures
from titan.resources import (
    Account,
    Alert,
    APIIntegration,
    Column,
    Database,
    DatabaseRole,
    DynamicTable,
    ExternalFunction,
    FailoverGroup,
    FileFormat,
    Grant,
    JavascriptUDF,
    NotificationIntegration,
    Pipe,
    PythonStoredProcedure,
    ResourceMonitor,
    Role,
    Schema,
    Sequence,
    SharedDatabase,
    Stage,
    StorageIntegration,
    Stream,
    Table,
    Tag,
    Task,
    User,
    View,
    Warehouse,
)
from titan.resources.warehouse import WarehouseSize


class TestResourceModel(unittest.TestCase):
    def test_resource_constructors(self):
        self.assertIsNotNone(Task(name="TASK", schedule="1 minute", as_="SELECT 1", warehouse="wh"))
        self.assertIsNotNone(Task(name="TASK", as_="SELECT 1", warehouse=Warehouse(name="wh")))
        self.assertIsNotNone(Task(name="TASK", as_="SELECT 1", warehouse={"name": "wh"}))
        self.assertIsNotNone(Task(**{"name": "TASK", "as_": "SELECT 1", "warehouse": {"name": "wh"}}))


class TestResources(unittest.TestCase):
    def test_view_fails_with_empty_columns(self):
        self.assertRaises(ValueError, View, name="MY_VIEW", columns=[], as_="SELECT 1")

    def test_view_with_columns(self):
        view = View.from_sql("CREATE VIEW MY_VIEW (COL1) AS SELECT 1")
        assert view._data.columns == [{"name": "COL1"}]

    def test_enum_field_serialization(self):
        self.assertEqual(Warehouse(name="WH", warehouse_size="XSMALL")._data.warehouse_size, WarehouseSize.XSMALL)


class TestResourceFixtures(unittest.TestCase):
    def validate_from_sql(self, resource_cls, sql):
        try:
            resource_cls.from_sql(sql)
        except ParseException:
            self.fail(f"Failed to parse {resource_cls.__name__} from SQL: {sql}")

    # def test_account(self):
    #     for sql in load_sql_fixtures("account.sql"):
    #         self.validate_from_sql(Account, sql)

    def test_alert(self):
        for sql in load_sql_fixtures("alert.sql"):
            self.validate_from_sql(Alert, sql)

    def test_api_integration(self):
        for sql in load_sql_fixtures("api_integration.sql"):
            self.validate_from_sql(APIIntegration, sql)

    def test_column(self):
        for sql in load_sql_fixtures("column.sql", lines=True):
            self.validate_from_sql(Column, sql)

    def test_database(self):
        for sql in load_sql_fixtures("database.sql"):
            self.validate_from_sql(Database, sql)

    def test_database_role(self):
        for sql in load_sql_fixtures("database_role.sql"):
            self.validate_from_sql(DatabaseRole, sql)

    def test_dynamic_table(self):
        for sql in load_sql_fixtures("dynamic_table.sql"):
            self.validate_from_sql(DynamicTable, sql)

    def test_external_function(self):
        for sql in load_sql_fixtures("external_function.sql"):
            self.validate_from_sql(ExternalFunction, sql)

    def test_failover_group(self):
        for sql in load_sql_fixtures("failover_group.sql"):
            self.validate_from_sql(FailoverGroup, sql)

    def test_file_format(self):
        for sql in load_sql_fixtures("file_format.sql"):
            self.validate_from_sql(FileFormat, sql)

    def test_grant(self):
        for sql in load_sql_fixtures("grant.sql"):
            self.validate_from_sql(Grant, sql)

    def test_javascript_udf(self):
        for sql in load_sql_fixtures("javascript_udf.sql"):
            self.validate_from_sql(JavascriptUDF, sql)

    def test_notification_integration(self):
        for sql in load_sql_fixtures("notification_integration.sql"):
            self.validate_from_sql(NotificationIntegration, sql)

    def test_pipe(self):
        for sql in load_sql_fixtures("pipe.sql"):
            self.validate_from_sql(Pipe, sql)

    def test_stored_procedure(self):
        for sql in load_sql_fixtures("stored_procedure.sql"):
            self.validate_from_sql(PythonStoredProcedure, sql)

    def test_resource_monitor(self):
        for sql in load_sql_fixtures("resource_monitor.sql"):
            self.validate_from_sql(ResourceMonitor, sql)

    def test_role(self):
        for sql in load_sql_fixtures("role.sql"):
            self.validate_from_sql(Role, sql)

    def test_schema(self):
        for sql in load_sql_fixtures("schema.sql"):
            self.validate_from_sql(Schema, sql)

    def test_sequence(self):
        for sql in load_sql_fixtures("sequence.sql"):
            self.validate_from_sql(Sequence, sql)

    def test_shared_database(self):
        for sql in load_sql_fixtures("share.sql"):
            self.validate_from_sql(SharedDatabase, sql)

    def test_stage(self):
        for sql in load_sql_fixtures("stage.sql"):
            self.validate_from_sql(Stage, sql)

    def test_storage_integration(self):
        for sql in load_sql_fixtures("storage_integration.sql"):
            self.validate_from_sql(StorageIntegration, sql)

    def test_stream(self):
        for sql in load_sql_fixtures("stream.sql"):
            self.validate_from_sql(Stream, sql)

    def test_table(self):
        for sql in load_sql_fixtures("table.sql"):
            self.validate_from_sql(Table, sql)

    def test_tag(self):
        for sql in load_sql_fixtures("tag.sql"):
            self.validate_from_sql(Tag, sql)

    def test_task(self):
        for sql in load_sql_fixtures("task.sql"):
            self.validate_from_sql(Task, sql)

    def test_view(self):
        for sql in load_sql_fixtures("view.sql"):
            self.validate_from_sql(View, sql)

    def test_warehouse(self):
        for sql in load_sql_fixtures("warehouse.sql"):
            self.validate_from_sql(Warehouse, sql)

    def test_user(self):
        for sql in load_sql_fixtures("user.sql"):
            self.validate_from_sql(User, sql)
