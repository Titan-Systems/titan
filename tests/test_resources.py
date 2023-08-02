import unittest

from tests.helpers import load_sql_fixtures
from titan.resources import (
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
    NotificationIntegration,
    Pipe,
    ResourceMonitor,
    Role,
    Schema,
    Sequence,
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


class TestResources(unittest.TestCase):
    def validate_from_sql(self, resource_cls, sql):
        resource_cls.from_sql(sql)

    def validate_dict_serde(self, resource_cls, data):
        self.assertEqual(resource_cls(**data).model_dump(mode="json", by_alias=True, exclude_none=True), data)

    def test_resource_composition(self):
        assert Task(name="TASK", schedule="1 minute", as_="SELECT 1", warehouse="wh")
        assert Task(name="TASK", as_="SELECT 1", warehouse=Warehouse(name="wh"))
        assert Task(name="TASK", as_="SELECT 1", warehouse={"name": "wh"})
        assert Task(**{"name": "TASK", "as_": "SELECT 1", "warehouse": {"name": "wh"}})

    def test_alert(self):
        for sql in load_sql_fixtures("alert.sql"):
            self.validate_from_sql(Alert, sql)
        self.validate_dict_serde(
            Alert,
            {
                "name": "ALERT",
                "warehouse": "wh",
                "schedule": "1 minute",
                "condition": "SELECT 1",
                "then": "INSERT INTO foo VALUES(1)",
            },
        )

    def test_api_integration(self):
        for sql in load_sql_fixtures("api_integration.sql"):
            self.validate_from_sql(APIIntegration, sql)

    def test_column(self):
        pass

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

    def test_notification_integration(self):
        for sql in load_sql_fixtures("notification_integration.sql"):
            self.validate_from_sql(NotificationIntegration, sql)

    def test_pipe(self):
        for sql in load_sql_fixtures("pipe.sql"):
            self.validate_from_sql(Pipe, sql)

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
        assert Warehouse(name="WH", warehouse_size="XSMALL").warehouse_size == WarehouseSize.XSMALL

    def test_user(self):
        for sql in load_sql_fixtures("user.sql"):
            self.validate_from_sql(User, sql)
