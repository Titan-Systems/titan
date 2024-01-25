import os
import pytest

from pyparsing import ParseException
from tests.helpers import load_sql_fixtures, get_json_fixtures, list_sql_fixtures
from titan import resources
from titan.enums import ResourceType
from titan.resources.warehouse import WarehouseSize
from titan.resources.resource import Resource


def test_resource_constructors():
    try:
        resources.Task(name="TASK", schedule="1 minute", as_="SELECT 1", warehouse="wh")
        resources.Task(name="TASK", as_="SELECT 1", warehouse=resources.Warehouse(name="wh"))
        resources.Task(name="TASK", as_="SELECT 1", warehouse={"name": "wh"})
        resources.Task(**{"name": "TASK", "as_": "SELECT 1", "warehouse": {"name": "wh"}})
    except Exception:
        pytest.fail("Resource constructor raised an exception unexpectedly!")


def test_view_fails_with_empty_columns():
    with pytest.raises(ValueError):
        resources.View(name="MY_VIEW", columns=[], as_="SELECT 1")


def test_view_with_columns():
    view = resources.View.from_sql("CREATE VIEW MY_VIEW (COL1) AS SELECT 1")
    assert view._data.columns == [{"name": "COL1"}]


def test_enum_field_serialization():
    assert resources.Warehouse(name="WH", warehouse_size="XSMALL")._data.warehouse_size == WarehouseSize.XSMALL


class TestResourceFixtures:
    @staticmethod
    def validate_from_sql(resource_cls, sql):
        try:
            resource_cls.from_sql(sql)
        except ParseException:
            pytest.fail(f"Failed to parse {resource_cls.__name__} from SQL: {sql}")

    # def test_sql_fixtures(self, sql_fixture_file, resource_cls):
    #     for sql in load_sql_fixtures(sql_fixture_file):
    #         self.validate_from_sql(resource_cls, sql)

    def test_json(self, resource_cls, json_fixture):
        try:
            resource_cls(**json_fixture)
        except Exception:
            pytest.fail(f"Failed to construct {resource_cls.__name__} from JSON fixture")


def pytest_generate_tests(metafunc):
    # if "sql_fixture_file" and "resource_cls" in metafunc.fixturenames:
    #     params = []
    #     for f in list_sql_fixtures():
    #         resource_name = f.split(".")[0]
    #         try:
    #             resource_cls = Resource.resolve_resource_cls(ResourceType(resource_name.replace("_", " ")))
    #         except ValueError:
    #             continue
    #         params.append((f, resource_cls))
    #     ids = [resource_name for _, resource_name in params]
    #     metafunc.parametrize("sql_fixture_file, resource_cls", params, ids=ids)
    if "resource_cls" and "json_fixture" in metafunc.fixturenames:
        params = []
        for resource_cls, fixture in get_json_fixtures():
            params.append((resource_cls, fixture))
        ids = [resource_cls.__name__ for resource_cls, _ in params]
        metafunc.parametrize("resource_cls, json_fixture", params, ids=ids)
