import pytest

from titan import resources
from tests.helpers import get_sql_fixtures, get_json_fixtures
from titan.enums import WarehouseSize


# def test_resource_init_from_sql():
#     res = resources.Resource.from_sql("CREATE TASK MY_TASK AS SELECT 1")
#     assert res.resource_type == resources.ResourceType.TASK


def test_resource_init_with_dict_pointer():
    resources.Task(**{"name": "TASK", "as_": "SELECT 1", "warehouse": {"name": "wh"}})


def test_resource_init_with_resource_pointer():
    resources.Task(name="TASK", schedule="1 minute", as_="SELECT 1", warehouse=resources.Warehouse(name="wh"))


def test_resource_init_with_resource_name():
    resources.Task(name="TASK", schedule="1 minute", as_="SELECT 1", warehouse="wh")


def test_resource_init_with_type():
    resources.Task(**{"name": "TASK", "as_": "SELECT 1", "warehouse": {"name": "wh"}, "resource_type": "TASK"})


def test_resource_init_from_dict():
    resources.Resource.from_dict(
        {"name": "TASK", "as_": "SELECT 1", "warehouse": {"name": "wh"}, "resource_type": "TASK"}
    )


def test_view_fails_with_empty_columns():
    with pytest.raises(ValueError):
        resources.View(name="MY_VIEW", columns=[], as_="SELECT 1")


def test_view_with_columns():
    view = resources.View.from_sql("CREATE VIEW MY_VIEW (COL1) AS SELECT 1")
    assert view._data.columns == [{"name": "COL1"}]


def test_enum_field_serialization():
    assert resources.Warehouse(name="WH", warehouse_size="XSMALL")._data.warehouse_size == WarehouseSize.XSMALL


SQL_FIXTURES = list(get_sql_fixtures())


@pytest.fixture(
    params=SQL_FIXTURES,
    ids=[f"{resource_cls.__name__}({idx})" for resource_cls, _, idx in SQL_FIXTURES],
    scope="function",
)
def sql_fixture(request):
    resource_cls, data, idx = request.param
    yield resource_cls, data


def test_init_from_sql(sql_fixture):
    resource_cls, data = sql_fixture
    try:
        resource_cls.from_sql(data)
    except Exception:
        pytest.fail(f"Failed to construct {resource_cls.__name__} from SQL fixture")
