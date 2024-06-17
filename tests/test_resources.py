import pytest

from titan import resources as res
from titan.resource_name import ResourceName
from titan.resource_tags import ResourceTags
from titan.enums import WarehouseSize
from tests.helpers import get_sql_fixtures, get_json_fixtures


def test_resource_init_with_dict_pointer():
    res.Task(**{"name": "TASK", "as_": "SELECT 1", "warehouse": {"name": "wh"}})


def test_resource_init_with_resource_pointer():
    res.Task(name="TASK", schedule="1 minute", as_="SELECT 1", warehouse=res.Warehouse(name="wh"))


def test_resource_init_with_resource_name():
    res.Task(name="TASK", schedule="1 minute", as_="SELECT 1", warehouse="wh")


def test_resource_init_with_type():
    res.Task(**{"name": "TASK", "as_": "SELECT 1", "warehouse": {"name": "wh"}, "resource_type": "TASK"})


def test_resource_init_from_dict():
    res.Resource.from_dict({"name": "TASK", "as_": "SELECT 1", "warehouse": {"name": "wh"}, "resource_type": "TASK"})


def test_view_fails_with_empty_columns():
    with pytest.raises(ValueError):
        res.View(name="MY_VIEW", columns=[], as_="SELECT 1")


def test_view_with_columns():
    view = res.View.from_sql("CREATE VIEW MY_VIEW (COL1) AS SELECT 1")
    assert view._data.columns == [{"name": "COL1"}]


def test_enum_field_serialization():
    assert res.Warehouse(name="WH", warehouse_size="XSMALL")._data.warehouse_size == WarehouseSize.XSMALL


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


def test_resource_name_serialization():
    task = res.Task(name="TASK")
    assert task.name == "TASK"
    assert task.name == ResourceName("task")
    assert task.to_dict()["name"] == "TASK"
    assert task.fqn.name == "TASK"

    task = res.Task(name="~task")
    assert task.name == "~task"
    assert task.name == ResourceName('"~task"')
    assert task.to_dict()["name"] == "~task"
    assert task.fqn.name == "~task"


def test_tags_definition():
    db = res.Database(name="DB", tags={"project": "test_deployment", "priority": "low"})
    assert db._data.tags is not None
    assert db._data.tags.to_dict() == {"project": "test_deployment", "priority": "low"}

    db = res.Database(name="DB", tags=ResourceTags({"project": "test_deployment", "priority": "low"}))
    assert db._data.tags is not None
    assert db._data.tags.to_dict() == {"project": "test_deployment", "priority": "low"}
