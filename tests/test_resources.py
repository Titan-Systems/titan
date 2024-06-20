import pytest

from titan import resources as res
from tests.helpers import get_sql_fixtures
from titan.enums import ResourceType, WarehouseSize
from titan.resource_name import ResourceName
from titan.resource_tags import ResourceTags
from titan.resources.resource import ResourcePointer

SQL_FIXTURES = list(get_sql_fixtures())


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
    warehouse = res.Warehouse(name="WH", warehouse_size="XSMALL")
    assert warehouse._data.warehouse_size == WarehouseSize.XSMALL


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


def test_database_scoped_container_construction():
    db = res.Database(name="my_database")
    schema = res.Schema(name="my_schema", database=db)
    assert schema.container is not None
    assert schema.container.name == "my_database"

    schema = res.Schema(name="my_database.my_schema")
    assert schema.container is not None
    assert schema.container.name == "my_database"


def test_schema_scoped_container_construction():
    db = res.Database(name="my_database")
    schema = res.Schema(name="my_schema", database=db)
    tbl = res.Table(
        name="my_table",
        schema=schema,
        columns=[{"name": "col1", "data_type": "VARCHAR(10)"}],
    )
    assert tbl.container is not None
    assert tbl.container.name == "my_schema"
    assert tbl.container.container is not None
    assert tbl.container.container.name == "my_database"

    tbl = res.Table(
        name="my_table",
        database="my_database",
        schema="my_schema",
        columns=[{"name": "col1", "data_type": "VARCHAR(10)"}],
    )
    assert tbl.container is not None
    assert tbl.container.name == "my_schema"
    assert tbl.container.container is not None
    assert tbl.container.container.name == "my_database"

    tbl = res.Table(
        name="my_table",
        database="my_database",
        columns=[{"name": "col1", "data_type": "VARCHAR(10)"}],
    )
    assert tbl.container is not None
    assert tbl.container.name == "PUBLIC"
    assert tbl.container.container is not None
    assert tbl.container.container.name == "my_database"

    tbl = res.Table(
        name="my_database.my_schema.my_table",
        columns=[{"name": "col1", "data_type": "VARCHAR(10)"}],
    )
    assert tbl.name == "my_table"
    assert tbl.container is not None
    assert tbl.container.name == "my_schema"
    assert tbl.container.container is not None
    assert tbl.container.container.name == "my_database"


def test_resource_with_named_nested_dependency():
    """
    TL;DR

    When we have a string with a fully qualified name in it (eg "db.sch.some_thing") and we
    want to pass that into a Resource init, that name needs to eventually be serialized out the
    exact same way.

    ----------------------------------------------------------------

    What happens when we pass in a fully qualified name string into a field that
    represents a different resource?

    In this case, we have an ExternalAccessIntegration. As input it takes a list of
    NetworkRules. Titan tries to support as many common-sense compositions of this input
    as possible.

    1. Pass in a NetworkRules resource object
    2. Pass in a string, representing the name of a resource

    In the first case, the ExternalAccessIntegration keeps a reference to the
    NetworkRules objects that were passed in during init.

    In the second case, the ExternalAccessIntegration creates a ResourcePointer
    from the string and resource type information, see resource.py : convert_to_resource()
    for more.

    When a resource is serialized into data, we need to make a decision on how each field
    should be serialized. This example represents the default behavior and the majority of
    cases: we want to serialize this reference or pointer into a fully qualified name.

    Unfortunately for titan, we're not a database. In Snowflake, name resolution always happens
    in the context of a session, where any name, qualified or not, can be looked up using standard
    SQL name resolution. Specifically, if a resource name doesn't specify a database or a schema,
    it is looked up in the user's search PATH and the session's current database and schema.

    Titan serializes resources far before a session is initiated, so we don't have that luxury.

    Why can't we just keep the string? Titan automatically managed implied references. Titan
    needs to know that this ExternalAccessIntegration relies on a NetworkRules resource.

    So what should happen here:

    1. ExternalAccessIntegration.__init__() is called with
        allowed_network_rules = ["db.sch.some_network_rule"]

    2. ResourceNameTrait __init__() is called, we can ignore this

    3. Resource __init__() is called, we can ignore this

    4. The spec class _ExternalAccessIntegration __init__ is called

    5. The ResourceSpec __post_init__ method is called. This is where
        the incoming value (string or Resource object) is coerced into
        something else.

    6a. If the value is a Resource object, no coercion, we keep the value as-is and return
    6b. If the value is a string, a ResourcePointer is created

    7. ResourcePointer __init__ is called

    8. ResourceNameTrait __init__() is called for the pointer. This should parse
        the fully qualified name and add database/schema kwargs

    9. Resource __init__() is called for the pointer. This should receive database/schema
        kwargs and pass them to Resource._register_scope()

    10. Resource _register_scope() is called with a database and schema. This should create
        more ResourcePointers and chain them with parent-child relationships:
        Database(db) -> Schema(sch) -> this


    """
    access_int = res.ExternalAccessIntegration(
        name="test",
        allowed_network_rules=["db.sch.some_network_rule"],
    )
    assert len(access_int._data.allowed_network_rules) == 1
    network_rule_pointer = access_int._data.allowed_network_rules[0]
    assert isinstance(network_rule_pointer, ResourcePointer)
    assert network_rule_pointer.name == "some_network_rule"

    network_rule_schema = network_rule_pointer.container
    assert network_rule_schema is not None
    assert isinstance(network_rule_schema, ResourcePointer)
    assert network_rule_schema.resource_type == ResourceType.SCHEMA
    assert network_rule_schema.name == "sch"

    network_rule_database = network_rule_schema.container
    assert network_rule_database is not None
    assert isinstance(network_rule_database, ResourcePointer)
    assert network_rule_database.resource_type == ResourceType.DATABASE
    assert network_rule_database.name == "db"

    access_int_data = access_int.to_dict()
    assert len(access_int_data["allowed_network_rules"]) == 1
    network_rule_serialized = access_int_data["allowed_network_rules"][0]
    assert isinstance(network_rule_serialized, str)
    assert str(network_rule_serialized) == "DB.SCH.SOME_NETWORK_RULE"

    assert True
