import pytest

from tests.helpers import STATIC_RESOURCES, get_json_fixtures

from titan.enums import ResourceType
from titan.scope import DatabaseScope, SchemaScope
from titan.resources.schema import Schema

JSON_FIXTURES = list(get_json_fixtures())


@pytest.fixture(
    params=JSON_FIXTURES,
    ids=[resource_cls.__name__ for resource_cls, _ in JSON_FIXTURES],
    scope="function",
)
def resource(request, test_db, cursor, marked_for_cleanup):
    resource_cls, data = request.param
    res = resource_cls(**data)
    marked_for_cleanup.append(res)
    test_schema = Schema(name="public", database=test_db)
    for ref in res.refs:
        if ref.resource_type in STATIC_RESOURCES:
            print(f"Creating static resource {ref.resource_type} for {res.urn}")
            static_res = STATIC_RESOURCES[ref.resource_type]
            if isinstance(res.scope, DatabaseScope):
                static_res._register_scope(database=test_db)
            elif isinstance(res.scope, SchemaScope):
                static_res._register_scope(schema=test_schema)
            cursor.execute(static_res.create_sql(if_not_exists=True))
            marked_for_cleanup.append(static_res)
    # A hack until we add proper SQL parsing for view code
    if res.resource_type == ResourceType.VIEW:
        static_table = STATIC_RESOURCES[ResourceType.TABLE]
        cursor.execute(static_table.create_sql(if_not_exists=True))
        marked_for_cleanup.append(static_table)
    yield res


@pytest.mark.requires_snowflake
def test_create_drop(resource, test_db, cursor):
    cursor.execute(f"USE DATABASE {test_db}")
    cursor.execute("USE WAREHOUSE CI")
    cursor.execute(resource.create_sql())
    cursor.execute(resource.drop_sql())
