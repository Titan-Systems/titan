import pytest

from tests.helpers import STATIC_RESOURCES, get_json_fixtures

from titan.enums import ResourceType

JSON_FIXTURES = list(get_json_fixtures())


@pytest.fixture(
    params=JSON_FIXTURES,
    ids=[resource_cls.__name__ for resource_cls, _ in JSON_FIXTURES],
    scope="function",
)
def resource(request, cursor, marked_for_cleanup):
    resource_cls, data = request.param
    res = resource_cls(**data)
    marked_for_cleanup.append(res)
    for ref in res.refs:
        if ref.resource_type in STATIC_RESOURCES:
            static_res = STATIC_RESOURCES[ref.resource_type]
            cursor.execute(static_res.create_sql(if_not_exists=True))
            marked_for_cleanup.append(static_res)
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
