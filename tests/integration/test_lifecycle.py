import pytest

from tests.helpers import get_json_fixtures

from titan.enums import AccountEdition

JSON_FIXTURES = list(get_json_fixtures())


@pytest.fixture(
    params=JSON_FIXTURES,
    ids=[resource_cls.__name__ for resource_cls, _ in JSON_FIXTURES],
    scope="function",
)
def resource(request):
    resource_cls, data = request.param
    res = resource_cls(**data)

    if AccountEdition.STANDARD not in resource_cls.edition:
        pytest.skip(f"Skipping {resource_cls.__name__}, it's not supported in standard edition")

    yield res


@pytest.mark.requires_snowflake
def test_create_drop_from_json(resource, test_db, cursor):
    cursor.execute(f"USE DATABASE {test_db}")
    cursor.execute("USE WAREHOUSE CI")
    try:
        create_sql = resource.create_sql()
        cursor.execute(create_sql)
    except Exception as e:
        pytest.fail(f"Failed to create resource with sql {create_sql}")
    finally:
        cursor.execute(resource.drop_sql(if_exists=True))
