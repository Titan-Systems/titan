import pytest

import snowflake.connector.errors

from tests.helpers import get_json_fixtures

from titan import resources as res
from titan.client import FEATURE_NOT_ENABLED_ERR, UNSUPPORTED_FEATURE

JSON_FIXTURES = list(get_json_fixtures())


@pytest.fixture(
    params=JSON_FIXTURES,
    ids=[resource_cls.__name__ for resource_cls, _ in JSON_FIXTURES],
    scope="function",
)
def resource(request):
    resource_cls, data = request.param
    res = resource_cls(**data)

    yield res


@pytest.mark.requires_snowflake
def test_create_drop_from_json(resource, test_db, cursor):
    cursor.execute(f"USE DATABASE {test_db}")
    cursor.execute("USE WAREHOUSE CI")

    feature_enabled = True

    if resource.__class__ == res.Service:
        pytest.skip("Skipping Service")

    try:
        create_sql = resource.create_sql()
        cursor.execute(create_sql)
    except snowflake.connector.errors.ProgrammingError as err:
        if err.errno == FEATURE_NOT_ENABLED_ERR or err.errno == UNSUPPORTED_FEATURE:
            feature_enabled = False
            pytest.skip(f"Skipping {resource.__class__.__name__}, feature not enabled")
        else:
            pytest.fail(f"Failed to create resource with sql {create_sql}")
    except Exception:
        pytest.fail(f"Failed to create resource with sql {create_sql}")
    finally:
        if feature_enabled:
            try:
                drop_sql = resource.drop_sql(if_exists=True)
                cursor.execute(drop_sql)
            except Exception:
                pytest.fail(f"Failed to drop resource with sql {drop_sql}")
