import os
import pytest

import snowflake.connector.errors

from tests.helpers import get_json_fixtures

from titan import resources as res
from titan.blueprint import Blueprint
from titan.client import FEATURE_NOT_ENABLED_ERR, UNSUPPORTED_FEATURE
from titan.scope import DatabaseScope, SchemaScope

JSON_FIXTURES = list(get_json_fixtures())
TEST_ROLE = os.environ.get("TEST_SNOWFLAKE_ROLE")


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
def test_create_drop_from_json(resource, cursor, suffix, marked_for_cleanup):
    lifecycle_db = f"LIFECYCLE_DB_{suffix}"
    cursor.execute("USE ROLE SYSADMIN")
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {lifecycle_db}")
    cursor.execute(f"USE DATABASE {lifecycle_db}")
    cursor.execute("USE WAREHOUSE CI")

    database = res.Database(name=lifecycle_db, owner="SYSADMIN")
    marked_for_cleanup.append(database)

    feature_enabled = True

    # Not easily testable without flakiness
    if resource.__class__ in (
        res.Service,
        res.Grant,
        res.RoleGrant,
    ):
        pytest.skip("Skipping Service")

    try:
        if isinstance(resource.scope, DatabaseScope):
            database.add(resource)
        elif isinstance(resource.scope, SchemaScope):
            database.public_schema.add(resource)

        blueprint = Blueprint()
        blueprint.add(resource)
        plan = blueprint.plan(cursor.connection)
        assert len(plan) == 1
        blueprint.apply(cursor.connection, plan)
    except snowflake.connector.errors.ProgrammingError as err:
        if err.errno == FEATURE_NOT_ENABLED_ERR or err.errno == UNSUPPORTED_FEATURE:
            feature_enabled = False
            pytest.skip(f"Skipping {resource.__class__.__name__}, feature not enabled")
        else:
            pytest.fail(f"Failed to create resource {resource}")
    except Exception:
        pytest.fail(f"Failed to create resource {resource}")
    finally:
        if feature_enabled:
            try:
                drop_sql = resource.drop_sql(if_exists=True)
                cursor.execute(drop_sql)
            except Exception:
                pytest.fail(f"Failed to drop resource with sql {drop_sql}")
