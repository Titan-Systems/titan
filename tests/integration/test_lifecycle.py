import os
import pytest

import snowflake.connector.errors

from tests.helpers import get_json_fixtures

from titan import resources as res
from titan.blueprint import Blueprint
from titan.client import FEATURE_NOT_ENABLED_ERR, UNSUPPORTED_FEATURE
from titan.data_provider import fetch_session
from titan.enums import AccountEdition
from titan.scope import DatabaseScope, SchemaScope

JSON_FIXTURES = list(get_json_fixtures())
TEST_ROLE = os.environ.get("TEST_SNOWFLAKE_ROLE")

pytestmark = pytest.mark.requires_snowflake


@pytest.fixture(
    params=JSON_FIXTURES,
    ids=[resource_cls.__name__ for resource_cls, _ in JSON_FIXTURES],
    scope="function",
)
def resource(request):
    resource_cls, data = request.param
    res = resource_cls(**data)

    yield res


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
        res.PasswordPolicy,
    ):
        pytest.skip("Skipping Service")

    try:

        session_ctx = fetch_session(cursor.connection)
        account_edition = AccountEdition.ENTERPRISE if session_ctx["tag_support"] else AccountEdition.STANDARD

        if account_edition not in resource.edition:
            feature_enabled = False
            pytest.skip(f"Skipping {resource.__class__.__name__}, not supported by account edition {account_edition}")

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
