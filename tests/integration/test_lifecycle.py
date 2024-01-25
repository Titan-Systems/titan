import os
import uuid

import pytest
import snowflake.connector

from tests.helpers import STATIC_RESOURCES, get_json_fixtures

TEST_ROLE = os.environ.get("TEST_SNOWFLAKE_ROLE")

connection_params = {
    "account": os.environ.get("TEST_SNOWFLAKE_ACCOUNT"),
    "user": os.environ.get("TEST_SNOWFLAKE_USER"),
    "password": os.environ.get("TEST_SNOWFLAKE_PASSWORD"),
    "role": TEST_ROLE,
}

JSON_FIXTURES = list(get_json_fixtures())


@pytest.fixture(scope="session")
def suffix():
    return str(uuid.uuid4())[:8]


@pytest.fixture(scope="session")
def test_db(suffix):
    return f"TEST_DB_RUN_{suffix}"


@pytest.fixture(scope="session")
def marked_for_cleanup():
    """List to keep track of resources created during tests."""
    return []


@pytest.fixture(scope="session")
def cursor(suffix, test_db, marked_for_cleanup):
    session = snowflake.connector.connect(**connection_params)
    with session.cursor() as cur:
        cur.execute(f"ALTER SESSION set query_tag='titan_package:test::{suffix}'")
        cur.execute(f"CREATE DATABASE {test_db}")
        cur.execute(f"USE ROLE {TEST_ROLE}")
        try:
            yield cur
            for res in marked_for_cleanup:
                cur.execute(res.drop_sql(if_exists=True))
        finally:
            cur.execute(f"DROP DATABASE {test_db}")


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
    yield res


@pytest.mark.requires_snowflake
def test_create_drop(resource, test_db, cursor):
    cursor.execute(f"USE DATABASE {test_db}")
    cursor.execute(resource.create_sql())
    cursor.execute(resource.drop_sql())
