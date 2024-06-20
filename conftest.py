import pytest
import os
import uuid

import snowflake.connector

TEST_ROLE = os.environ.get("TEST_SNOWFLAKE_ROLE")


def connection_params():
    return {
        "account": os.environ["TEST_SNOWFLAKE_ACCOUNT"],
        "user": os.environ["TEST_SNOWFLAKE_USER"],
        "password": os.environ["TEST_SNOWFLAKE_PASSWORD"],
        "role": TEST_ROLE,
    }


def pytest_addoption(parser):
    parser.addoption(
        "--snowflake",
        action="store_true",
        default=False,
        help="Runs tests that require a Snowflake connection",
    )


def pytest_runtest_setup(item):
    if "requires_snowflake" in item.keywords and not item.config.getoption("--snowflake"):
        pytest.skip("need --snowflake option to run this test")


def pytest_collection_modifyitems(items):
    for item in items:
        if not item.get_closest_marker("enterprise"):
            item.add_marker("standard")


@pytest.fixture(scope="session")
def suffix():
    return str(uuid.uuid4())[:8].upper()


@pytest.fixture(scope="session")
def test_db(suffix):
    return f"TEST_DB_RUN_{suffix}"


@pytest.fixture(scope="session")
def marked_for_cleanup() -> list:
    """List to keep track of resources created during tests."""
    return []


@pytest.fixture(scope="session")
def cursor(suffix, test_db, marked_for_cleanup):
    session = snowflake.connector.connect(**connection_params())
    with session.cursor(snowflake.connector.DictCursor) as cur:
        cur.execute(f"ALTER SESSION set query_tag='titan_package:test::{suffix}'")
        cur.execute(f"CREATE DATABASE {test_db}")
        try:
            yield cur
            cur.execute(f"USE ROLE {TEST_ROLE}")
            cur.execute(f"USE DATABASE {test_db}")
            for res in marked_for_cleanup:
                cur.execute(res.drop_sql(if_exists=True))
        finally:
            cur.execute(f"DROP DATABASE {test_db}")


@pytest.fixture(scope="session")
def dummy_cursor(request):
    if not request.config.getoption("--snowflake"):
        yield None
    else:
        yield request.getfixturevalue("cursor")


@pytest.fixture(autouse=True)
def reset_cursor_context(dummy_cursor, test_db):
    """
    This fixture resets the cursor's context to the initial test database before each test.
    It uses `autouse=True` to automatically apply it to each test without needing to explicitly include it.
    """
    cursor = dummy_cursor
    if cursor:
        cursor.execute(f"USE ROLE {TEST_ROLE}")
        cursor.execute("USE WAREHOUSE CI")
        cursor.execute(f"USE DATABASE {test_db}")
    yield
