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
    session = snowflake.connector.connect(**connection_params())
    with session.cursor() as cur:
        cur.execute(f"ALTER SESSION set query_tag='titan_package:test::{suffix}'")
        cur.execute(f"CREATE DATABASE {test_db}")
        cur.execute("CREATE WAREHOUSE IF NOT EXISTS CI WAREHOUSE_SIZE = XSMALL AUTO_SUSPEND = 60 AUTO_RESUME = TRUE")
        try:
            cur.execute("USE WAREHOUSE CI")
            cur.execute(f"USE ROLE {TEST_ROLE}")
            yield cur
            cur.execute(f"USE DATABASE {test_db}")
            for res in marked_for_cleanup:
                cur.execute(res.drop_sql(if_exists=True))
        finally:
            cur.execute(f"DROP DATABASE {test_db}")
