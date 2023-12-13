import os
import uuid

import pytest
import snowflake.connector

from titan import data_provider
from titan.enums import Scope
from titan.identifiers import FQN
from titan.resources import Resource

TEST_ROLE = os.environ.get("TEST_SNOWFLAKE_ROLE")

connection_params = {
    "account": os.environ.get("TEST_SNOWFLAKE_ACCOUNT"),
    "user": os.environ.get("TEST_SNOWFLAKE_USER"),
    "password": os.environ.get("TEST_SNOWFLAKE_PASSWORD"),
    "role": TEST_ROLE,
}

resources = [
    {
        "resource_key": "alert",
        "setup_sql": [
            "CREATE WAREHOUSE {name}_wh",
            "CREATE ALERT {name} WAREHOUSE = {name}_wh SCHEDULE = '60 MINUTE' IF(EXISTS(SELECT 1)) THEN SELECT 1",
        ],
        "teardown_sql": ["DROP ALERT {name}", "DROP WAREHOUSE {name}_wh"],
        "data": lambda name: {
            "name": name,
            "warehouse": f"{name}_WH",
            "schedule": "60 MINUTE",
            "condition": "SELECT 1",
            "then": "SELECT 1",
            "owner": TEST_ROLE,
        },
    },
    {
        "resource_key": "database",
        "setup_sql": "CREATE DATABASE {name}",
        "teardown_sql": "DROP DATABASE {name}",
        "data": lambda name: {
            "name": name,
            "owner": TEST_ROLE,
            "data_retention_time_in_days": 1,
            "max_data_extension_time_in_days": 14,
            "transient": False,
        },
    },
    {
        "resource_key": "javascript_udf",
        "setup_sql": "CREATE FUNCTION {name}() RETURNS double LANGUAGE JAVASCRIPT AS 'return 42;'",
        "teardown_sql": "DROP FUNCTION {name}()",
        "fetch_method": "fetch_javascript_udf",
        "data": lambda name: {
            "name": name,
            "secure": False,
            "returns": "FLOAT",
            "language": "JAVASCRIPT",
            "volatility": "VOLATILE",
            "as_": "return 42;",
        },
    },
    {
        "resource_key": "role",
        "setup_sql": "CREATE ROLE {name}",
        "teardown_sql": "DROP ROLE {name}",
        "data": lambda name: {
            "name": name,
            "owner": TEST_ROLE,
        },
    },
    {
        "resource_key": "schema",
        "setup_sql": "CREATE TRANSIENT SCHEMA {name}",
        "teardown_sql": "DROP SCHEMA {name}",
        "data": lambda name: {
            "name": name,
            "owner": TEST_ROLE,
            "data_retention_time_in_days": 1,
            "max_data_extension_time_in_days": 1,
            "transient": True,
            "with_managed_access": False,
        },
    },
    {
        "resource_key": "shared_database",
        "setup_sql": [
            "CALL SYSTEM$ACCEPT_LEGAL_TERMS('DATA_EXCHANGE_LISTING', 'GZSOZ1LLE9')",
            """CREATE DATABASE {name} FROM SHARE WEATHERSOURCE.SNOWFLAKE_MANAGED$PUBLIC_GCP_US_CENTRAL1."WEATHERSOURCE_SNOWFLAKE_SNOWPARK_TILE_SNOWFLAKE_SECURE_SHARE_1651768630709" """,
        ],
        "teardown_sql": "DROP DATABASE {name}",
        "data": lambda name: {
            "name": name,
            "owner": TEST_ROLE,
            "from_share": "WEATHERSOURCE.SNOWFLAKE_MANAGED$PUBLIC_GCP_US_CENTRAL1.WEATHERSOURCE_SNOWFLAKE_SNOWPARK_TILE_SNOWFLAKE_SECURE_SHARE_1651768630709",
        },
    },
    {
        "resource_key": "table",
        "setup_sql": "CREATE TABLE {name} (id INT)",
        "teardown_sql": "DROP TABLE {name}",
        "data": lambda name: {
            "name": name,
            "owner": TEST_ROLE,
            "columns": [{"name": "ID", "nullable": True, "data_type": "NUMBER(38,0)"}],
        },
    },
]


def _generate_fqn(resource, test_db):
    resource_cls = Resource.classes[resource["resource_key"]]
    if resource_cls.scope == Scope.ACCOUNT:
        return FQN(name=resource["name"])
    elif resource_cls.scope == Scope.DATABASE:
        return FQN(name=resource["name"], database=test_db)
    elif resource_cls.scope == Scope.SCHEMA:
        return FQN(name=resource["name"], schema="PUBLIC")


@pytest.fixture(scope="session")
def suffix():
    return str(uuid.uuid4())[:8]


@pytest.fixture(scope="session")
def test_db(suffix):
    return f"TEST_DB_RUN_{suffix}"


@pytest.fixture(scope="session")
def db_session(suffix):
    return snowflake.connector.connect(**connection_params)


@pytest.fixture(scope="session")
def cursor(db_session, suffix, test_db):
    with db_session.cursor() as cur:
        cur.execute(f"ALTER SESSION set query_tag='titan_package:test::{suffix}'")
        cur.execute(f"USE ROLE {TEST_ROLE}")
        cur.execute(f"CREATE DATABASE {test_db}")
        yield cur
        cur.execute(f"DROP DATABASE {test_db}")


@pytest.fixture(
    params=resources,
    ids=[f"test_fetch_{config['resource_key']}" for config in resources],
    scope="function",
)
def resource(request, cursor, suffix, test_db):
    config = request.param
    setup_sqls = config["setup_sql"] if isinstance(config["setup_sql"], list) else [config["setup_sql"]]
    teardown_sqls = config["teardown_sql"] if isinstance(config["teardown_sql"], list) else [config["teardown_sql"]]
    resource_name = f"test_{config['resource_key']}_{suffix}".upper()

    cursor.execute(f"USE DATABASE {test_db}")
    for setup_sql in setup_sqls:
        cursor.execute(setup_sql.format(name=resource_name))
    try:
        data = config["data"](name=resource_name)
        fetch_method = config.get("fetch_method", f"fetch_{config['resource_key'].lower()}")
        yield {
            "name": resource_name,
            "resource_key": config["resource_key"],
            "data": data,
            "fetch_method": fetch_method,
        }
    finally:
        for teardown_sql in teardown_sqls:
            cursor.execute(teardown_sql.format(name=resource_name))


def test_fetch_resource(resource, db_session, test_db):
    fetch = getattr(data_provider, resource["fetch_method"])
    fqn = _generate_fqn(resource, test_db)
    result = fetch(db_session, fqn)
    assert result is not None
    result = data_provider.remove_none_values(result)
    assert result == resource["data"]
