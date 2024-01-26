import os

# import uuid

import pytest

# import snowflake.connector

from titan import data_provider
from titan.enums import ResourceType
from titan.identifiers import FQN, URN
from titan.parse import parse_identifier

TEST_ROLE = os.environ.get("TEST_SNOWFLAKE_ROLE")

# connection_params = {
#     "account": os.environ.get("TEST_SNOWFLAKE_ACCOUNT"),
#     "user": os.environ.get("TEST_SNOWFLAKE_USER"),
#     "password": os.environ.get("TEST_SNOWFLAKE_PASSWORD"),
#     "role": TEST_ROLE,
# }

account_resources = [
    {
        "resource_type": ResourceType.DATABASE,
        "setup_sql": "CREATE DATABASE SOMEDB",
        "teardown_sql": "DROP DATABASE IF EXISTS SOMEDB",
        "data": {
            "name": "SOMEDB",
            "owner": TEST_ROLE,
            "data_retention_time_in_days": 1,
            "max_data_extension_time_in_days": 14,
            "transient": False,
        },
    },
    {
        "resource_type": ResourceType.ROLE,
        "setup_sql": "CREATE ROLE somerole",
        "teardown_sql": "DROP ROLE IF EXISTS somerole",
        "data": {
            "name": "SOMEROLE",
            "owner": TEST_ROLE,
        },
    },
    # {
    #     "resource_type": "shared_database",
    #     "setup_sql": [
    #         "CALL SYSTEM$ACCEPT_LEGAL_TERMS('DATA_EXCHANGE_LISTING', 'GZSOZ1LLE9')",
    #         "CREATE DATABASE {name} FROM SHARE WEATHERSOURCE_SNOWFLAKE_SNOWPARK_TILE_SNOWFLAKE_SECURE_SHARE_1651768630709",
    #     ],
    #     "teardown_sql": "DROP DATABASE {name}",
    #     "data": lambda name: {
    #         "name": name,
    #         "owner": TEST_ROLE,
    #         "from_share": "WEATHERSOURCE_SNOWFLAKE_SNOWPARK_TILE_SNOWFLAKE_SECURE_SHARE_1651768630709",
    #     },
    # },
    {
        "resource_type": ResourceType.ROLE_GRANT,
        "setup_sql": [
            "CREATE USER recipient",
            "CREATE ROLE thatrole",
            "GRANT ROLE thatrole TO USER recipient",
        ],
        "teardown_sql": [
            "DROP USER IF EXISTS recipient",
            "DROP ROLE IF EXISTS thatrole",
        ],
        "fqn": "THATROLE?user=RECIPIENT",
        "data": {
            # "owner": "CI",
            "role": "THATROLE",
            "to_user": "RECIPIENT",
        },
    },
    {
        "resource_type": ResourceType.USER,
        "setup_sql": "CREATE USER someuser",
        "teardown_sql": "DROP USER IF EXISTS someuser",
        "data": {
            "name": "SOMEUSER",
            "owner": TEST_ROLE,
            "display_name": "SOMEUSER",
            "login_name": "SOMEUSER",
            "disabled": False,
            "must_change_password": False,
        },
    },
]

scoped_resources = [
    {
        "resource_type": ResourceType.ALERT,
        "setup_sql": [
            "CREATE WAREHOUSE TEST_WH",
            "CREATE ALERT somealert WAREHOUSE = TEST_WH SCHEDULE = '60 MINUTE' IF(EXISTS(SELECT 1)) THEN SELECT 1",
        ],
        "teardown_sql": ["DROP ALERT IF EXISTS somealert", "DROP WAREHOUSE IF EXISTS TEST_WH"],
        "data": {
            "name": "SOMEALERT",
            "warehouse": "TEST_WH",
            "schedule": "60 MINUTE",
            "condition": "SELECT 1",
            "then": "SELECT 1",
            "owner": TEST_ROLE,
        },
    },
    {
        "resource_type": ResourceType.DYNAMIC_TABLE,
        "setup_sql": [
            "CREATE TABLE upstream (id INT) AS select 1",
            "CREATE DYNAMIC TABLE product (id INT) TARGET_LAG = '20 minutes' WAREHOUSE = CI REFRESH_MODE = AUTO INITIALIZE = ON_CREATE COMMENT = 'this is a comment' AS SELECT id FROM upstream",
        ],
        "teardown_sql": [
            "DROP TABLE IF EXISTS upstream",
            "DROP TABLE IF EXISTS product",
        ],
        "data": {
            "name": "PRODUCT",
            "owner": TEST_ROLE,
            "columns": [{"name": "ID", "data_type": "NUMBER(38,0)", "nullable": True}],
            "target_lag": "20 minutes",
            "warehouse": "CI",
            "refresh_mode": "AUTO",
            "initialize": "ON_CREATE",
            "comment": "this is a comment",
            "as_": "SELECT id FROM upstream",
        },
    },
    {
        "resource_type": ResourceType.FUNCTION,
        "setup_sql": "CREATE FUNCTION somefunc() RETURNS double LANGUAGE JAVASCRIPT AS 'return 42;'",
        "teardown_sql": "DROP FUNCTION somefunc()",
        "data": {
            "name": "SOMEFUNC",
            "secure": False,
            "returns": "FLOAT",
            "language": "JAVASCRIPT",
            "volatility": "VOLATILE",
            "as_": "return 42;",
        },
    },
    {
        "resource_type": ResourceType.PASSWORD_POLICY,
        "setup_sql": """
            CREATE PASSWORD POLICY SOMEPOLICY
                PASSWORD_MIN_LENGTH = 12
                PASSWORD_MAX_LENGTH = 24
                PASSWORD_MIN_UPPER_CASE_CHARS = 2
                PASSWORD_MIN_LOWER_CASE_CHARS = 2
                PASSWORD_MIN_NUMERIC_CHARS = 2
                PASSWORD_MIN_SPECIAL_CHARS = 2
                PASSWORD_MIN_AGE_DAYS = 1
                PASSWORD_MAX_AGE_DAYS = 30
                PASSWORD_MAX_RETRIES = 3
                PASSWORD_LOCKOUT_TIME_MINS = 30
                PASSWORD_HISTORY = 5
                COMMENT = 'production account password policy';
        """,
        "teardown_sql": "DROP PASSWORD POLICY IF EXISTS SOMEPOLICY",
        "data": {
            "name": "SOMEPOLICY",
            "owner": TEST_ROLE,
            "password_min_length": 12,
            "password_max_length": 24,
            "password_min_upper_case_chars": 2,
            "password_min_lower_case_chars": 2,
            "password_min_numeric_chars": 2,
            "password_min_special_chars": 2,
            "password_min_age_days": 1,
            "password_max_age_days": 30,
            "password_max_retries": 3,
            "password_lockout_time_mins": 30,
            "password_history": 5,
            "comment": "production account password policy",
        },
    },
    {
        "resource_type": ResourceType.PROCEDURE,
        "setup_sql": """
            CREATE PROCEDURE somesproc(ARG1 VARCHAR)
                RETURNS INT NOT NULL
                language python
                packages = ('snowflake-snowpark-python')
                runtime_version = '3.9'
                handler = 'main'
                as 'def main(_, arg1: str): return 42'
        """,
        "teardown_sql": "DROP PROCEDURE somesproc(VARCHAR)",
        "data": {
            "name": "somesproc",
            "args": [{"name": "ARG1", "data_type": "VARCHAR"}],
            "returns": "NUMBER",
            "language": "PYTHON",
            "packages": ["snowflake-snowpark-python"],
            "runtime_version": "3.9",
            "handler": "main",
            "execute_as": "OWNER",
            "comment": "user-defined procedure",
            "imports": [],
            "null_handling": "CALLED ON NULL INPUT",
            "secure": False,
            "owner": TEST_ROLE,
            "as_": "def main(_, arg1: str): return 42",
        },
    },
    {
        "resource_type": ResourceType.SCHEMA,
        "setup_sql": "CREATE TRANSIENT SCHEMA somesch MAX_DATA_EXTENSION_TIME_IN_DAYS = 3",
        "teardown_sql": "DROP SCHEMA IF EXISTS somesch",
        "data": {
            "name": "SOMESCH",
            "owner": TEST_ROLE,
            "data_retention_time_in_days": 1,
            "max_data_extension_time_in_days": 3,
            "transient": True,
            "managed_access": False,
        },
    },
    {
        "resource_type": ResourceType.TABLE,
        "setup_sql": "CREATE TABLE sometbl (id INT)",
        "teardown_sql": "DROP TABLE IF EXISTS sometbl",
        "data": {
            "name": "SOMETBL",
            "owner": TEST_ROLE,
            "columns": [{"name": "ID", "nullable": True, "data_type": "NUMBER(38,0)"}],
        },
    },
]


@pytest.fixture(scope="session")
def account_locator(cursor):
    return data_provider.fetch_account_locator(cursor)


@pytest.fixture(
    params=scoped_resources,
    ids=[f"test_fetch_{config['resource_type']}" for config in scoped_resources],
    scope="function",
)
def scoped_resource(request, cursor, test_db):
    config = request.param
    setup_sqls = config["setup_sql"] if isinstance(config["setup_sql"], list) else [config["setup_sql"]]
    teardown_sqls = config["teardown_sql"] if isinstance(config["teardown_sql"], list) else [config["teardown_sql"]]

    cursor.execute(f"USE DATABASE {test_db}")
    cursor.execute("USE SCHEMA PUBLIC")
    for setup_sql in setup_sqls:
        cursor.execute(setup_sql)
    try:
        yield config
    finally:
        for teardown_sql in teardown_sqls:
            cursor.execute(teardown_sql)


@pytest.mark.requires_snowflake
def test_fetch_scoped_resource(scoped_resource, cursor, account_locator, test_db):
    fqn = FQN(
        name=scoped_resource["data"]["name"],
        database=test_db,
        schema=None if scoped_resource["resource_type"] == ResourceType.SCHEMA else "PUBLIC",
    )
    urn = URN(
        resource_type=scoped_resource["resource_type"],
        fqn=fqn,
        account_locator=account_locator,
    )
    cursor.execute("USE WAREHOUSE CI")
    result = data_provider.fetch_resource(cursor, urn)
    assert result is not None
    result = data_provider.remove_none_values(result)
    assert result == scoped_resource["data"]


@pytest.fixture(
    params=account_resources,
    ids=[f"test_fetch_{config['resource_type']}" for config in account_resources],
    scope="function",
)
def account_resource(request, cursor):
    config = request.param
    setup_sqls = config["setup_sql"] if isinstance(config["setup_sql"], list) else [config["setup_sql"]]
    teardown_sqls = config["teardown_sql"] if isinstance(config["teardown_sql"], list) else [config["teardown_sql"]]

    for setup_sql in setup_sqls:
        cursor.execute(setup_sql)
    try:
        yield config
    finally:
        for teardown_sql in teardown_sqls:
            cursor.execute(teardown_sql)


@pytest.mark.requires_snowflake
def test_fetch_account_resource(account_resource, cursor, account_locator):
    if "name" in account_resource["data"]:
        fqn = parse_identifier(account_resource["data"]["name"])
    else:
        fqn = parse_identifier(account_resource["fqn"])
    urn = URN(
        resource_type=account_resource["resource_type"],
        fqn=fqn,
        account_locator=account_locator,
    )

    result = data_provider.fetch_resource(cursor, urn)
    assert result is not None
    result = data_provider.remove_none_values(result)
    assert result == account_resource["data"]
