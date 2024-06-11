import os
import pytest

from titan import data_provider
from titan.client import reset_cache
from titan.enums import ResourceType
from titan.identifiers import FQN, URN
from titan.parse import parse_identifier, parse_URN
from titan.resources.grant import _FutureGrant, _Grant, future_grant_fqn, grant_fqn
from titan.resources import ExternalStage, CSVFileFormat
from titan.resource_name import ResourceName


from tests.helpers import STATIC_RESOURCES, get_json_fixture

TEST_ROLE = os.environ.get("TEST_SNOWFLAKE_ROLE")

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
        "setup_sql": "CREATE ROLE thisisatestrole",
        "teardown_sql": "DROP ROLE IF EXISTS thisisatestrole",
        "data": {
            "name": "THISISATESTROLE",
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
        "setup_sql": 'CREATE USER "someuser@applytitan.com"',
        "teardown_sql": 'DROP USER "someuser@applytitan.com"',
        "data": {
            "name": "someuser@applytitan.com",
            "owner": TEST_ROLE,
            "display_name": "someuser@applytitan.com",
            "login_name": "SOMEUSER@APPLYTITAN.COM",
            "disabled": False,
            "must_change_password": False,
        },
    },
    {
        "resource_type": ResourceType.CATALOG_INTEGRATION,
        "setup_sql": "CREATE CATALOG INTEGRATION objectStoreCatalogInt CATALOG_SOURCE=OBJECT_STORE TABLE_FORMAT=ICEBERG ENABLED=TRUE COMMENT='This is a test catalog integration';",
        "teardown_sql": "DROP CATALOG INTEGRATION objectStoreCatalogInt",
        "data": {
            "name": "OBJECTSTORECATALOGINT",
            "catalog_source": "OBJECT_STORE",
            "table_format": "ICEBERG",
            "enabled": True,
            "owner": "ACCOUNTADMIN",
            "comment": "This is a test catalog integration",
        },
    },
    {
        "resource_type": ResourceType.SHARE,
        "setup_sql": "CREATE SHARE SOME_SHARE COMMENT = 'A share for testing'",
        "teardown_sql": "DROP SHARE IF EXISTS SOME_SHARE",
        "data": {
            "name": "SOME_SHARE",
            "owner": "ACCOUNTADMIN",
            "comment": "A share for testing",
        },
    },
    {
        "resource_type": ResourceType.STORAGE_INTEGRATION,
        "setup_sql": """CREATE STORAGE INTEGRATION SOME_STORAGE
            TYPE = EXTERNAL_STAGE
            STORAGE_PROVIDER = 'S3'
            STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::001234567890:role/myrole'
            ENABLED = TRUE
            STORAGE_ALLOWED_LOCATIONS = ('s3://mybucket1/path1/', 's3://mybucket2/path2/');""",
        "teardown_sql": "DROP STORAGE INTEGRATION IF EXISTS SOME_STORAGE",
        "data": {
            "name": "SOME_STORAGE",
            "type": "EXTERNAL_STAGE",
            "storage_provider": "S3",
            "storage_aws_role_arn": "arn:aws:iam::001234567890:role/myrole",
            "enabled": True,
            "owner": "ACCOUNTADMIN",
            "storage_allowed_locations": ["s3://mybucket1/path1/", "s3://mybucket2/path2/"],
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
        "resource_type": ResourceType.SEQUENCE,
        "setup_sql": "CREATE SEQUENCE someseq START 1 INCREMENT 2 COMMENT = '+3'",
        "teardown_sql": "DROP SEQUENCE IF EXISTS someseq",
        "data": {
            "name": "SOMESEQ",
            "owner": TEST_ROLE,
            "start": 1,
            "increment": 2,
            "comment": "+3",
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
    {
        "resource_type": ResourceType.TASK,
        "setup_sql": "CREATE TASK sometask SCHEDULE = '60 MINUTE' AS SELECT 1",
        "teardown_sql": "DROP TASK IF EXISTS sometask",
        "data": {
            "name": "SOMETASK",
            "owner": TEST_ROLE,
            "schedule": "60 MINUTE",
            "state": "SUSPENDED",
            "as_": "SELECT 1",
        },
    },
]

grants = [
    {
        "setup_sql": [
            "CREATE ROLE IF NOT EXISTS thatrole",
            "GRANT USAGE ON DATABASE STATIC_DATABASE TO ROLE thatrole",
        ],
        "teardown_sql": [
            "DROP ROLE IF EXISTS thatrole",
        ],
        "test_name": "test_usage_grant",
        "resource_type": ResourceType.GRANT,
        "data": {
            "priv": "USAGE",
            "on_type": "DATABASE",
            "on": "STATIC_DATABASE",
            "to": "THATROLE",
            "owner": TEST_ROLE,
            "grant_option": False,
            "_privs": ["USAGE"],
        },
    },
]

future_grants = [
    {
        "setup_sql": [
            "CREATE ROLE IF NOT EXISTS thatrole",
            "GRANT USAGE ON FUTURE SCHEMAS IN DATABASE STATIC_DATABASE TO ROLE thatrole",
        ],
        "teardown_sql": [
            "DROP ROLE IF EXISTS thatrole",
        ],
        "test_name": "test_future_grant",
        "resource_type": ResourceType.FUTURE_GRANT,
        "data": {
            "priv": "USAGE",
            "on_type": "SCHEMA",
            "in_type": "database",
            "in_name": "STATIC_DATABASE",
            "to": "THATROLE",
            "grant_option": False,
        },
    },
]


def safe_fetch(cursor, urn):
    reset_cache()
    return data_provider.fetch_resource(cursor, urn)


@pytest.fixture(scope="session")
def account_locator(cursor):
    reset_cache()
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
    result = safe_fetch(cursor, urn)
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


@pytest.fixture(
    params=grants,
    ids=[config["test_name"] for config in grants],
    scope="function",
)
def grant_resource(request, cursor, account_locator):
    config = request.param

    static_db = STATIC_RESOURCES[ResourceType.DATABASE]
    cursor.execute(static_db.create_sql(if_not_exists=True))

    static_wh = STATIC_RESOURCES[ResourceType.WAREHOUSE]
    cursor.execute(static_wh.create_sql(if_not_exists=True))

    setup_sqls = config["setup_sql"] if isinstance(config["setup_sql"], list) else [config["setup_sql"]]
    teardown_sqls = config["teardown_sql"] if isinstance(config["teardown_sql"], list) else [config["teardown_sql"]]

    if config["resource_type"] == ResourceType.GRANT:
        fqn = grant_fqn(_Grant(**config["data"]))
        urn = URN(
            resource_type=config["resource_type"],
            fqn=fqn,
            account_locator=account_locator,
        )
        config["urn"] = urn
    else:
        raise ValueError(f"Invalid resource type: {config['resource_type']}")

    for setup_sql in setup_sqls:
        cursor.execute(setup_sql)
    try:
        yield config
    finally:
        for teardown_sql in teardown_sqls:
            cursor.execute(teardown_sql)


@pytest.fixture(
    params=future_grants,
    ids=[config["test_name"] for config in future_grants],
    scope="function",
)
def future_grant_resource(request, cursor, account_locator):
    config = request.param

    static_db = STATIC_RESOURCES[ResourceType.DATABASE]
    cursor.execute(static_db.create_sql(if_not_exists=True))

    setup_sqls = config["setup_sql"] if isinstance(config["setup_sql"], list) else [config["setup_sql"]]
    teardown_sqls = config["teardown_sql"] if isinstance(config["teardown_sql"], list) else [config["teardown_sql"]]

    fqn = future_grant_fqn(_FutureGrant(**config["data"]))
    urn = URN(
        resource_type=config["resource_type"],
        fqn=fqn,
        account_locator=account_locator,
    )
    config["urn"] = urn

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
        fqn = FQN(name=ResourceName(account_resource["data"]["name"]))
    else:
        fqn = parse_identifier(account_resource["fqn"])
    urn = URN(
        resource_type=account_resource["resource_type"],
        fqn=fqn,
        account_locator=account_locator,
    )

    result = safe_fetch(cursor, urn)
    assert result is not None
    result = data_provider.remove_none_values(result)
    assert result == account_resource["data"]


@pytest.mark.requires_snowflake
def test_fetch_grant(grant_resource, cursor):
    result = safe_fetch(cursor, grant_resource["urn"])
    assert result is not None
    result = data_provider.remove_none_values(result)
    assert result == grant_resource["data"]


@pytest.mark.requires_snowflake
def test_fetch_future_grant(future_grant_resource, cursor):
    result = safe_fetch(cursor, future_grant_resource["urn"])
    assert result is not None
    assert result == future_grant_resource["data"]


@pytest.mark.requires_snowflake
@pytest.mark.enterprise
def test_fetch_enterprise_schema(cursor, account_locator, test_db):
    static_tag = STATIC_RESOURCES[ResourceType.TAG]
    cursor.execute(static_tag.create_sql(if_not_exists=True))

    urn = URN(
        resource_type=ResourceType.SCHEMA,
        fqn=FQN(name="ENTERPRISE_TEST_SCHEMA", database=test_db),
        account_locator=account_locator,
    )
    cursor.execute(
        f"""
            CREATE SCHEMA {test_db}.ENTERPRISE_TEST_SCHEMA
                DATA_RETENTION_TIME_IN_DAYS = 90
                WITH TAG (STATIC_TAG = 'SOMEVALUE')
        """
    )

    result = safe_fetch(cursor, urn)
    assert result == {
        "name": "ENTERPRISE_TEST_SCHEMA",
        "transient": False,
        "managed_access": False,
        "data_retention_time_in_days": 90,
        "max_data_extension_time_in_days": 14,
        "default_ddl_collation": None,
        "tags": {"STATIC_TAG": "SOMEVALUE"},
        "owner": TEST_ROLE,
        "comment": None,
    }


@pytest.fixture(scope="session")
def account_grant(cursor, marked_for_cleanup):
    static_role = STATIC_RESOURCES[ResourceType.ROLE]
    cursor.execute(static_role.create_sql(if_not_exists=True))
    marked_for_cleanup.append(static_role)
    cursor.execute(f"GRANT AUDIT ON ACCOUNT TO ROLE {static_role.name}")
    cursor.execute(f"GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO ROLE {static_role.name}")
    yield
    cursor.execute(f"REVOKE AUDIT ON ACCOUNT FROM ROLE {static_role.name}")
    cursor.execute(f"REVOKE BIND SERVICE ENDPOINT ON ACCOUNT FROM ROLE {static_role.name}")


@pytest.mark.requires_snowflake
def test_fetch_grant_on_account(cursor, account_grant):
    static_role = STATIC_RESOURCES[ResourceType.ROLE]
    bind_service_urn = parse_URN(f"urn:::grant/{static_role.name}?priv=BIND SERVICE ENDPOINT&on=account/ACCOUNT")
    bind_service_grant = safe_fetch(cursor, bind_service_urn)
    assert bind_service_grant is not None
    assert bind_service_grant["priv"] == "BIND SERVICE ENDPOINT"
    assert bind_service_grant["on"] == "ACCOUNT"
    assert bind_service_grant["on_type"] == "ACCOUNT"
    assert bind_service_grant["to"] == static_role.name
    audit_urn = parse_URN(f"urn:::grant/{static_role.name}?priv=AUDIT&on=account/ACCOUNT")
    audit_grant = safe_fetch(cursor, audit_urn)
    assert audit_grant is not None
    assert audit_grant["priv"] == "AUDIT"
    assert audit_grant["on"] == "ACCOUNT"
    assert audit_grant["on_type"] == "ACCOUNT"
    assert audit_grant["to"] == static_role.name


@pytest.mark.requires_snowflake
def test_fetch_grant_all_on_resource(cursor, marked_for_cleanup):
    # Setup
    static_role = STATIC_RESOURCES[ResourceType.ROLE]
    static_wh = STATIC_RESOURCES[ResourceType.WAREHOUSE]
    cursor.execute("DROP ROLE IF EXISTS STATIC_ROLE")
    cursor.execute(static_role.create_sql(if_not_exists=True))
    cursor.execute(static_wh.create_sql(if_not_exists=True))
    marked_for_cleanup.append(static_role)
    marked_for_cleanup.append(static_wh)
    cursor.execute(f"GRANT ALL ON WAREHOUSE {static_wh.name} TO ROLE {static_role.name}")

    # Test
    grant_all_urn = parse_URN(f"urn:::grant/{static_role.name}?priv=ALL&on=warehouse/{static_wh.name}")

    grant = safe_fetch(cursor, grant_all_urn)
    assert grant is not None
    assert grant["priv"] == "ALL"
    assert grant["on_type"] == "WAREHOUSE"
    assert grant["on"] == static_wh.name
    assert grant["to"] == static_role.name
    assert grant["owner"] == TEST_ROLE
    assert grant["grant_option"] is False
    assert grant["_privs"] == ["APPLYBUDGET", "MODIFY", "MONITOR", "OPERATE", "USAGE"]

    cursor.execute(f"REVOKE MODIFY ON WAREHOUSE {static_wh.name} FROM ROLE {static_role.name}")

    grant = safe_fetch(cursor, grant_all_urn)
    assert grant is not None
    assert "MODIFY" not in grant["_privs"]


@pytest.mark.requires_snowflake
def test_fetch_external_stage(cursor, test_db):
    external_stage = ExternalStage(
        name="EXTERNAL_STAGE_EXAMPLE",
        url="s3://titan-snowflake/",
        owner=TEST_ROLE,
    )
    cursor.execute(f"USE DATABASE {test_db}")
    cursor.execute("USE SCHEMA PUBLIC")
    cursor.execute(external_stage.create_sql(if_not_exists=True))

    result = safe_fetch(cursor, external_stage.urn)
    assert result is not None
    result = data_provider.remove_none_values(result)
    assert result == data_provider.remove_none_values(external_stage.to_dict())


@pytest.mark.requires_snowflake
def test_fetch_csv_file_format(cursor, test_db):
    csv_file_format = CSVFileFormat(
        name="CSV_FILE_FORMAT_EXAMPLE",
        owner=TEST_ROLE,
        field_delimiter="|",
        skip_header=1,
        null_if=["NULL", "null"],
        empty_field_as_null=True,
        compression="GZIP",
    )
    cursor.execute(f"USE DATABASE {test_db}")
    cursor.execute("USE SCHEMA PUBLIC")
    cursor.execute(csv_file_format.create_sql(if_not_exists=True))

    result = safe_fetch(cursor, csv_file_format.urn)
    assert result is not None
    result = data_provider.remove_none_values(result)
    assert result == data_provider.remove_none_values(csv_file_format.to_dict())
