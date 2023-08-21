import uuid

import pytest
from titan.data_provider import DataProvider, remove_none_values
from titan.client import get_session
from titan.enums import Scope
from titan.identifiers import FQN
from titan.resources import Resource


resources = [
    {
        "resource_key": "alert",
        "setup_sql": [
            "CREATE WAREHOUSE {name}_wh",
            "CREATE ALERT {name} WAREHOUSE = {name}_wh SCHEDULE = '60 MINUTE' IF(EXISTS(SELECT 1)) THEN SELECT 1",
        ],
        "drop_sql": ["DROP ALERT {name}", "DROP WAREHOUSE {name}_wh"],
        "data": lambda name: {
            "name": name,
            "warehouse": f"{name}_WH",
            "schedule": "60 MINUTE",
            "condition": "SELECT 1",
            "then": "SELECT 1",
            "owner": "SYSADMIN",
        },
    },
    {
        "resource_key": "database",
        "setup_sql": "CREATE DATABASE {name}",
        "drop_sql": "DROP DATABASE {name}",
        "data": lambda name: {
            "name": name,
            "owner": "SYSADMIN",
            "data_retention_time_in_days": 1,
            "max_data_extension_time_in_days": 14,
            "transient": False,
        },
    },
    {
        "resource_key": "javascript_udf",
        "setup_sql": "CREATE FUNCTION {name}() RETURNS double LANGUAGE JAVASCRIPT AS 'return 42;'",
        "drop_sql": "DROP FUNCTION {name}()",
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
        "resource_key": "schema",
        "setup_sql": "CREATE TRANSIENT SCHEMA {name}",
        "drop_sql": "DROP SCHEMA {name}",
        "data": lambda name: {
            "name": name,
            "owner": "SYSADMIN",
            "data_retention_time_in_days": 1,
            "max_data_extension_time_in_days": 1,
            "transient": True,
            "with_managed_access": False,
        },
    },
]


def _generate_fqn(resource):
    resource_cls = Resource.classes[resource["resource_key"]]
    if resource_cls.scope == Scope.ACCOUNT:
        return FQN(name=resource["name"], resource_key=resource["resource_key"])
    elif resource_cls.scope == Scope.DATABASE:
        return FQN(name=resource["name"], database=resource["db"], resource_key=resource["resource_key"])
    elif resource_cls.scope == Scope.SCHEMA:
        return FQN(name=resource["name"], schema=resource["schema"], resource_key=resource["resource_key"])


@pytest.fixture(scope="session")
def db_session():
    yield get_session()


@pytest.fixture(
    params=resources,
    ids=[f"test_fetch_{config['resource_key']}" for config in resources],
    scope="function",
)
def resource(request, db_session):
    config = request.param
    suffix = str(uuid.uuid4())[:8]
    setup_sqls = config["setup_sql"] if isinstance(config["setup_sql"], list) else [config["setup_sql"]]
    drop_sqls = config["drop_sql"] if isinstance(config["drop_sql"], list) else [config["drop_sql"]]
    resource_name = f"test_{config['resource_key']}_{suffix}".upper()
    test_db = f"test_{suffix}__db".upper()
    test_schema = f"test_{suffix}__schema".upper()

    with db_session.cursor() as cur:
        cur.execute(f"ALTER SESSION set query_tag='titan_package:test::{suffix}'")
        cur.execute(f"CREATE DATABASE {test_db}")
        cur.execute(f"USE DATABASE {test_db}")
        cur.execute(f"CREATE SCHEMA {test_db}.{test_schema}")
        cur.execute(f"USE SCHEMA {test_db}.{test_schema}")
        for setup_sql in setup_sqls:
            cur.execute(setup_sql.format(name=resource_name))
        try:
            yield {"name": resource_name, "db": test_db, "schema": test_schema, **config}
        finally:
            for drop_sql in drop_sqls:
                cur.execute(drop_sql.format(name=resource_name))
            cur.execute(f"DROP SCHEMA {test_db}.{test_schema}")
            cur.execute(f"DROP DATABASE {test_db}")


def test_fetch_resource(resource, db_session):
    provider = DataProvider(db_session)
    data = resource["data"](resource["name"])
    fetch = getattr(provider, resource.get("fetch_method") or f"fetch_{resource['resource_key'].lower()}")
    fqn = _generate_fqn(resource)
    result = fetch(fqn)
    result = remove_none_values(result)
    assert result is not None
    assert result == data
