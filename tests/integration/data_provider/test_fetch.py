import uuid

import pytest
from titan.data_provider import DataProvider, remove_none_values
from titan.client import get_session
from titan.identifiers import FQN

resources = [
    {
        "resource_key": "database",
        "setup_sql": "CREATE DATABASE {name}",
        "drop_sql": "DROP DATABASE {name}",
        "fetch_method": "fetch_database",
        "fqn": lambda name: FQN.from_str(name, resource_key="database"),
        "data": lambda name: {
            "name": name,
            "owner": "SYSADMIN",
            "data_retention_time_in_days": 1,
            "max_data_extension_time_in_days": 14,
            "transient": False,
        },
    },
    {
        "resource_key": "schema",
        "setup_sql": ["CREATE DATABASE {name}_db", "CREATE TRANSIENT SCHEMA {name}"],
        "drop_sql": ["DROP SCHEMA {name}", "DROP DATABASE {name}_db"],
        "fetch_method": "fetch_schema",
        "fqn": lambda name: FQN(name=name, database=f"{name}_db", resource_key="schema"),
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


@pytest.fixture(scope="session")
def db_session():
    session = get_session()
    yield session
    session.close()


@pytest.fixture(
    params=resources,
    ids=[f"test_fetch_{config['resource_key']}" for config in resources],
    scope="function",
)
def resource(request, db_session):
    config = request.param
    setup_sql = [config["setup_sql"]] if isinstance(config["setup_sql"], str) else config["setup_sql"]
    drop_sql = [config["drop_sql"]] if isinstance(config["drop_sql"], str) else config["drop_sql"]
    suffix = str(uuid.uuid4())[:8]
    resource_name = f"test_{config['resource_key']}_{suffix}".upper()
    with db_session.cursor() as cur:
        for sql in setup_sql:
            cur.execute(sql.format(name=resource_name))
        try:
            yield {"name": resource_name, "config": config}
        finally:
            for sql in drop_sql:
                cur.execute(sql.format(name=resource_name))


def test_fetch_resource(resource, db_session):
    provider = DataProvider(db_session)

    data = resource["config"]["data"](resource["name"])
    fqn = resource["config"]["fqn"](resource["name"])

    fetch = getattr(provider, resource["config"]["fetch_method"])
    result = fetch(fqn)
    result = remove_none_values(result)

    assert result is not None
    assert result == data
