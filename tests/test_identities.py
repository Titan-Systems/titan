import pytest

from titan.privs import GlobalPriv
from titan.resources import (
    Account,
    Alert,
    Database,
    Grant,
    JavascriptUDF,
    Role,
    RoleGrant,
    Schema,
    SharedDatabase,
    Table,
    User,
    View,
)

resources = [
    {
        "test": "account",
        "resource_cls": Account,
        "data": {
            "name": "SOMEACCOUNT",
        },
    },
    {
        "test": "alert",
        "resource_cls": Alert,
        "data": {
            "name": "ALERT",
            "owner": "SYSADMIN",
            "warehouse": "wh",
            "schedule": "1 minute",
            "condition": "SELECT 1",
            "then": "INSERT INTO foo VALUES(1)",
        },
    },
    {
        "test": "database",
        "resource_cls": Database,
        "data": {
            "name": "SOMEDB",
            "owner": "SYSADMIN",
            "data_retention_time_in_days": 1,
            "max_data_extension_time_in_days": 14,
            "transient": False,
        },
    },
    {
        "test": "grant",
        "resource_cls": Grant,
        "data": {
            "priv": GlobalPriv.CREATE_DATABASE.value,
            "to": "SOMEROLE",
            "owner": "SYSADMIN",
            "on": "ACCOUNT",
            "grant_option": False,
        },
    },
    {
        "test": "javascript_udf",
        "resource_cls": JavascriptUDF,
        "data": {
            "name": "NOOP",
            "owner": "SYSADMIN",
            "args": [],
            "secure": False,
            "returns": "FLOAT",
            "language": "JAVASCRIPT",
            "volatility": "VOLATILE",
            "as_": "return 42;",
        },
    },
    {
        "test": "role",
        "resource_cls": Role,
        "data": {"name": "SOMEROLE", "owner": "SYSADMIN"},
    },
    {
        "test": "role_grant",
        "resource_cls": RoleGrant,
        "data": {"role": "SOMEROLE", "to_user": "SOMEUSER", "owner": "SYSADMIN"},
    },
    {
        "test": "schema",
        "resource_cls": Schema,
        "data": {
            "name": "SOMESCHEMA",
            "owner": "SYSADMIN",
            "transient": False,
            "managed_access": False,
            "max_data_extension_time_in_days": 14,
        },
    },
    {
        "test": "shared_database",
        "resource_cls": SharedDatabase,
        "data": {"name": "SOMESHARENAME", "owner": "ACCOUNTADMIN", "from_share": "SOMEACCOUNT.SOMESHARE"},
    },
    {
        "test": "table",
        "resource_cls": Table,
        "data": {
            "name": "SOMETABLE",
            "owner": "SYSADMIN",
            "columns": [{"data_type": "INT", "name": "ID"}],
            "change_tracking": False,
            "cluster_by": [],
            "copy_grants": False,
            "enable_schema_evolution": False,
            "transient": False,
            "volatile": False,
        },
    },
    {
        "test": "user",
        "resource_cls": User,
        "data": {
            "name": "SOMEUSER",
            "owner": "USERADMIN",
            "disabled": False,
            "display_name": "SOMEUSER",
            "login_name": "SOMEUSER",
            "must_change_password": False,
        },
    },
    {
        "test": "view",
        "resource_cls": View,
        "data": {"name": "MY_VIEW", "owner": "SYSADMIN", "volatile": True, "as_": "SELECT * FROM tbl"},
    },
]


@pytest.fixture(
    params=resources,
    ids=[f"test_{config['test']}" for config in resources],
    scope="function",
)
def resource(request):
    resource = request.param
    yield resource


def dump(resource):
    return resource.model_dump(mode="json", by_alias=True, exclude_none=True)


def test_data_identity(resource):
    instance = resource["resource_cls"](**resource["data"])
    assert dump(instance) == resource["data"]


def test_sql_identity(resource):
    instance = resource["resource_cls"](**resource["data"])
    sql = instance.create_sql()
    new = resource["resource_cls"].from_sql(sql)
    assert new == instance
