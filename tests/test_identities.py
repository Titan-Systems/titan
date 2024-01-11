import pytest

from titan.privs import GlobalPriv
from titan import resources

resources = [
    {
        "test": "account",
        "resource_cls": resources.Account,
        "data": {
            "name": "SOMEACCOUNT",
        },
    },
    {
        "test": "alert",
        "resource_cls": resources.Alert,
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
        "resource_cls": resources.Database,
        "data": {
            "name": "SOMEDB",
            "owner": "SYSADMIN",
            "data_retention_time_in_days": 1,
            "max_data_extension_time_in_days": 14,
            "transient": False,
        },
    },
    {
        "test": "dynamic_table",
        "resource_cls": resources.DynamicTable,
        "data": {
            "name": "SOMEDT",
            "owner": "SYSADMIN",
            "target_lag": "42 minutes",
            "warehouse": "SOMEWH",
            "refresh_mode": "FULL",
            "initialize": "ON_CREATE",
            "as_": "SELECT * FROM tbl",
        },
    },
    {
        "test": "grant",
        "resource_cls": resources.Grant,
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
        "resource_cls": resources.JavascriptUDF,
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
        "resource_cls": resources.Role,
        "data": {"name": "SOMEROLE", "owner": "SYSADMIN"},
    },
    {
        "test": "role_grant",
        "resource_cls": resources.RoleGrant,
        "data": {"role": "SOMEROLE", "to_user": "SOMEUSER", "owner": "SYSADMIN"},
    },
    {
        "test": "schema",
        "resource_cls": resources.Schema,
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
        "resource_cls": resources.SharedDatabase,
        "data": {"name": "SOMESHARENAME", "owner": "ACCOUNTADMIN", "from_share": "SOMEACCOUNT.SOMESHARE"},
    },
    {
        "test": "python_stored_procedure",
        "resource_cls": resources.PythonStoredProcedure,
        "data": {
            "name": "MY_PYTHON_SPROC",
            "owner": "SYSADMIN",
            "args": [],
            "secure": True,
            "returns": "INT",
            "language": "PYTHON",
            "execute_as": "OWNER",
            "runtime_version": "3.8",
            "packages": ["snowflake-snowpark-python"],
            "handler": "main",
            "as_": "def main(_): return 42;",
            "copy_grants": True,
            "external_access_integrations": ["someint"],
        },
    },
    {
        "test": "python_udf",
        "resource_cls": resources.PythonUDF,
        "data": {
            "name": "MY_PYTHON_UDF",
            "owner": "SYSADMIN",
            "args": [],
            "secure": False,
            "returns": "INT",
            "language": "PYTHON",
            "runtime_version": "3.8",
            "packages": ["snowflake-snowpark-python"],
            "handler": "titan.foobar.help",
            "imports": ["pyparsing"],
        },
    },
    {
        "test": "table",
        "resource_cls": resources.Table,
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
        "resource_cls": resources.User,
        "data": {
            "name": "SOMEUSER",
            "owner": "USERADMIN",
            "display_name": "SOMEUSER",
            "login_name": "SOMEUSER",
            "must_change_password": True,
        },
    },
    {
        "test": "view",
        "resource_cls": resources.View,
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
    if hasattr(resource, "model_dump"):
        return resource.model_dump(mode="json", by_alias=True, exclude_none=True)
    else:
        return resource.to_dict(packed=True)


def test_data_identity(resource):
    instance = resource["resource_cls"](**resource["data"])
    assert dump(instance) == resource["data"]


def test_sql_identity(resource):
    instance = resource["resource_cls"](**resource["data"])
    sql = instance.create_sql()
    new = resource["resource_cls"].from_sql(sql)
    assert dump(new) == dump(instance)
