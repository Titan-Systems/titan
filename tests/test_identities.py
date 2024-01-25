import pytest

from titan.privs import GlobalPriv
from titan import resources, Resource

resources = [
    {
        "test": "account",
        "resource_cls": resources.Account,
        "data": {
            "name": "SOMEACCOUNT",
            "locator": "ABC123",
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
        "test": "api_integration",
        "resource_cls": resources.APIIntegration,
        "data": {
            "name": "SOMEINT",
            "owner": "ACCOUNTADMIN",
            "api_provider": "AWS_API_GATEWAY",
            "api_key": "api-987654321",
            "api_aws_role_arn": "arn:aws:iam::123456789012:role/my_cloud_account_role",
            "api_allowed_prefixes": ["https://xyz.execute-api.us-west-2.amazonaws.com/production"],
            "api_blocked_prefixes": ["https://xyz.execute-api.us-west-2.amazonaws.com/development"],
            "enabled": True,
        },
    },
    {
        "test": "database",
        "resource_cls": resources.Database,
        "data": {
            "name": "SOMEDB",
            "owner": "SYSADMIN",
            "data_retention_time_in_days": 2,
            "max_data_extension_time_in_days": 13,
            "transient": True,
        },
    },
    {
        "test": "dynamic_table",
        "resource_cls": resources.DynamicTable,
        "data": {
            "name": "SOMEDT",
            "owner": "SYSADMIN",
            "columns": [{"name": "ID", "data_type": "INT"}],
            "target_lag": "42 minutes",
            "warehouse": "SOMEWH",
            "refresh_mode": "FULL",
            "initialize": "ON_CREATE",
            "as_": "SELECT id FROM tbl",
        },
    },
    {
        "test": "external_function",
        "resource_cls": resources.ExternalFunction,
        "data": {
            "name": "SOMEFUNC",
            "owner": "SYSADMIN",
            "returns": "VARIANT",
            "api_integration": "someint",
            "as_": "https://xyz.execute-api.us-west-2.amazonaws.com/prod/remote_echo",
            "secure": True,
            "args": [
                {"name": "string_col", "data_type": "VARCHAR"},
                {"name": "somesuch", "data_type": "INTEGER"},
            ],
            "null_handling": "RETURNS NULL ON NULL INPUT",
            "volatility": "IMMUTABLE",
            "comment": "external function comment",
            "headers": {"volume-measure": "liters", "distance-measure": "kilometers"},
            "max_batch_rows": 42,
            "compression": "DEFLATE",
            "request_translator": '"DB"."SCHEMA".function',
            "response_translator": '"DB"."SCHEMA".function',
        },
    },
    {
        "test": "failover_group",
        "resource_cls": resources.FailoverGroup,
        "data": {
            "name": "SOMEFG",
            "owner": "ACCOUNTADMIN",
            "object_types": ["NETWORK POLICIES", "WAREHOUSES"],
            "allowed_accounts": ["SOMEORG.SOMEACCOUNT"],
            "allowed_databases": ["SOMEDB"],
            "allowed_shares": ["SOMESHARE"],
            "allowed_integration_types": ["SECURITY INTEGRATIONS", "API INTEGRATIONS"],
            "ignore_edition_check": True,
            "replication_schedule": "15 MINUTE",
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
            "grant_option": True,
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
            "volatility": "VOLATILE",
            "as_": "return 42;",
        },
    },
    {
        "test": "email_notification_integration",
        "resource_cls": resources.EmailNotificationIntegration,
        "data": {
            "name": "SOMEINT",
            "owner": "ACCOUNTADMIN",
            "enabled": True,
            "allowed_recipients": ["first.last@example.com", "first2.last2@example.com"],
        },
    },
    {
        "test": "external_stage",
        "resource_cls": resources.ExternalStage,
        "data": {
            "name": "SOMESTAGE",
            "owner": "SYSADMIN",
            "url": "s3://bucket/path/",
            "storage_integration": "someint",
            "encryption": {
                "type": "AWS_CSE",
                "master_key": "arn:aws:kms:us-west-2:123456789012:key/12345678-1234-1234-1234-123456789012",
            },
            "file_format": "somefmt",
        },
    },
    {
        "test": "internal_stage",
        "resource_cls": resources.InternalStage,
        "data": {
            "name": "SOMESTAGE",
            "owner": "SYSADMIN",
            "copy_options": {
                "on_error": "skip_file",
                "force": True,
                "enforce_length": True,
                "match_by_column_name": "CASE_INSENSITIVE",
            },
            "file_format": {"type": "JSON"},
        },
    },
    {
        "test": "network_rule",
        "resource_cls": resources.NetworkRule,
        "data": {
            "name": "SOMERULE",
            "owner": "SYSADMIN",
            "type": "AWSVPCEID",
            "value_list": ["vpce-1234567890abcdef0"],
            "mode": "INTERNAL_STAGE",
            "comment": "corporate privatelink endpoint",
        },
    },
    {
        "test": "password_policy",
        "resource_cls": resources.PasswordPolicy,
        "data": {
            "name": "SOMEPOLICY",
            "owner": "SYSADMIN",
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
        "test": "pipe",
        "resource_cls": resources.Pipe,
        "data": {
            "name": "SOMEPIPE",
            "owner": "SYSADMIN",
            "auto_ingest": True,
            "error_integration": "someint",
            "aws_sns_topic": "sometopic",
            "integration": "someint",
            "comment": "mario",
            "as_": "copy into mytable(C1, C2) from (select $5, $4 from @mystage)",
        },
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
            "returns": "INT",
            "runtime_version": "3.8",
            "packages": ["snowflake-snowpark-python"],
            "handler": "titan.foobar.help",
            "imports": ["pyparsing"],
        },
    },
    {
        "test": "role",
        "resource_cls": resources.Role,
        "data": {"name": "SOMEROLE", "owner": "SYSADMIN"},
    },
    {
        "test": "role_grant_to_user",
        "resource_cls": resources.RoleGrant,
        "data": {
            "role": "SOMEROLE",
            "to_user": "SOMEUSER",
            "owner": "USERADMIN",
        },
    },
    {
        "test": "role_grant_to_role",
        "resource_cls": resources.RoleGrant,
        "data": {
            "role": "SOMEROLE",
            "to_role": "ANOTHERROLE",
            "owner": "USERADMIN",
        },
    },
    {
        "test": "schema",
        "resource_cls": resources.Schema,
        "data": {
            "name": "SOMESCHEMA",
            "owner": "SYSADMIN",
            "transient": True,
            "max_data_extension_time_in_days": 7,
        },
    },
    {
        "test": "sequence",
        "resource_cls": resources.Sequence,
        "data": {
            "name": "SOMESEQ",
            "owner": "SYSADMIN",
            "start": 1,
            "increment": 2,
            "comment": "+3",
        },
    },
    # {
    #     "test": "shared_database",
    #     "resource_cls": resources.SharedDatabase,
    #     "data": {
    #         "name": "SOMESHARENAME",
    #         "owner": "ACCOUNTADMIN",
    #         "from_share": "SOMEACCOUNT.SOMESHARE",
    #     },
    # },
    {
        "test": "s3_storage_integration",
        "resource_cls": resources.S3StorageIntegration,
        "data": {
            "name": "S3_INT",
            "owner": "ACCOUNTADMIN",
            "storage_aws_role_arn": "arn:aws:iam::123456789012:role/my_cloud_account_role",
            "storage_allowed_locations": ["s3://bucket/prod/"],
            "storage_blocked_locations": ["s3://bucket/dev/"],
            "enabled": True,
        },
    },
    {
        "test": "table",
        "resource_cls": resources.Table,
        "data": {
            "name": "SOMETABLE",
            "owner": "SYSADMIN",
            "columns": [{"data_type": "INT", "name": "ID"}],
            "change_tracking": True,
            "cluster_by": ["ID"],
            "copy_grants": True,
            "enable_schema_evolution": True,
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


def dump(resource: Resource):
    return resource.to_dict(packed=True)


def test_data_identity(resource):
    instance = resource["resource_cls"](**resource["data"])
    assert dump(instance) == resource["data"]


def test_sql_identity(resource):
    instance = resource["resource_cls"](**resource["data"])
    sql = instance.create_sql()
    new = resource["resource_cls"].from_sql(sql)
    assert dump(new) == dump(instance)
