import os

import pytest

from tests.helpers import safe_fetch
from titan import data_provider
from titan import resources as res
from titan.enums import AccountEdition
from titan.resources import Resource
from titan.scope import AccountScope, DatabaseScope, SchemaScope

pytestmark = pytest.mark.requires_snowflake

TEST_ROLE = os.environ.get("TEST_SNOWFLAKE_ROLE", "ACCOUNTADMIN")
TEST_USER = os.environ.get("TEST_SNOWFLAKE_USER")


@pytest.fixture(scope="session")
def account_edition(cursor):
    session_ctx = data_provider.fetch_session(cursor.connection)
    return session_ctx["account_edition"]


@pytest.fixture(scope="session")
def email_address(cursor):
    user = cursor.execute(f"SHOW TERSE USERS LIKE '{TEST_USER}'").fetchone()
    return user["email"]


def strip_unfetchable_fields(spec, data: dict) -> dict:
    keys = set(data.keys())
    for attr in keys:
        attr_metadata = spec.get_metadata(attr)
        if not attr_metadata.fetchable or attr_metadata.known_after_apply:
            data.pop(attr, None)
    return data


def resource_fixtures() -> list:
    return [
        res.Alert(
            name="TEST_FETCH_ALERT",
            warehouse="STATIC_WAREHOUSE",
            schedule="60 MINUTE",
            condition="SELECT 1",
            then="SELECT 1",
            owner=TEST_ROLE,
        ),
        res.AuthenticationPolicy(
            name="TEST_FETCH_AUTHENTICATION_POLICY",
            mfa_authentication_methods=["PASSWORD", "SAML"],
            mfa_enrollment="REQUIRED",
            client_types=["SNOWFLAKE_UI"],
            security_integrations=["STATIC_SECURITY_INTEGRATION"],
            owner=TEST_ROLE,
        ),
        res.AzureStorageIntegration(
            name="TEST_FETCH_AZURE_STORAGE_INTEGRATION",
            enabled=True,
            azure_tenant_id="a123b4c5-1234-123a-a12b-1a23b45678c9",
            storage_allowed_locations=[
                "azure://myaccount.blob.core.windows.net/mycontainer/path1/",
                "azure://myaccount.blob.core.windows.net/mycontainer/path2/",
            ],
            owner=TEST_ROLE,
        ),
        res.CSVFileFormat(
            name="TEST_FETCH_CSV_FILE_FORMAT",
            owner=TEST_ROLE,
            field_delimiter="|",
            skip_header=1,
            null_if=["NULL", "null"],
            empty_field_as_null=True,
            compression="GZIP",
        ),
        res.Database(
            name="TEST_FETCH_DATABASE",
            owner=TEST_ROLE,
            transient=True,
            data_retention_time_in_days=1,
            max_data_extension_time_in_days=3,
            comment="This is a test database",
        ),
        res.DynamicTable(
            name="TEST_FETCH_DYNAMIC_TABLE",
            columns=[{"name": "ID", "comment": "This is a comment"}],
            target_lag="20 minutes",
            warehouse="STATIC_WAREHOUSE",
            refresh_mode="AUTO",
            initialize="ON_CREATE",
            comment="this is a comment",
            as_="SELECT id FROM STATIC_DATABASE.PUBLIC.STATIC_TABLE",
            owner=TEST_ROLE,
        ),
        res.EventTable(
            name="TEST_FETCH_EVENT_TABLE",
            change_tracking=True,
            cluster_by=["START_TIMESTAMP"],
            data_retention_time_in_days=1,
            owner=TEST_ROLE,
            comment="This is a test event table",
        ),
        res.ExternalAccessIntegration(
            name="TEST_FETCH_EXTERNAL_ACCESS_INTEGRATION",
            allowed_network_rules=["static_database.public.static_network_rule"],
            allowed_authentication_secrets=["static_database.public.static_secret"],
            enabled=True,
            comment="External access integration for testing",
            owner=TEST_ROLE,
        ),
        res.ExternalStage(
            name="TEST_FETCH_EXTERNAL_STAGE",
            url="s3://titan-snowflake/",
            owner=TEST_ROLE,
            directory={"enable": True},
            comment="This is a test external stage",
        ),
        res.GCSStorageIntegration(
            name="TEST_FETCH_GCS_STORAGE_INTEGRATION",
            enabled=True,
            storage_allowed_locations=["gcs://mybucket1/path1/", "gcs://mybucket2/path2/"],
            owner=TEST_ROLE,
        ),
        res.GlueCatalogIntegration(
            name="TEST_FETCH_GLUE_CATALOG_INTEGRATION",
            table_format="ICEBERG",
            glue_aws_role_arn="arn:aws:iam::123456789012:role/SnowflakeAccess",
            glue_catalog_id="123456789012",
            catalog_namespace="some_namespace",
            enabled=True,
            glue_region="us-west-2",
            comment="Integration for AWS Glue with Snowflake.",
            owner=TEST_ROLE,
        ),
        res.ImageRepository(
            name="TEST_FETCH_IMAGE_REPOSITORY",
            owner=TEST_ROLE,
        ),
        res.InternalStage(
            name="TEST_FETCH_INTERNAL_STAGE",
            directory={"enable": True},
            owner=TEST_ROLE,
            comment="This is a test internal stage",
        ),
        res.JavascriptUDF(
            name="SOME_JAVASCRIPT_UDF",
            args=[{"name": "INPUT_ARG", "data_type": "VARIANT"}],
            returns="FLOAT",
            volatility="VOLATILE",
            as_="return 42;",
            secure=False,
            owner=TEST_ROLE,
        ),
        res.JSONFileFormat(
            name="TEST_FETCH_JSON_FILE_FORMAT",
            owner=TEST_ROLE,
            compression="GZIP",
            replace_invalid_characters=True,
            comment="This is a test JSON file format",
        ),
        res.Notebook(
            name="TEST_FETCH_NOTEBOOK",
            query_warehouse="static_warehouse",
            comment="This is a test notebook",
            owner=TEST_ROLE,
        ),
        res.ObjectStoreCatalogIntegration(
            name="TEST_FETCH_OBJECT_STORE_CATALOG_INTEGRATION",
            catalog_source="OBJECT_STORE",
            table_format="ICEBERG",
            enabled=True,
            comment="Catalog integration for testing",
            owner=TEST_ROLE,
        ),
        res.PasswordPolicy(
            name="TEST_FETCH_PASSWORD_POLICY",
            password_min_length=12,
            password_max_length=24,
            password_min_upper_case_chars=2,
            password_min_lower_case_chars=2,
            password_min_numeric_chars=2,
            password_min_special_chars=2,
            password_min_age_days=1,
            password_max_age_days=30,
            password_max_retries=3,
            password_lockout_time_mins=30,
            password_history=5,
            # comment="production account password policy", # Leaving this out until Snowflake fixes their bugs
            owner=TEST_ROLE,
        ),
        res.PackagesPolicy(
            name="TEST_FETCH_PACKAGES_POLICY",
            allowlist=["numpy", "pandas"],
            blocklist=["os", "sys"],
            additional_creation_blocklist=["numpy.random.randint"],
            comment="Example packages policy",
            owner=TEST_ROLE,
        ),
        res.PythonStoredProcedure(
            name="TEST_FETCH_PYTHON_STORED_PROCEDURE",
            args=[{"name": "ARG1", "data_type": "VARCHAR"}],
            returns="NUMBER",
            packages=["snowflake-snowpark-python"],
            runtime_version="3.9",
            handler="main",
            execute_as="OWNER",
            comment="user-defined procedure",
            imports=[],
            null_handling="CALLED ON NULL INPUT",
            secure=False,
            owner=TEST_ROLE,
            as_="def main(arg1): return 42",
        ),
        res.ResourceMonitor(
            name="TEST_FETCH_RESOURCE_MONITOR",
            credit_quota=1000,
            start_timestamp="2049-01-01 00:00",
        ),
        res.Role(
            name="TEST_FETCH_ROLE",
            owner=TEST_ROLE,
        ),
        res.S3StorageIntegration(
            name="TEST_FETCH_S3_STORAGE_INTEGRATION",
            storage_provider="S3",
            storage_aws_role_arn="arn:aws:iam::001234567890:role/myrole",
            enabled=True,
            storage_allowed_locations=["s3://mybucket1/path1/", "s3://mybucket2/path2/"],
            owner=TEST_ROLE,
        ),
        res.Schema(
            name="TEST_FETCH_SCHEMA",
            owner=TEST_ROLE,
            transient=True,
            managed_access=True,
            comment="This is a test schema",
        ),
        res.Sequence(
            name="TEST_FETCH_SEQUENCE",
            start=1,
            increment=2,
            comment="+3",
            owner=TEST_ROLE,
        ),
        res.Share(
            name="TEST_FETCH_SHARE",
            comment="Share for testing",
            owner=TEST_ROLE,
        ),
        res.SnowflakeIcebergTable(
            name="TEST_FETCH_SNOWFLAKE_ICEBERG_TABLE",
            columns=[
                res.Column(name="ID", data_type="NUMBER(38,0)", not_null=True),
                res.Column(name="NAME", data_type="VARCHAR(16777216)", not_null=False),
            ],
            owner=TEST_ROLE,
            catalog="SNOWFLAKE",
            external_volume="static_external_volume",
            base_location="some_prefix",
        ),
        res.Table(
            name="TEST_FETCH_TABLE",
            columns=[
                res.Column(name="ID", data_type="NUMBER(38,0)", not_null=True),
                res.Column(name="NAME", data_type="VARCHAR(16777216)", not_null=False),
            ],
            owner=TEST_ROLE,
        ),
        res.Task(
            name="TEST_FETCH_TASK",
            schedule="60 MINUTE",
            state="SUSPENDED",
            as_="SELECT 1",
            owner=TEST_ROLE,
        ),
        res.User(
            name="TEST_FETCH_USER@applytitan.com",
            owner=TEST_ROLE,
            type="PERSON",
            password="hunter2",
            must_change_password=True,
            display_name="Test User",
            first_name="Test",
            middle_name="Q.",
            last_name="User",
            comment="This is a test user",
            default_warehouse="a_default_warehouse",
            days_to_expiry=30,
        ),
        res.View(
            name="TEST_FETCH_VIEW",
            as_="SELECT 1 as id FROM STATIC_DATABASE.PUBLIC.STATIC_TABLE",
            columns=[{"name": "ID", "data_type": "NUMBER(1,0)", "not_null": False}],
            comment="View for testing",
            owner=TEST_ROLE,
        ),
        res.Warehouse(
            name="TEST_FETCH_WAREHOUSE",
            warehouse_size="XSMALL",
            auto_suspend=60,
            auto_resume=True,
            owner=TEST_ROLE,
            comment="This is a test warehouse",
        ),
    ]


def create(cursor, resource: Resource, account_edition):
    sql = resource.create_sql(account_edition=account_edition)
    try:
        cursor.execute(sql)
    except Exception as err:
        raise Exception(f"Error creating resource: \nQuery: {err.query}\nMsg: {err.msg}") from err
    return resource


@pytest.fixture(
    params=resource_fixtures(),
    ids=[resource.__class__.__name__ for resource in resource_fixtures()],
    scope="function",
)
def resource_fixture(request, cursor, suffix):
    resource = request.param
    if isinstance(resource.scope, AccountScope):
        cursor.execute(resource.drop_sql(if_exists=True))
        yield resource
        cursor.execute(resource.drop_sql(if_exists=True))
    elif isinstance(resource.scope, DatabaseScope):
        db = res.Database(name=f"test_fetch_{resource.__class__.__name__}_{suffix}")
        cursor.execute(db.create_sql(if_not_exists=True))
        db.add(resource)
        yield resource
        cursor.execute(db.drop_sql(if_exists=True))
    elif isinstance(resource.scope, SchemaScope):
        db = res.Database(name=f"test_fetch_{resource.__class__.__name__}_{suffix}")
        cursor.execute(db.create_sql(if_not_exists=True))
        db.public_schema.add(resource)
        yield resource
        cursor.execute(db.drop_sql(if_exists=True))


def test_fetch(
    cursor,
    resource_fixture,
    account_edition,
):
    if account_edition not in resource_fixture.edition:
        pytest.skip(f"Skipping test for {resource_fixture.__class__.__name__} on {account_edition} edition")

    create(cursor, resource_fixture, account_edition)

    fetched = safe_fetch(cursor, resource_fixture.urn)
    assert fetched is not None
    fetched = resource_fixture.spec(**fetched).to_dict(account_edition)
    fetched = strip_unfetchable_fields(resource_fixture.spec, fetched)
    fixture = strip_unfetchable_fields(resource_fixture.spec, resource_fixture.to_dict(account_edition))

    if "columns" in fetched:
        fetched_columns = fetched["columns"]
        fixture_columns = fixture["columns"]
        assert len(fetched_columns) == len(fixture_columns)
        for fetched_column, fixture_column in zip(fetched_columns, fixture_columns):
            assert fetched_column == fixture_column

    assert fetched == fixture
