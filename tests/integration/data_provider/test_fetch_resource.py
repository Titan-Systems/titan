import os

import pytest
import snowflake.connector.errors

from tests.helpers import (
    assert_resource_dicts_eq_ignore_nulls,
    assert_resource_dicts_eq_ignore_nulls_and_unfetchable,
    clean_resource_data,
    safe_fetch,
)
from titan import data_provider
from titan import resources as res
from titan.client import UNSUPPORTED_FEATURE, reset_cache
from titan.enums import ResourceType
from titan.identifiers import URN, parse_FQN, parse_URN
from titan.resource_name import ResourceName
from titan.resources import Resource
from titan.resources.resource import ResourcePointer

pytestmark = pytest.mark.requires_snowflake

TEST_ROLE = os.environ.get("TEST_SNOWFLAKE_ROLE")
TEST_USER = os.environ.get("TEST_SNOWFLAKE_USER")


@pytest.fixture(scope="session")
def account_locator(cursor):
    reset_cache()
    return data_provider.fetch_account_locator(cursor)


@pytest.fixture(scope="session")
def email_address(cursor):
    user = cursor.execute(f"SHOW TERSE USERS LIKE '{TEST_USER}'").fetchone()
    return user["email"]


def create(cursor, resource: Resource):
    session_ctx = data_provider.fetch_session(cursor.connection)
    account_edition = session_ctx["account_edition"]
    sql = resource.create_sql(account_edition=account_edition, if_not_exists=True)
    try:
        cursor.execute(sql)
    except snowflake.connector.errors.ProgrammingError as err:
        if err.errno == UNSUPPORTED_FEATURE:
            pytest.skip(f"{resource.resource_type} is not supported")
        else:
            raise
    except Exception as err:
        raise Exception(f"Error creating resource: \nQuery: {err.query}\nMsg: {err.msg}") from err
    return resource


def test_fetch_privilege_grant(cursor, suffix, marked_for_cleanup):
    role = res.Role(name=f"grant_role_{suffix}")
    create(cursor, role)
    marked_for_cleanup.append(role)

    grant = res.Grant(priv="usage", on_type="database", on="STATIC_DATABASE", to=role)
    create(cursor, grant)

    result = safe_fetch(cursor, grant.urn)
    assert result is not None
    result = data_provider.remove_none_values(result)
    data = data_provider.remove_none_values(grant.to_dict())
    assert result == data


@pytest.mark.enterprise
def test_fetch_enterprise_schema(cursor, account_locator, test_db):

    urn = URN(
        resource_type=ResourceType.SCHEMA,
        fqn=parse_FQN(f"{test_db}.ENTERPRISE_TEST_SCHEMA", is_db_scoped=True),
        account_locator=account_locator,
    )
    cursor.execute(
        f"""
            CREATE SCHEMA {test_db}.ENTERPRISE_TEST_SCHEMA
                DATA_RETENTION_TIME_IN_DAYS = 90
                WITH TAG (STATIC_DATABASE.PUBLIC.STATIC_TAG = 'STATIC_TAG_VALUE')
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
        "owner": TEST_ROLE,
        "comment": None,
    }
    tag_ref = safe_fetch(
        cursor,
        URN(
            resource_type=ResourceType.TAG_REFERENCE,
            fqn=parse_FQN(f"{test_db}.ENTERPRISE_TEST_SCHEMA?domain=SCHEMA", is_db_scoped=True),
            account_locator=account_locator,
        ),
    )
    assert tag_ref is not None
    assert "STATIC_DATABASE.PUBLIC.STATIC_TAG" in tag_ref["tags"]
    assert tag_ref["tags"]["STATIC_DATABASE.PUBLIC.STATIC_TAG"] == "STATIC_TAG_VALUE"


def test_fetch_grant_on_account(cursor, suffix):
    role = res.Role(name=f"TEST_ACCOUNT_GRANTS_ROLE_{suffix}")
    create(cursor, role)
    cursor.execute(f"GRANT AUDIT ON ACCOUNT TO ROLE {role.name}")
    cursor.execute(f"GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO ROLE {role.name}")

    try:
        bind_service_urn = parse_URN(
            f"urn:::grant/GRANT?priv=BIND SERVICE ENDPOINT&on=account/ACCOUNT&to=role/{role.name}"
        )
        bind_service_grant = safe_fetch(cursor, bind_service_urn)
        assert bind_service_grant is not None
        assert bind_service_grant["priv"] == "BIND SERVICE ENDPOINT"
        assert bind_service_grant["on"] == "ACCOUNT"
        assert bind_service_grant["on_type"] == "ACCOUNT"
        assert bind_service_grant["to"] == role.name
        assert bind_service_grant["to_type"] == "ROLE"
        audit_urn = parse_URN(f"urn:::grant/GRANT?priv=AUDIT&on=account/ACCOUNT&to=role/{role.name}")
        audit_grant = safe_fetch(cursor, audit_urn)
        assert audit_grant is not None
        assert audit_grant["priv"] == "AUDIT"
        assert audit_grant["on"] == "ACCOUNT"
        assert audit_grant["on_type"] == "ACCOUNT"
        assert audit_grant["to"] == role.name
        assert audit_grant["to_type"] == "ROLE"
    finally:
        cursor.execute(role.drop_sql(if_exists=True))


def test_fetch_grant_all_on_resource(cursor):
    cursor.execute("GRANT ALL ON WAREHOUSE STATIC_WAREHOUSE TO ROLE STATIC_ROLE")
    grant_all_urn = parse_URN("urn:::grant/GRANT_ON_ALL?priv=ALL&on=warehouse/STATIC_WAREHOUSE&to=role/STATIC_ROLE")
    try:
        grant = safe_fetch(cursor, grant_all_urn)
        assert grant is not None
        assert grant["priv"] == "ALL"
        assert grant["on_type"] == "WAREHOUSE"
        assert grant["on"] == "STATIC_WAREHOUSE"
        assert grant["to"] == "STATIC_ROLE"
        assert grant["to_type"] == "ROLE"
        assert grant["owner"] == "SYSADMIN"
        assert grant["grant_option"] is False
        assert grant["_privs"] == ["APPLYBUDGET", "MODIFY", "MONITOR", "OPERATE", "USAGE"]

        cursor.execute("REVOKE MODIFY ON WAREHOUSE STATIC_WAREHOUSE FROM ROLE STATIC_ROLE")

        grant = safe_fetch(cursor, grant_all_urn)
        assert grant is not None
        assert "MODIFY" not in grant["_privs"]
    finally:
        cursor.execute("REVOKE ALL ON WAREHOUSE STATIC_WAREHOUSE FROM ROLE STATIC_ROLE")


def test_fetch_email_notification_integration(cursor, email_address, marked_for_cleanup):

    email_notification_integration = res.EmailNotificationIntegration(
        name="EMAIL_NOTIFICATION_INTEGRATION_EXAMPLE",
        enabled=True,
        allowed_recipients=[email_address],
        comment="Example email notification integration",
    )
    create(cursor, email_notification_integration)
    marked_for_cleanup.append(email_notification_integration)

    result = safe_fetch(cursor, email_notification_integration.urn)
    assert result is not None
    result = data_provider.remove_none_values(result)
    assert result == data_provider.remove_none_values(email_notification_integration.to_dict())


def test_fetch_grant_with_fully_qualified_ref(cursor, test_db, suffix, marked_for_cleanup):
    cursor.execute(f"USE DATABASE {test_db}")
    cursor.execute(f"CREATE SCHEMA if not exists {test_db}.my_schema")
    role = res.Role(name=f"test_role_grant_{suffix}")
    create(cursor, role)
    marked_for_cleanup.append(role)
    cursor.execute(f"GRANT USAGE ON SCHEMA {test_db}.my_schema TO ROLE {role.name}")
    grant = res.Grant.from_sql(f"GRANT USAGE ON SCHEMA {test_db}.my_schema TO ROLE {role.name}")
    grant._data.owner = ResourcePointer(name=TEST_ROLE, resource_type=ResourceType.ROLE)
    result = safe_fetch(cursor, grant.urn)
    assert result is not None
    result = data_provider.remove_none_values(result)
    result["on"] = ResourceName(result["on"])
    assert result == data_provider.remove_none_values(grant.to_dict())


def test_fetch_grant_with_quoted_ref(cursor, test_db, suffix, marked_for_cleanup):
    cursor.execute(f"USE DATABASE {test_db}")
    cursor.execute(f'CREATE SCHEMA if not exists {test_db}."This_is_A_quoted_schema"')
    role = res.Role(name=f"test_role_grant_quoted_{suffix}")
    create(cursor, role)
    marked_for_cleanup.append(role)
    cursor.execute(f'GRANT USAGE ON SCHEMA {test_db}."This_is_A_quoted_schema" TO ROLE {role.name}')
    grant = res.Grant.from_sql(f'GRANT USAGE ON SCHEMA {test_db}."This_is_A_quoted_schema" TO ROLE {role.name}')
    grant._data.owner = ResourcePointer(name=TEST_ROLE, resource_type=ResourceType.ROLE)
    result = safe_fetch(cursor, grant.urn)
    assert result is not None
    result = data_provider.remove_none_values(result)
    result["on"] = ResourceName(result["on"])
    assert result == data_provider.remove_none_values(grant.to_dict())


def test_fetch_pipe(cursor, test_db, marked_for_cleanup):
    pipe = res.Pipe(
        name="PIPE_EXAMPLE",
        as_="""
        COPY INTO pipe_destination
        FROM '@%pipe_destination'
        FILE_FORMAT = (TYPE = 'CSV');
        """,
        comment="Pipe for testing",
        owner=TEST_ROLE,
        database=test_db,
        schema="PUBLIC",
    )
    cursor.execute(f"CREATE TABLE {test_db}.PUBLIC.pipe_destination (id INT)")
    create(cursor, pipe)
    marked_for_cleanup.append(pipe)

    result = safe_fetch(cursor, pipe.urn)
    assert result is not None
    result = data_provider.remove_none_values(result)
    assert result == data_provider.remove_none_values(pipe.to_dict())


@pytest.mark.enterprise
def test_fetch_tag(cursor, test_db, marked_for_cleanup):
    tag = res.Tag(
        name="TAG_EXAMPLE_WITH_ALLOWED_VALUES",
        database=test_db,
        schema="PUBLIC",
        comment="Tag for testing",
        allowed_values=["SOME_VALUE"],
        owner=TEST_ROLE,
    )
    create(cursor, tag)
    marked_for_cleanup.append(tag)

    result = safe_fetch(cursor, tag.urn)
    assert result is not None
    result = data_provider.remove_none_values(result)
    assert result == data_provider.remove_none_values(tag.to_dict())

    tag = res.Tag(
        name="TAG_EXAMPLE",
        database=test_db,
        schema="PUBLIC",
        comment="Tag for testing",
        owner=TEST_ROLE,
    )
    create(cursor, tag)
    marked_for_cleanup.append(tag)

    result = safe_fetch(cursor, tag.urn)
    assert result is not None
    result = data_provider.remove_none_values(result)
    assert result == data_provider.remove_none_values(tag.to_dict())


def test_fetch_role_grant(cursor, suffix, marked_for_cleanup):
    parent = res.Role(name=f"PARENT_ROLE_{suffix}", owner=TEST_ROLE)
    child = res.Role(name=f"CHILD_ROLE_{suffix}", owner=TEST_ROLE)
    create(cursor, parent)
    create(cursor, child)
    marked_for_cleanup.append(parent)
    marked_for_cleanup.append(child)

    # Role-to-role grant
    grant = res.RoleGrant(role=child, to_role=parent)
    create(cursor, grant)

    result = safe_fetch(cursor, grant.urn)
    assert result is not None
    assert_resource_dicts_eq_ignore_nulls(result, grant.to_dict())

    user = res.User(name=f"ROLE_RECIPIENT_USER_{suffix}", owner=TEST_ROLE)
    create(cursor, user)
    marked_for_cleanup.append(user)

    # Role-to-user grant
    grant = res.RoleGrant(role=child, to_user=user)
    create(cursor, grant)
    result = safe_fetch(cursor, grant.urn)
    assert result is not None
    assert_resource_dicts_eq_ignore_nulls(result, grant.to_dict())


def test_fetch_network_rule(cursor, suffix, test_db, marked_for_cleanup):
    network_rule = res.NetworkRule(
        name=f"NETWORK_RULE_EXAMPLE_HOST_PORT_{suffix}",
        database=test_db,
        schema="PUBLIC",
        type="HOST_PORT",
        value_list=["example.com:443", "company.com"],
        mode="EGRESS",
        comment="Network rule for testing",
        owner=TEST_ROLE,
    )
    create(cursor, network_rule)
    marked_for_cleanup.append(network_rule)

    result = safe_fetch(cursor, network_rule.urn)
    assert result is not None
    assert_resource_dicts_eq_ignore_nulls(result, network_rule.to_dict())

    network_rule = res.NetworkRule(
        name=f"NETWORK_RULE_EXAMPLE_IPV4_{suffix}",
        database=test_db,
        schema="PUBLIC",
        type="IPV4",
        value_list=["1.1.1.1", "2.2.2.2"],
        mode="INGRESS",
        comment="Network rule for testing",
        owner=TEST_ROLE,
    )
    create(cursor, network_rule)
    marked_for_cleanup.append(network_rule)

    result = safe_fetch(cursor, network_rule.urn)
    assert result is not None
    assert_resource_dicts_eq_ignore_nulls(result, network_rule.to_dict())


def test_fetch_api_integration(cursor, suffix, marked_for_cleanup):
    api_integration = res.AWSAPIIntegration(
        name=f"API_INTEGRATION_EXAMPLE_{suffix}",
        api_provider="AWS_API_GATEWAY",
        api_aws_role_arn="arn:aws:iam::123456789012:role/MyRole",
        api_allowed_prefixes=["https://xyz.execute-api.us-west-2.amazonaws.com/production"],
        api_blocked_prefixes=["https://xyz.execute-api.us-west-2.amazonaws.com/test"],
        comment="Example API integration",
        enabled=False,
        owner=TEST_ROLE,
    )

    create(cursor, api_integration)
    marked_for_cleanup.append(api_integration)

    result = safe_fetch(cursor, api_integration.urn)
    assert result is not None
    result = clean_resource_data(res.AWSAPIIntegration.spec, result)
    data = clean_resource_data(res.AWSAPIIntegration.spec, api_integration.to_dict())
    assert result == data

    api_integration = res.AWSAPIIntegration(
        name=f"API_INTEGRATION_EXAMPLE_{suffix}_WITH_API_KEY",
        api_provider="AWS_API_GATEWAY",
        api_aws_role_arn="arn:aws:iam::123456789012:role/MyRole",
        api_allowed_prefixes=["https://xyz.execute-api.us-west-2.amazonaws.com/production"],
        api_blocked_prefixes=["https://xyz.execute-api.us-west-2.amazonaws.com/test"],
        api_key="api-123456789",
        comment="Example API integration",
        enabled=False,
        owner=TEST_ROLE,
    )

    create(cursor, api_integration)
    marked_for_cleanup.append(api_integration)

    result = safe_fetch(cursor, api_integration.urn)
    assert result is not None
    result = clean_resource_data(res.AWSAPIIntegration.spec, result)
    data = clean_resource_data(res.AWSAPIIntegration.spec, api_integration.to_dict())
    assert result == data


@pytest.mark.enterprise
def test_fetch_aggregation_policy(cursor, suffix, test_db, marked_for_cleanup):
    aggregation_policy = res.AggregationPolicy(
        name=f"AGGREGATION_POLICY_EXAMPLE_{suffix}",
        body="AGGREGATION_CONSTRAINT(MIN_GROUP_SIZE => 5)",
        owner=TEST_ROLE,
        database=test_db,
        schema="PUBLIC",
    )
    create(cursor, aggregation_policy)
    marked_for_cleanup.append(aggregation_policy)

    result = safe_fetch(cursor, aggregation_policy.urn)
    assert result is not None
    assert_resource_dicts_eq_ignore_nulls(result, aggregation_policy.to_dict())


def test_fetch_compute_pool(cursor, suffix, marked_for_cleanup):
    compute_pool = res.ComputePool(
        name=f"SOME_COMPUTE_POOL_{suffix}",
        min_nodes=1,
        max_nodes=1,
        instance_family="CPU_X64_XS",
        auto_resume=False,
        auto_suspend_secs=60,
        comment="Compute Pool comment",
        owner=TEST_ROLE,
    )
    create(cursor, compute_pool)
    marked_for_cleanup.append(compute_pool)

    result = safe_fetch(cursor, compute_pool.urn)
    assert result is not None
    result = clean_resource_data(res.ComputePool.spec, result)
    data = clean_resource_data(res.ComputePool.spec, compute_pool.to_dict())
    assert result == data


def test_fetch_password_secret(cursor, suffix, marked_for_cleanup):
    secret = res.PasswordSecret(
        name=f"PASSWORD_SECRET_EXAMPLE_{suffix}",
        username="my_username",
        password="my_password",
        comment="Password secret for accessing external database",
        owner=TEST_ROLE,
    )
    create(cursor, secret)
    marked_for_cleanup.append(secret)

    result = safe_fetch(cursor, secret.urn)
    assert result is not None
    assert_resource_dicts_eq_ignore_nulls_and_unfetchable(secret.spec, result, secret.to_dict())


def test_fetch_generic_secret(cursor, suffix, marked_for_cleanup):
    secret = res.GenericSecret(
        name=f"GENERIC_SECRET_EXAMPLE_{suffix}",
        secret_string="my_secret_string",
        comment="Generic secret for various purposes",
        owner=TEST_ROLE,
    )
    create(cursor, secret)
    marked_for_cleanup.append(secret)

    result = safe_fetch(cursor, secret.urn)
    assert result is not None
    assert_resource_dicts_eq_ignore_nulls_and_unfetchable(secret.spec, result, secret.to_dict())


def test_fetch_oauth_secret(cursor, suffix, marked_for_cleanup):
    secret = res.OAuthSecret(
        name=f"OAUTH_SECRET_EXAMPLE_WITH_SCOPES_{suffix}",
        api_authentication="STATIC_SECURITY_INTEGRATION",
        comment="OAuth secret for accessing external API",
        owner=TEST_ROLE,
    )
    create(cursor, secret)
    marked_for_cleanup.append(secret)

    result = safe_fetch(cursor, secret.urn)
    assert result is not None
    assert_resource_dicts_eq_ignore_nulls_and_unfetchable(secret.spec, result, secret.to_dict())

    secret = res.OAuthSecret(
        name=f"OAUTH_SECRET_EXAMPLE_WITH_TOKEN_{suffix}",
        api_authentication="STATIC_SECURITY_INTEGRATION",
        oauth_refresh_token="my_refresh_token",
        oauth_refresh_token_expiry_time="2049-01-06 20:00:00",
        comment="OAuth secret for accessing external API",
        owner=TEST_ROLE,
    )
    create(cursor, secret)
    marked_for_cleanup.append(secret)

    result = safe_fetch(cursor, secret.urn)
    assert result is not None
    assert_resource_dicts_eq_ignore_nulls_and_unfetchable(secret.spec, result, secret.to_dict())


@pytest.mark.skip("Seeing weirdness with Snowflake")
def test_fetch_snowservices_oauth_security_integration(cursor, suffix, marked_for_cleanup):
    security_integration = res.SnowservicesOAuthSecurityIntegration(
        name=f"SNOWSERVICES_INGRESS_OAUTH_{suffix}",
        type="OAUTH",
        oauth_client="snowservices_ingress",
        enabled=True,
    )
    create(cursor, security_integration)
    marked_for_cleanup.append(security_integration)

    result = safe_fetch(cursor, security_integration.urn)
    assert result is not None
    assert_resource_dicts_eq_ignore_nulls(result, security_integration.to_dict())


def test_fetch_api_authentication_security_integration(cursor, suffix, marked_for_cleanup):
    security_integration = res.APIAuthenticationSecurityIntegration(
        name=f"API_AUTHENTICATION_SECURITY_INTEGRATION_{suffix}",
        type="api_authentication",
        auth_type="OAUTH2",
        oauth_client_id="sn-oauth-134o9erqfedlc",
        oauth_client_secret="eb9vaXsrcEvrFdfcvCaoijhilj4fc",
        oauth_token_endpoint="https://myinstance.service-now.com/oauth_token.do",
        enabled=True,
    )
    create(cursor, security_integration)
    marked_for_cleanup.append(security_integration)

    result = safe_fetch(cursor, security_integration.urn)
    assert result is not None
    result = clean_resource_data(res.APIAuthenticationSecurityIntegration.spec, result)
    data = clean_resource_data(res.APIAuthenticationSecurityIntegration.spec, security_integration.to_dict())
    assert result == data


def test_fetch_table_stream(cursor, suffix, marked_for_cleanup):
    stream = res.TableStream(
        name=f"SOME_TABLE_STREAM_{suffix}",
        on_table="STATIC_DATABASE.PUBLIC.STATIC_TABLE",
        copy_grants=None,
        before=None,
        append_only=False,
        show_initial_rows=None,
        comment=None,
        owner=TEST_ROLE,
    )
    create(cursor, stream)
    marked_for_cleanup.append(stream)

    result = safe_fetch(cursor, stream.urn)
    assert result is not None
    assert_resource_dicts_eq_ignore_nulls_and_unfetchable(res.TableStream.spec, result, stream.to_dict())


def test_fetch_view_stream(cursor, suffix, marked_for_cleanup):
    stream = res.ViewStream(
        name=f"SOME_VIEW_STREAM_{suffix}",
        on_view="STATIC_DATABASE.PUBLIC.STATIC_VIEW",
        copy_grants=None,
        before=None,
        append_only=False,
        show_initial_rows=None,
        comment=None,
        owner=TEST_ROLE,
    )
    create(cursor, stream)
    marked_for_cleanup.append(stream)

    result = safe_fetch(cursor, stream.urn)
    assert result is not None
    assert_resource_dicts_eq_ignore_nulls_and_unfetchable(res.ViewStream.spec, result, stream.to_dict())


@pytest.mark.skip("Snowflake doesnt return the fully qualified stage name")
def test_fetch_stage_stream(cursor, suffix, marked_for_cleanup):
    stream = res.StageStream(
        name=f"SOME_STAGE_STREAM_{suffix}",
        on_stage="STATIC_DATABASE.PUBLIC.STATIC_STAGE",
        copy_grants=None,
        comment=None,
        owner=TEST_ROLE,
    )
    create(cursor, stream)
    marked_for_cleanup.append(stream)

    result = safe_fetch(cursor, stream.urn)
    assert result is not None
    assert_resource_dicts_eq_ignore_nulls_and_unfetchable(res.StageStream.spec, result, stream.to_dict())


def test_fetch_parquet_file_format(cursor, suffix, marked_for_cleanup):
    file_format = res.ParquetFileFormat(
        name=f"SOME_PARQUET_FILE_FORMAT_{suffix}",
        compression="SNAPPY",
        owner=TEST_ROLE,
    )
    create(cursor, file_format)
    marked_for_cleanup.append(file_format)

    result = safe_fetch(cursor, file_format.urn)
    assert result is not None
    result = clean_resource_data(res.ParquetFileFormat.spec, result)
    data = clean_resource_data(res.ParquetFileFormat.spec, file_format.to_dict())
    assert result == data


def test_fetch_network_policy(cursor, suffix, marked_for_cleanup):
    policy = res.NetworkPolicy(
        name=f"SOME_NETWORK_POLICY_{suffix}",
        allowed_network_rule_list=["static_database.public.static_network_rule"],
        blocked_network_rule_list=["static_database.public.static_network_rule"],
        allowed_ip_list=["1.1.1.1", "2.2.2.2"],
        blocked_ip_list=["3.3.3.3", "4.4.4.4"],
        comment="Network policy for testing",
        owner=TEST_ROLE,
    )
    create(cursor, policy)
    marked_for_cleanup.append(policy)

    result = safe_fetch(cursor, policy.urn)
    assert result is not None
    result = clean_resource_data(res.NetworkPolicy.spec, result)
    data = clean_resource_data(res.NetworkPolicy.spec, policy.to_dict())
    assert result == data


def test_fetch_external_volume(cursor, suffix, marked_for_cleanup):
    from titan.resources.external_volume import ExternalVolumeStorageLocation

    volume = res.ExternalVolume(
        name=f"SOME_EXTERNAL_VOLUME_{suffix}",
        owner=TEST_ROLE,
        storage_locations=[
            {
                "name": "my-s3-us-east-2-a",
                "storage_provider": "S3",
                "storage_base_url": "s3://s3-bucket/",
                "storage_aws_role_arn": "arn:aws:iam::12345678:role/role-name",
                "storage_aws_external_id": "external-id",
                "encryption": {"type": "AWS_SSE_S3"},
            }
        ],
    )
    create(cursor, volume)
    marked_for_cleanup.append(volume)

    result = safe_fetch(cursor, volume.urn)
    assert result is not None
    result = clean_resource_data(res.ExternalVolume.spec, result)
    data = clean_resource_data(res.ExternalVolume.spec, volume.to_dict())
    result_storage_locations = result.pop("storage_locations")
    data_storage_locations = data.pop("storage_locations")
    assert len(result_storage_locations) == len(data_storage_locations)
    assert clean_resource_data(ExternalVolumeStorageLocation.spec, result_storage_locations[0]) == clean_resource_data(
        ExternalVolumeStorageLocation.spec, data_storage_locations[0]
    )
    assert result == data


def test_fetch_task(cursor, suffix, marked_for_cleanup):
    task = res.Task(
        name=f"TEST_FETCH_TASK_SERVERLESS_{suffix}",
        schedule="60 MINUTE",
        state="SUSPENDED",
        as_="SELECT 1",
        owner=TEST_ROLE,
        comment="This is a test task",
        allow_overlapping_execution=True,
        user_task_managed_initial_warehouse_size="XSMALL",
        user_task_timeout_ms=1000,
        suspend_task_after_num_failures=1,
        config='{"output_dir": "/temp/test_directory/", "learning_rate": 0.1}',
        database="STATIC_DATABASE",
        schema="PUBLIC",
    )
    create(cursor, task)
    marked_for_cleanup.append(task)

    result = safe_fetch(cursor, task.urn)
    assert result is not None
    result = clean_resource_data(res.Task.spec, result)
    data = clean_resource_data(res.Task.spec, task.to_dict())
    assert result == data

    task = res.Task(
        name=f"TEST_FETCH_TASK_WAREHOUSE_{suffix}",
        schedule="60 MINUTE",
        state="SUSPENDED",
        as_="SELECT 1",
        owner=TEST_ROLE,
        comment="This is a test task",
        allow_overlapping_execution=False,
        warehouse="STATIC_WAREHOUSE",
        suspend_task_after_num_failures=1,
        config='{"output_dir": "/temp/test_directory/", "learning_rate": 0.1}',
        database="STATIC_DATABASE",
        schema="PUBLIC",
    )
    create(cursor, task)
    marked_for_cleanup.append(task)

    result = safe_fetch(cursor, task.urn)
    assert result is not None
    result = clean_resource_data(res.Task.spec, result)
    data = clean_resource_data(res.Task.spec, task.to_dict())
    assert result == data


def test_fetch_task_trailing_whitespace(cursor, suffix, marked_for_cleanup):
    task = res.Task(
        name=f"TEST_FETCH_TASK_TRAILING_WHITESPACE_{suffix}",
        schedule="60 MINUTE",
        state="SUSPENDED",
        as_="SELECT 1\n\n\t\t  ",
        owner=TEST_ROLE,
        database="STATIC_DATABASE",
        schema="PUBLIC",
    )
    create(cursor, task)
    marked_for_cleanup.append(task)

    result = safe_fetch(cursor, task.urn)
    assert result is not None
    result = clean_resource_data(res.Task.spec, result)
    data = clean_resource_data(res.Task.spec, task.to_dict())
    assert result == data


def test_fetch_task_predecessor(cursor, suffix, marked_for_cleanup):
    parent_task = res.Task(
        name=f"TEST_FETCH_TASK_PARENT_{suffix}",
        state="SUSPENDED",
        as_="SELECT 1",
        owner=TEST_ROLE,
        database="STATIC_DATABASE",
        schema="PUBLIC",
    )
    child_task = res.Task(
        name=f"TEST_FETCH_TASK_CHILD_{suffix}",
        state="SUSPENDED",
        as_="SELECT 1",
        owner=TEST_ROLE,
        database="STATIC_DATABASE",
        schema="PUBLIC",
        after=[str(parent_task.fqn)],
    )
    create(cursor, parent_task)
    create(cursor, child_task)
    marked_for_cleanup.append(parent_task)
    marked_for_cleanup.append(child_task)

    result = safe_fetch(cursor, child_task.urn)
    assert result is not None
    result = clean_resource_data(res.Task.spec, result)
    data = clean_resource_data(res.Task.spec, child_task.to_dict())
    assert result == data


def test_fetch_database_role_grant(cursor, suffix, marked_for_cleanup):
    role = res.DatabaseRole(name=f"TEST_FETCH_DATABASE_ROLE_GRANT_{suffix}", database="STATIC_DATABASE")
    create(cursor, role)
    marked_for_cleanup.append(role)

    grant = res.Grant(priv="USAGE", on_schema="STATIC_DATABASE.PUBLIC", to=role)
    create(cursor, grant)

    result = safe_fetch(cursor, grant.urn)
    assert result is not None
    result = clean_resource_data(res.Grant.spec, result)
    data = clean_resource_data(res.Grant.spec, grant.to_dict())
    assert result == data


def test_fetch_database_role(cursor, suffix, marked_for_cleanup):
    role = res.DatabaseRole(
        name=f"TEST_FETCH_DATABASE_ROLE_{suffix}",
        database="STATIC_DATABASE",
        owner=TEST_ROLE,
    )
    create(cursor, role)
    marked_for_cleanup.append(role)

    result = safe_fetch(cursor, role.urn)
    assert result is not None
    result = clean_resource_data(res.DatabaseRole.spec, result)
    data = clean_resource_data(res.DatabaseRole.spec, role.to_dict())
    assert result == data


def test_fetch_grant_of_database_role(cursor, suffix, marked_for_cleanup):
    db_role = res.DatabaseRole(
        name=f"TEST_FETCH_GRANT_OF_DATABASE_ROLE_{suffix}",
        database="STATIC_DATABASE",
        owner=TEST_ROLE,
    )
    create(cursor, db_role)
    marked_for_cleanup.append(db_role)

    role = res.Role(name=f"TEST_FETCH_GRANT_OF_DATABASE_ROLE_{suffix}", owner=TEST_ROLE)
    create(cursor, role)
    marked_for_cleanup.append(role)

    grant = res.DatabaseRoleGrant(database_role=db_role, to_role=role)
    create(cursor, grant)

    result = safe_fetch(cursor, grant.urn)
    assert result is not None
    result = clean_resource_data(res.DatabaseRoleGrant.spec, result)
    data = clean_resource_data(res.DatabaseRoleGrant.spec, grant.to_dict())
    assert result == data


def test_fetch_masking_policy(cursor, suffix, marked_for_cleanup):
    policy = res.MaskingPolicy(
        name=f"TEST_FETCH_MASKING_POLICY_{suffix}",
        args=[{"name": "val", "data_type": "STRING"}],
        returns="STRING",
        body="CASE WHEN current_role() IN ('ANALYST') THEN VAL ELSE '*********' END",
        comment="Masks email addresses",
        owner=TEST_ROLE,
    )
    create(cursor, policy)
    marked_for_cleanup.append(policy)

    result = safe_fetch(cursor, policy.urn)
    assert result is not None
    result = clean_resource_data(res.MaskingPolicy.spec, result)
    data = clean_resource_data(res.MaskingPolicy.spec, policy.to_dict())
    assert result == data
