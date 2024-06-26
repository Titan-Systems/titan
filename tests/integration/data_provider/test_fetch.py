import os
import pytest

from titan import data_provider
from titan import resources as res
from titan.client import reset_cache
from titan.enums import ResourceType
from titan.identifiers import FQN, URN
from titan.parse import parse_identifier, parse_URN
from titan.resource_name import ResourceName


pytestmark = pytest.mark.requires_snowflake

TEST_ROLE = os.environ.get("TEST_SNOWFLAKE_ROLE")
TEST_USER = os.environ.get("TEST_SNOWFLAKE_USER")


def _assert_resource_dicts_eq_ignore_nulls(lhs: dict, rhs: dict) -> None:
    assert data_provider.remove_none_values(lhs) == data_provider.remove_none_values(rhs)


def _assert_resource_dicts_eq_ignore_nulls_and_unfetchable(spec, lhs: dict, rhs: dict) -> None:
    lhs = data_provider.remove_none_values(lhs)
    rhs = data_provider.remove_none_values(rhs)
    keys = set(lhs.keys()) | set(rhs.keys())
    for attr in keys:
        attr_metadata = spec.get_metadata(attr)
        if not attr_metadata.get("fetchable", True):
            lhs.pop(attr, None)
            rhs.pop(attr, None)
    assert lhs == rhs


def safe_fetch(cursor, urn):
    reset_cache()
    return data_provider.fetch_resource(cursor, urn)


@pytest.fixture(scope="session")
def account_locator(cursor):
    reset_cache()
    return data_provider.fetch_account_locator(cursor)


@pytest.fixture(scope="session")
def email_address(cursor):
    user = cursor.execute(f"SHOW TERSE USERS LIKE '{TEST_USER}'").fetchone()
    return user["email"]


def test_fetch_privilege_grant(cursor, suffix, marked_for_cleanup):
    role = res.Role(name=f"grant_role_{suffix}")
    cursor.execute(role.create_sql(if_not_exists=True))
    marked_for_cleanup.append(role)

    grant = res.Grant(priv="usage", on_type="database", on="STATIC_DATABASE", to=role)
    cursor.execute(grant.create_sql(if_not_exists=True))

    result = safe_fetch(cursor, grant.urn)
    assert result is not None
    _assert_resource_dicts_eq_ignore_nulls(result, grant.to_dict())


def test_fetch_future_grant(cursor, suffix, marked_for_cleanup):
    role = res.Role(name=f"future_grant_role_{suffix}")
    marked_for_cleanup.append(role)
    cursor.execute(role.create_sql(if_not_exists=True))

    future_grant = res.FutureGrant(priv="usage", to=role, on_future_schemas_in_database="STATIC_DATABASE")
    cursor.execute(future_grant.create_sql(if_not_exists=True))

    result = safe_fetch(cursor, future_grant.urn)
    assert result is not None
    _assert_resource_dicts_eq_ignore_nulls(result, future_grant.to_dict())

    # TODO: support hoisting the schema database into the future grant
    # schema = res.Schema(name="PUBLIC", database="STATIC_DATABASE")
    # future_grant = res.FutureGrant(priv="SELECT", to=role, on_future_tables_in=schema)
    # cursor.execute(future_grant.create_sql(if_not_exists=True))

    # result = safe_fetch(cursor, future_grant.urn)
    # assert result is not None
    # _assert_resource_dicts_eq_ignore_nulls(result, future_grant.to_dict())


@pytest.mark.enterprise
def test_fetch_enterprise_schema(cursor, account_locator, test_db):

    urn = URN(
        resource_type=ResourceType.SCHEMA,
        fqn=FQN(name="ENTERPRISE_TEST_SCHEMA", database=test_db),
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
        "tags": {"STATIC_DATABASE.PUBLIC.STATIC_TAG": "STATIC_TAG_VALUE"},
        "owner": TEST_ROLE,
        "comment": None,
    }


def test_fetch_grant_on_account(cursor, suffix):
    role = res.Role(name=f"TEST_ACCOUNT_GRANTS_ROLE_{suffix}")
    cursor.execute(role.create_sql(if_not_exists=True))
    cursor.execute(f"GRANT AUDIT ON ACCOUNT TO ROLE {role.name}")
    cursor.execute(f"GRANT BIND SERVICE ENDPOINT ON ACCOUNT TO ROLE {role.name}")

    try:
        bind_service_urn = parse_URN(f"urn:::grant/{role.name}?priv=BIND SERVICE ENDPOINT&on=account/ACCOUNT")
        bind_service_grant = safe_fetch(cursor, bind_service_urn)
        assert bind_service_grant is not None
        assert bind_service_grant["priv"] == "BIND SERVICE ENDPOINT"
        assert bind_service_grant["on"] == "ACCOUNT"
        assert bind_service_grant["on_type"] == "ACCOUNT"
        assert bind_service_grant["to"] == role.name
        audit_urn = parse_URN(f"urn:::grant/{role.name}?priv=AUDIT&on=account/ACCOUNT")
        audit_grant = safe_fetch(cursor, audit_urn)
        assert audit_grant is not None
        assert audit_grant["priv"] == "AUDIT"
        assert audit_grant["on"] == "ACCOUNT"
        assert audit_grant["on_type"] == "ACCOUNT"
        assert audit_grant["to"] == role.name
    finally:
        cursor.execute(role.drop_sql(if_exists=True))


def test_fetch_database(cursor, suffix):
    database = res.Database(name=f"SOMEDB_{suffix}", owner=TEST_ROLE)
    cursor.execute(database.create_sql(if_not_exists=True))

    try:
        result = safe_fetch(cursor, database.urn)
        assert result is not None
        _assert_resource_dicts_eq_ignore_nulls(result, database.to_dict())
    finally:
        cursor.execute(database.drop_sql(if_exists=True))


def test_fetch_grant_all_on_resource(cursor):
    cursor.execute(f"GRANT ALL ON WAREHOUSE STATIC_WAREHOUSE TO ROLE STATIC_ROLE")
    grant_all_urn = parse_URN(f"urn:::grant/STATIC_ROLE?priv=ALL&on=warehouse/STATIC_WAREHOUSE")

    grant = safe_fetch(cursor, grant_all_urn)
    assert grant is not None
    assert grant["priv"] == "ALL"
    assert grant["on_type"] == "WAREHOUSE"
    assert grant["on"] == "STATIC_WAREHOUSE"
    assert grant["to"] == "STATIC_ROLE"
    assert grant["owner"] == "SYSADMIN"
    assert grant["grant_option"] is False
    assert grant["_privs"] == ["APPLYBUDGET", "MODIFY", "MONITOR", "OPERATE", "USAGE"]

    cursor.execute(f"REVOKE MODIFY ON WAREHOUSE STATIC_WAREHOUSE FROM ROLE STATIC_ROLE")

    grant = safe_fetch(cursor, grant_all_urn)
    assert grant is not None
    assert "MODIFY" not in grant["_privs"]


def test_fetch_external_stage(cursor, test_db, marked_for_cleanup):
    external_stage = res.ExternalStage(
        name="EXTERNAL_STAGE_EXAMPLE",
        url="s3://titan-snowflake/",
        owner=TEST_ROLE,
        database=test_db,
        schema="PUBLIC",
    )
    cursor.execute(external_stage.create_sql(if_not_exists=True))
    marked_for_cleanup.append(external_stage)

    result = safe_fetch(cursor, external_stage.urn)
    assert result is not None
    _assert_resource_dicts_eq_ignore_nulls(result, external_stage.to_dict())

    external_stage = res.ExternalStage(
        name="EXTERNAL_STAGE_EXAMPLE_WITH_DIRECTORY",
        url="s3://titan-snowflake/",
        owner=TEST_ROLE,
        database=test_db,
        schema="PUBLIC",
        directory={"enable": True},
    )
    cursor.execute(external_stage.create_sql(if_not_exists=True))
    marked_for_cleanup.append(external_stage)

    result = safe_fetch(cursor, external_stage.urn)
    assert result is not None
    _assert_resource_dicts_eq_ignore_nulls(result, external_stage.to_dict())


def test_fetch_internal_stage(cursor, test_db, marked_for_cleanup):
    internal_stage = res.InternalStage(
        name="INTERNAL_STAGE_EXAMPLE",
        owner=TEST_ROLE,
        database=test_db,
        schema="PUBLIC",
    )
    cursor.execute(internal_stage.create_sql(if_not_exists=True))
    marked_for_cleanup.append(internal_stage)

    result = safe_fetch(cursor, internal_stage.urn)
    assert result is not None
    _assert_resource_dicts_eq_ignore_nulls(result, internal_stage.to_dict())

    internal_stage = res.InternalStage(
        name="INTERNAL_STAGE_EXAMPLE_WITH_DIRECTORY",
        directory={"enable": True},
        owner=TEST_ROLE,
        database=test_db,
        schema="PUBLIC",
    )
    cursor.execute(internal_stage.create_sql(if_not_exists=True))
    marked_for_cleanup.append(internal_stage)

    result = safe_fetch(cursor, internal_stage.urn)
    assert result is not None
    _assert_resource_dicts_eq_ignore_nulls(result, internal_stage.to_dict())


def test_fetch_csv_file_format(cursor, test_db, marked_for_cleanup):
    csv_file_format = res.CSVFileFormat(
        name="CSV_FILE_FORMAT_EXAMPLE",
        owner=TEST_ROLE,
        field_delimiter="|",
        skip_header=1,
        null_if=["NULL", "null"],
        empty_field_as_null=True,
        compression="GZIP",
        database=test_db,
        schema="PUBLIC",
    )
    cursor.execute(csv_file_format.create_sql(if_not_exists=True))
    marked_for_cleanup.append(csv_file_format)

    result = safe_fetch(cursor, csv_file_format.urn)
    assert result is not None
    _assert_resource_dicts_eq_ignore_nulls(result, csv_file_format.to_dict())


def test_fetch_resource_monitor(cursor, marked_for_cleanup):
    resource_monitor = res.ResourceMonitor(
        name="RESOURCE_MONITOR_EXAMPLE",
        credit_quota=1000,
        start_timestamp="2049-01-01 00:00",
    )
    cursor.execute(resource_monitor.create_sql(if_not_exists=True))
    marked_for_cleanup.append(resource_monitor)

    result = safe_fetch(cursor, resource_monitor.urn)
    assert result is not None
    _assert_resource_dicts_eq_ignore_nulls(result, resource_monitor.to_dict())


def test_fetch_email_notification_integration(cursor, email_address, marked_for_cleanup):

    email_notification_integration = res.EmailNotificationIntegration(
        name="EMAIL_NOTIFICATION_INTEGRATION_EXAMPLE",
        enabled=True,
        allowed_recipients=[email_address],
        comment="Example email notification integration",
    )
    cursor.execute(email_notification_integration.create_sql(if_not_exists=True))
    marked_for_cleanup.append(email_notification_integration)

    result = safe_fetch(cursor, email_notification_integration.urn)
    assert result is not None
    result = data_provider.remove_none_values(result)
    assert result == data_provider.remove_none_values(email_notification_integration.to_dict())


def test_fetch_event_table(cursor, test_db, marked_for_cleanup):
    event_table = res.EventTable(
        name="EVENT_TABLE_EXAMPLE",
        change_tracking=True,
        cluster_by=["START_TIMESTAMP"],
        data_retention_time_in_days=1,
        owner=TEST_ROLE,
        database=test_db,
        schema="PUBLIC",
    )
    cursor.execute(event_table.create_sql(if_not_exists=True))
    marked_for_cleanup.append(event_table)

    result = safe_fetch(cursor, event_table.urn)
    assert result is not None
    result = data_provider.remove_none_values(result)
    assert result == data_provider.remove_none_values(event_table.to_dict())


def test_fetch_grant_with_fully_qualified_ref(cursor, test_db, suffix, marked_for_cleanup):
    cursor.execute(f"USE DATABASE {test_db}")
    cursor.execute(f"CREATE SCHEMA if not exists {test_db}.my_schema")
    role = res.Role(name=f"test_role_grant_{suffix}")
    cursor.execute(role.create_sql(if_not_exists=True))
    marked_for_cleanup.append(role)
    cursor.execute(f"GRANT USAGE ON SCHEMA {test_db}.my_schema TO ROLE {role.name}")
    grant = res.Grant.from_sql(f"GRANT USAGE ON SCHEMA {test_db}.my_schema TO ROLE {role.name}")
    grant._data.owner = TEST_ROLE
    result = safe_fetch(cursor, grant.urn)
    assert result is not None
    result = data_provider.remove_none_values(result)
    result["on"] = ResourceName(result["on"])
    assert result == data_provider.remove_none_values(grant.to_dict())


def test_fetch_pipe(cursor, test_db, marked_for_cleanup):
    pipe = res.Pipe(
        name="PIPE_EXAMPLE",
        as_=f"""
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
    cursor.execute(pipe.create_sql(if_not_exists=True))
    marked_for_cleanup.append(pipe)

    result = safe_fetch(cursor, pipe.urn)
    assert result is not None
    result = data_provider.remove_none_values(result)
    assert result == data_provider.remove_none_values(pipe.to_dict())


# @pytest.mark.skip(reason="Requires view DDL parsing")
# def test_fetch_view(cursor, test_db, marked_for_cleanup):
#     view = res.View(
#         name="VIEW_EXAMPLE",
#         as_=f"""
#         SELECT 1 as id FROM STATIC_DATABASE.PUBLIC.STATIC_TABLE
#         """,
#         columns=[{"name": "ID", "data_type": "NUMBER(1,0)", "not_null": False}],
#         comment="View for testing",
#         owner=TEST_ROLE,
#         database=test_db,
#         schema="PUBLIC",
#     )
#     cursor.execute(view.create_sql(if_not_exists=True))
#     marked_for_cleanup.append(view)

#     result = safe_fetch(cursor, view.urn)
#     assert result is not None
#     result = data_provider.remove_none_values(result)
#     assert result == data_provider.remove_none_values(view.to_dict())


@pytest.mark.enterprise
def test_fetch_tag(cursor, test_db, marked_for_cleanup):
    tag = res.Tag(
        name="TAG_EXAMPLE_WITH_ALLOWED_VALUES",
        database=test_db,
        schema="PUBLIC",
        comment="Tag for testing",
        allowed_values=["SOME_VALUE"],
    )
    cursor.execute(tag.create_sql(if_not_exists=True))
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
    )
    cursor.execute(tag.create_sql(if_not_exists=True))
    marked_for_cleanup.append(tag)

    result = safe_fetch(cursor, tag.urn)
    assert result is not None
    result = data_provider.remove_none_values(result)
    assert result == data_provider.remove_none_values(tag.to_dict())


def test_fetch_role(cursor, suffix, marked_for_cleanup):
    role = res.Role(name=f"ANOTHER_ROLE_{suffix}", owner=TEST_ROLE)
    cursor.execute(role.create_sql(if_not_exists=True))
    marked_for_cleanup.append(role)

    result = safe_fetch(cursor, role.urn)
    assert result is not None
    _assert_resource_dicts_eq_ignore_nulls(result, role.to_dict())


def test_fetch_role_grant(cursor, suffix, marked_for_cleanup):
    parent = res.Role(name=f"PARENT_ROLE_{suffix}", owner=TEST_ROLE)
    child = res.Role(name=f"CHILD_ROLE_{suffix}", owner=TEST_ROLE)
    cursor.execute(parent.create_sql(if_not_exists=True))
    cursor.execute(child.create_sql(if_not_exists=True))
    marked_for_cleanup.append(parent)
    marked_for_cleanup.append(child)

    # Role-to-role grant
    grant = res.RoleGrant(role=child, to_role=parent)
    cursor.execute(grant.create_sql(if_not_exists=True))

    result = safe_fetch(cursor, grant.urn)
    assert result is not None
    _assert_resource_dicts_eq_ignore_nulls(result, grant.to_dict())

    user = res.User(name=f"ROLE_RECIPIENT_USER_{suffix}", owner=TEST_ROLE)
    cursor.execute(user.create_sql(if_not_exists=True))
    marked_for_cleanup.append(user)

    # Role-to-user grant
    grant = res.RoleGrant(role=child, to_user=user)
    cursor.execute(grant.create_sql(if_not_exists=True))
    result = safe_fetch(cursor, grant.urn)
    assert result is not None
    _assert_resource_dicts_eq_ignore_nulls(result, grant.to_dict())


def test_fetch_user(cursor, suffix, marked_for_cleanup):
    user = res.User(name=f"SOME_USER_{suffix}@applytitan.com", owner=TEST_ROLE)
    cursor.execute(user.create_sql(if_not_exists=True))
    marked_for_cleanup.append(user)

    result = safe_fetch(cursor, user.urn)
    assert result is not None
    _assert_resource_dicts_eq_ignore_nulls(result, user.to_dict())


def test_fetch_object_store_catalog_integration(cursor, marked_for_cleanup):
    catalog_integration = res.ObjectStoreCatalogIntegration(
        name="OBJECT_STORE_CATALOG_INTEGRATION_EXAMPLE",
        catalog_source="OBJECT_STORE",
        table_format="ICEBERG",
        enabled=True,
        comment="Catalog integration for testing",
        owner=TEST_ROLE,
    )
    cursor.execute(catalog_integration.create_sql(if_not_exists=True))
    marked_for_cleanup.append(catalog_integration)

    result = safe_fetch(cursor, catalog_integration.urn)
    assert result is not None
    _assert_resource_dicts_eq_ignore_nulls(result, catalog_integration.to_dict())


def test_fetch_share(cursor, suffix, marked_for_cleanup):
    share = res.Share(
        name=f"SHARE_EXAMPLE_{suffix}",
        comment="Share for testing",
        owner=TEST_ROLE,
    )
    cursor.execute(share.create_sql(if_not_exists=True))
    marked_for_cleanup.append(share)

    result = safe_fetch(cursor, share.urn)
    assert result is not None
    _assert_resource_dicts_eq_ignore_nulls(result, share.to_dict())


def test_fetch_s3_storage_integration(cursor, suffix, marked_for_cleanup):
    storage_integration = res.S3StorageIntegration(
        name=f"S3_STORAGE_INTEGRATION_EXAMPLE_{suffix}",
        storage_provider="S3",
        storage_aws_role_arn="arn:aws:iam::001234567890:role/myrole",
        enabled=True,
        storage_allowed_locations=["s3://mybucket1/path1/", "s3://mybucket2/path2/"],
        owner=TEST_ROLE,
    )
    cursor.execute(storage_integration.create_sql(if_not_exists=True))
    marked_for_cleanup.append(storage_integration)

    result = safe_fetch(cursor, storage_integration.urn)
    assert result is not None
    _assert_resource_dicts_eq_ignore_nulls(result, storage_integration.to_dict())


def test_fetch_alert(cursor, suffix, test_db, marked_for_cleanup):
    alert = res.Alert(
        name=f"SOMEALERT_{suffix}",
        warehouse="STATIC_WAREHOUSE",
        schedule="60 MINUTE",
        condition="SELECT 1",
        then="SELECT 1",
        owner=TEST_ROLE,
        database=test_db,
        schema="PUBLIC",
    )
    cursor.execute(alert.create_sql(if_not_exists=True))
    marked_for_cleanup.append(alert)

    result = safe_fetch(cursor, alert.urn)
    assert result is not None
    _assert_resource_dicts_eq_ignore_nulls(result, alert.to_dict())


def test_fetch_dynamic_table(cursor, test_db, marked_for_cleanup):
    dynamic_table = res.DynamicTable(
        name="PRODUCT",
        columns=[{"name": "ID", "comment": "This is a comment"}],
        target_lag="20 minutes",
        warehouse="CI",
        refresh_mode="AUTO",
        initialize="ON_CREATE",
        comment="this is a comment",
        as_="SELECT id FROM STATIC_DATABASE.PUBLIC.STATIC_TABLE",
        owner=TEST_ROLE,
        database=test_db,
        schema="PUBLIC",
    )
    cursor.execute(dynamic_table.create_sql(if_not_exists=True))
    marked_for_cleanup.append(dynamic_table)

    result = safe_fetch(cursor, dynamic_table.urn)
    assert result is not None
    _assert_resource_dicts_eq_ignore_nulls(result, dynamic_table.to_dict())


@pytest.mark.skip(reason="Generates invalid SQL")
def test_fetch_javascript_udf(cursor, test_db, marked_for_cleanup):
    function = res.JavascriptUDF(
        name="SOMEFUNC",
        returns="FLOAT",
        volatility="VOLATILE",
        as_="return 42;",
        secure=False,
        owner=TEST_ROLE,
        database=test_db,
        schema="PUBLIC",
    )
    cursor.execute(function.create_sql(if_not_exists=True))
    marked_for_cleanup.append(function)

    result = safe_fetch(cursor, function.urn)
    assert result is not None
    _assert_resource_dicts_eq_ignore_nulls(result, function.to_dict())


def test_fetch_password_policy(cursor, test_db, marked_for_cleanup):
    password_policy = res.PasswordPolicy(
        name="SOMEPOLICY",
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
        comment="production account password policy",
        owner=TEST_ROLE,
        database=test_db,
        schema="PUBLIC",
    )
    cursor.execute(password_policy.create_sql(if_not_exists=True))
    marked_for_cleanup.append(password_policy)

    result = safe_fetch(cursor, password_policy.urn)
    assert result is not None
    _assert_resource_dicts_eq_ignore_nulls(result, password_policy.to_dict())


@pytest.mark.skip(reason="Generates invalid SQL")
def test_fetch_python_stored_procedure(cursor, suffix, test_db, marked_for_cleanup):
    procedure = res.PythonStoredProcedure(
        name=f"somesproc_{suffix}",
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
        database=test_db,
        schema="PUBLIC",
    )
    cursor.execute(procedure.create_sql())
    marked_for_cleanup.append(procedure)

    result = safe_fetch(cursor, procedure.urn)
    assert result is not None
    _assert_resource_dicts_eq_ignore_nulls(result, procedure.to_dict())


def test_fetch_schema(cursor, test_db, marked_for_cleanup):
    schema = res.Schema(
        name="SOMESCH",
        data_retention_time_in_days=1,
        max_data_extension_time_in_days=3,
        transient=False,
        managed_access=False,
        owner=TEST_ROLE,
        database=test_db,
    )
    cursor.execute(schema.create_sql(if_not_exists=True))
    marked_for_cleanup.append(schema)

    result = safe_fetch(cursor, schema.urn)
    assert result is not None
    _assert_resource_dicts_eq_ignore_nulls(result, schema.to_dict())


def test_fetch_sequence(cursor, suffix, test_db, marked_for_cleanup):
    sequence = res.Sequence(
        name=f"SOMESEQ_{suffix}",
        start=1,
        increment=2,
        comment="+3",
        owner=TEST_ROLE,
        database=test_db,
        schema="PUBLIC",
    )
    cursor.execute(sequence.create_sql(if_not_exists=True))
    marked_for_cleanup.append(sequence)

    result = safe_fetch(cursor, sequence.urn)
    assert result is not None
    _assert_resource_dicts_eq_ignore_nulls(result, sequence.to_dict())


def test_fetch_task(cursor, suffix, test_db, marked_for_cleanup):
    task = res.Task(
        name=f"SOMETASK_{suffix}",
        schedule="60 MINUTE",
        state="SUSPENDED",
        as_="SELECT 1",
        owner=TEST_ROLE,
        database=test_db,
        schema="PUBLIC",
    )
    cursor.execute(task.create_sql(if_not_exists=True))
    marked_for_cleanup.append(task)

    result = safe_fetch(cursor, task.urn)
    assert result is not None
    _assert_resource_dicts_eq_ignore_nulls(result, task.to_dict())


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
    cursor.execute(network_rule.create_sql(if_not_exists=True))
    marked_for_cleanup.append(network_rule)

    result = safe_fetch(cursor, network_rule.urn)
    assert result is not None
    _assert_resource_dicts_eq_ignore_nulls(result, network_rule.to_dict())

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
    cursor.execute(network_rule.create_sql(if_not_exists=True))
    marked_for_cleanup.append(network_rule)

    result = safe_fetch(cursor, network_rule.urn)
    assert result is not None
    _assert_resource_dicts_eq_ignore_nulls(result, network_rule.to_dict())


def test_fetch_api_integration(cursor, suffix, marked_for_cleanup):
    api_integration = res.APIIntegration(
        name=f"API_INTEGRATION_EXAMPLE_{suffix}",
        api_provider="AWS_API_GATEWAY",
        api_aws_role_arn="arn:aws:iam::123456789012:role/MyRole",
        api_allowed_prefixes=["https://xyz.execute-api.us-west-2.amazonaws.com/production"],
        api_blocked_prefixes=["https://xyz.execute-api.us-west-2.amazonaws.com/test"],
        comment="Example API integration",
        enabled=False,
        owner=TEST_ROLE,
    )

    cursor.execute(api_integration.create_sql(if_not_exists=True))
    marked_for_cleanup.append(api_integration)

    result = safe_fetch(cursor, api_integration.urn)
    assert result is not None
    _assert_resource_dicts_eq_ignore_nulls(result, api_integration.to_dict())


def test_fetch_database_role(cursor, suffix, test_db, marked_for_cleanup):
    database_role = res.DatabaseRole(
        name=f"DATABASE_ROLE_EXAMPLE_{suffix}",
        database=test_db,
        owner=TEST_ROLE,
    )
    cursor.execute(database_role.create_sql(if_not_exists=True))
    marked_for_cleanup.append(database_role)

    result = safe_fetch(cursor, database_role.urn)
    assert result is not None
    _assert_resource_dicts_eq_ignore_nulls(result, database_role.to_dict())


def test_fetch_packages_policy(cursor, suffix, marked_for_cleanup):
    packages_policy = res.PackagesPolicy(
        name=f"PACKAGES_POLICY_EXAMPLE_{suffix}",
        allowlist=["numpy", "pandas"],
        blocklist=["os", "sys"],
        comment="Example packages policy",
        owner=TEST_ROLE,
    )
    cursor.execute(packages_policy.create_sql(if_not_exists=True))
    marked_for_cleanup.append(packages_policy)

    result = safe_fetch(cursor, packages_policy.urn)
    assert result is not None
    _assert_resource_dicts_eq_ignore_nulls(result, packages_policy.to_dict())


@pytest.mark.enterprise
def test_fetch_aggregation_policy(cursor, suffix, test_db, marked_for_cleanup):
    aggregation_policy = res.AggregationPolicy(
        name=f"AGGREGATION_POLICY_EXAMPLE_{suffix}",
        body="AGGREGATION_CONSTRAINT(MIN_GROUP_SIZE => 5)",
        owner=TEST_ROLE,
        database=test_db,
        schema="PUBLIC",
    )
    cursor.execute(aggregation_policy.create_sql(if_not_exists=True))
    marked_for_cleanup.append(aggregation_policy)

    result = safe_fetch(cursor, aggregation_policy.urn)
    assert result is not None
    _assert_resource_dicts_eq_ignore_nulls(result, aggregation_policy.to_dict())


def test_fetch_compute_pool(cursor, suffix, marked_for_cleanup):
    compute_pool = res.ComputePool(
        name=f"SOME_COMPUTE_POOL_{suffix}",
        min_nodes=1,
        max_nodes=1,
        instance_family="CPU_X64_XS",
        auto_resume=False,
        auto_suspend_secs=60,
        comment="Compute Pool comment",
    )
    cursor.execute(compute_pool.create_sql(if_not_exists=True))
    marked_for_cleanup.append(compute_pool)

    result = safe_fetch(cursor, compute_pool.urn)
    assert result is not None
    _assert_resource_dicts_eq_ignore_nulls(result, compute_pool.to_dict())


def test_fetch_warehouse(cursor, suffix, marked_for_cleanup):
    warehouse = res.Warehouse(
        name=f"SOME_WAREHOUSE_{suffix}",
        warehouse_size="XSMALL",
        auto_suspend=60,
        auto_resume=True,
        owner=TEST_ROLE,
    )
    cursor.execute(warehouse.create_sql(if_not_exists=True))
    marked_for_cleanup.append(warehouse)

    result = safe_fetch(cursor, warehouse.urn)
    assert result is not None
    _assert_resource_dicts_eq_ignore_nulls(result, warehouse.to_dict())


def test_fetch_password_secret(cursor, suffix, marked_for_cleanup):
    secret = res.PasswordSecret(
        name=f"PASSWORD_SECRET_EXAMPLE_{suffix}",
        username="my_username",
        password="my_password",
        comment="Password secret for accessing external database",
        owner=TEST_ROLE,
    )
    cursor.execute(secret.create_sql(if_not_exists=True))
    marked_for_cleanup.append(secret)

    result = safe_fetch(cursor, secret.urn)
    assert result is not None
    _assert_resource_dicts_eq_ignore_nulls_and_unfetchable(secret.spec, result, secret.to_dict())


def test_fetch_generic_secret(cursor, suffix, marked_for_cleanup):
    secret = res.GenericSecret(
        name=f"GENERIC_SECRET_EXAMPLE_{suffix}",
        secret_string="my_secret_string",
        comment="Generic secret for various purposes",
        owner=TEST_ROLE,
    )
    cursor.execute(secret.create_sql(if_not_exists=True))
    marked_for_cleanup.append(secret)

    result = safe_fetch(cursor, secret.urn)
    assert result is not None
    _assert_resource_dicts_eq_ignore_nulls_and_unfetchable(secret.spec, result, secret.to_dict())


def test_fetch_oauth_secret(cursor, suffix, marked_for_cleanup):
    secret = res.OAuthSecret(
        name=f"OAUTH_SECRET_EXAMPLE_WITH_SCOPES_{suffix}",
        api_authentication="STATIC_SECURITY_INTEGRATION",
        comment="OAuth secret for accessing external API",
        owner=TEST_ROLE,
    )
    cursor.execute(secret.create_sql(if_not_exists=True))
    marked_for_cleanup.append(secret)

    result = safe_fetch(cursor, secret.urn)
    assert result is not None
    _assert_resource_dicts_eq_ignore_nulls_and_unfetchable(secret.spec, result, secret.to_dict())

    secret = res.OAuthSecret(
        name=f"OAUTH_SECRET_EXAMPLE_WITH_TOKEN_{suffix}",
        api_authentication="STATIC_SECURITY_INTEGRATION",
        oauth_refresh_token="my_refresh_token",
        oauth_refresh_token_expiry_time="2049-01-06 20:00:00",
        comment="OAuth secret for accessing external API",
        owner=TEST_ROLE,
    )
    cursor.execute(secret.create_sql(if_not_exists=True))
    marked_for_cleanup.append(secret)

    result = safe_fetch(cursor, secret.urn)
    assert result is not None
    _assert_resource_dicts_eq_ignore_nulls_and_unfetchable(secret.spec, result, secret.to_dict())


def test_fetch_snowservices_oauth_security_integration(cursor, suffix, marked_for_cleanup):
    security_integration = res.SnowservicesOAuthSecurityIntegration(
        name=f"SNOWSERVICES_INGRESS_OAUTH_{suffix}",
        type="OAUTH",
        oauth_client="snowservices_ingress",
        enabled=True,
    )
    cursor.execute(security_integration.create_sql(if_not_exists=True))
    marked_for_cleanup.append(security_integration)

    result = safe_fetch(cursor, security_integration.urn)
    assert result is not None
    _assert_resource_dicts_eq_ignore_nulls(result, security_integration.to_dict())


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
    cursor.execute(security_integration.create_sql(if_not_exists=True))
    marked_for_cleanup.append(security_integration)

    result = safe_fetch(cursor, security_integration.urn)
    assert result is not None
    _assert_resource_dicts_eq_ignore_nulls_and_unfetchable(
        res.APIAuthenticationSecurityIntegration.spec,
        result,
        security_integration.to_dict(),
    )


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
    cursor.execute(stream.create_sql(if_not_exists=True))
    marked_for_cleanup.append(stream)

    result = safe_fetch(cursor, stream.urn)
    assert result is not None
    _assert_resource_dicts_eq_ignore_nulls_and_unfetchable(res.TableStream.spec, result, stream.to_dict())
