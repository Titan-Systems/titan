import os

import pytest
import snowflake.connector.errors

from tests.helpers import get_json_fixtures
from titan import resources as res
from titan import data_provider
from titan.resources import Resource
from titan.blueprint import Blueprint, CreateResource, UpdateResource
from titan.client import FEATURE_NOT_ENABLED_ERR, UNSUPPORTED_FEATURE
from titan.data_provider import fetch_session
from titan.client import reset_cache
from titan.scope import DatabaseScope, SchemaScope

JSON_FIXTURES = list(get_json_fixtures())
TEST_ROLE = os.environ.get("TEST_SNOWFLAKE_ROLE")

pytestmark = pytest.mark.requires_snowflake


@pytest.fixture(
    params=JSON_FIXTURES,
    ids=[resource_cls.__name__ for resource_cls, _ in JSON_FIXTURES],
    scope="function",
)
def resource(request):
    resource_cls, data = request.param
    res = resource_cls(**data)

    yield res


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


def test_create_drop_from_json(resource, cursor, suffix):

    # Not easily testable without flakiness
    if resource.__class__ in (
        res.AccountParameter,
        res.FutureGrant,
        res.Grant,
        res.RoleGrant,
        res.ScannerPackage,
        res.Service,
    ):
        pytest.skip("Skipping")

    lifecycle_db = f"LIFECYCLE_DB_{suffix}_{resource.__class__.__name__}"
    database = res.Database(name=lifecycle_db, owner="SYSADMIN")

    feature_enabled = True

    try:
        fetch_session.cache_clear()
        session_ctx = fetch_session(cursor.connection)

        if session_ctx["account_edition"] not in resource.edition:
            feature_enabled = False
            pytest.skip(
                f"Skipping {resource.__class__.__name__}, not supported by account edition {session_ctx['account_edition']}"
            )

        if isinstance(resource.scope, (DatabaseScope, SchemaScope)):
            cursor.execute("USE ROLE SYSADMIN")
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {lifecycle_db}")
            cursor.execute(f"USE DATABASE {lifecycle_db}")
            cursor.execute("USE WAREHOUSE CI")

        if isinstance(resource.scope, DatabaseScope):
            database.add(resource)
        elif isinstance(resource.scope, SchemaScope):
            database.public_schema.add(resource)

        fetch_session.cache_clear()
        reset_cache()
        blueprint = Blueprint()
        blueprint.add(resource)
        plan = blueprint.plan(cursor.connection)
        assert len(plan) == 1
        assert isinstance(plan[0], CreateResource)
        blueprint.apply(cursor.connection, plan)
    except snowflake.connector.errors.ProgrammingError as err:
        if err.errno == FEATURE_NOT_ENABLED_ERR or err.errno == UNSUPPORTED_FEATURE:
            feature_enabled = False
            pytest.skip(f"Skipping {resource.__class__.__name__}, feature not enabled")
        else:
            pytest.fail(f"Failed to create resource {resource}")
    finally:
        if feature_enabled:
            try:
                drop_sql = resource.drop_sql(if_exists=True)
                cursor.execute(drop_sql)
            except Exception:
                pytest.fail(f"Failed to drop resource with sql {drop_sql}")
        cursor.execute("USE ROLE SYSADMIN")
        cursor.execute(database.drop_sql(if_exists=True))


def test_task_lifecycle(cursor, suffix, marked_for_cleanup):
    task = res.Task(
        database="STATIC_DATABASE",
        schema="PUBLIC",
        name=f"TEST_TASK_LIFECYCLE_{suffix}",
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
        when="1=1",
    )
    create(cursor, task)
    marked_for_cleanup.append(task)

    # Change task attributes
    task = res.Task(
        database="STATIC_DATABASE",
        schema="PUBLIC",
        name=f"TEST_TASK_LIFECYCLE_{suffix}",
        schedule="59 MINUTE",
        state="STARTED",
        as_="SELECT 2",
        owner=TEST_ROLE,
        comment="This is a test task modified",
        allow_overlapping_execution=False,
        user_task_managed_initial_warehouse_size="LARGE",
        user_task_timeout_ms=2000,
        suspend_task_after_num_failures=2,
        config='{"output_dir": "/temp/test_directory/", "learning_rate": 0.2}',
        when="2=2",
    )
    blueprint = Blueprint()
    blueprint.add(task)
    plan = blueprint.plan(cursor.connection)
    assert len(plan) == 1
    assert isinstance(plan[0], UpdateResource)
    blueprint.apply(cursor.connection, plan)

    # Remove task attributes
    task = res.Task(
        database="STATIC_DATABASE",
        schema="PUBLIC",
        name=f"TEST_TASK_LIFECYCLE_{suffix}",
        as_="SELECT 3",
        owner=TEST_ROLE,
    )
    blueprint = Blueprint()
    blueprint.add(task)
    plan = blueprint.plan(cursor.connection)
    assert len(plan) == 1
    assert isinstance(plan[0], UpdateResource)
    blueprint.apply(cursor.connection, plan)


@pytest.mark.skip("This requires significant changes to lifecycle update")
def test_task_lifecycle_remove_predecessor(cursor, suffix, marked_for_cleanup):
    parent_task = res.Task(
        name=f"TEST_TASK_LIFECYCLE_REMOVE_PREDECESSOR_PARENT_{suffix}",
        state="SUSPENDED",
        as_="SELECT 1",
        owner=TEST_ROLE,
        database="STATIC_DATABASE",
        schema="PUBLIC",
    )
    child_task = res.Task(
        name=f"TEST_TASK_LIFECYCLE_REMOVE_PREDECESSOR_CHILD_{suffix}",
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

    # Remove predecessor
    child_task = res.Task(
        name=f"TEST_TASK_LIFECYCLE_REMOVE_PREDECESSOR_CHILD_{suffix}",
        state="SUSPENDED",
        as_="SELECT 1",
        owner=TEST_ROLE,
        database="STATIC_DATABASE",
        schema="PUBLIC",
    )
    blueprint = Blueprint()
    blueprint.add(child_task)
    plan = blueprint.plan(cursor.connection)
    assert len(plan) == 1
    assert isinstance(plan[0], UpdateResource)
    blueprint.apply(cursor.connection, plan)
