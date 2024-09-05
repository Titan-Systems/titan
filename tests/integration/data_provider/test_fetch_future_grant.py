import os

import pytest

from tests.helpers import (
    assert_resource_dicts_eq_ignore_nulls,
    safe_fetch,
)
from titan import resources as res

pytestmark = pytest.mark.requires_snowflake

TEST_ROLE = os.environ.get("TEST_SNOWFLAKE_ROLE")
TEST_USER = os.environ.get("TEST_SNOWFLAKE_USER")


def test_fetch_on_future_schemas_in_database(cursor):
    future_grant = res.FutureGrant(priv="usage", to=TEST_ROLE, on_future_schemas_in_database="STATIC_DATABASE")
    cursor.execute(future_grant.create_sql())

    result = safe_fetch(cursor, future_grant.urn)
    assert result is not None
    assert result["on_type"] == "SCHEMA"
    assert result["in_type"] == "DATABASE"
    assert result["in_name"] == "STATIC_DATABASE"
    assert result["priv"] == "USAGE"
    assert_resource_dicts_eq_ignore_nulls(result, future_grant.to_dict())


def test_fetch_on_future_tables_in_schema(cursor):
    schema = res.Schema(name="PUBLIC", database="STATIC_DATABASE")
    future_grant = res.FutureGrant(priv="SELECT", to=TEST_ROLE, on_future_tables_in=schema)
    cursor.execute(future_grant.create_sql())

    result = safe_fetch(cursor, future_grant.urn)
    assert result is not None
    assert result["on_type"] == "TABLE"
    assert result["in_type"] == "SCHEMA"
    assert result["in_name"] == "STATIC_DATABASE.PUBLIC"
    assert result["priv"] == "SELECT"
    assert_resource_dicts_eq_ignore_nulls(result, future_grant.to_dict())


def test_fetch_on_future_tables_in_database(cursor):
    future_grant = res.FutureGrant(priv="SELECT", to=TEST_ROLE, on_future_tables_in_database="STATIC_DATABASE")
    cursor.execute(future_grant.create_sql())

    result = safe_fetch(cursor, future_grant.urn)
    assert result is not None
    assert result["on_type"] == "TABLE"
    assert result["in_type"] == "DATABASE"
    assert result["in_name"] == "STATIC_DATABASE"
    assert result["priv"] == "SELECT"
    assert_resource_dicts_eq_ignore_nulls(result, future_grant.to_dict())


def test_fetch_on_future_git_repositories(cursor):
    future_grant = res.FutureGrant(
        priv="READ", to=TEST_ROLE, on_future_git_repositories_in_schema="STATIC_DATABASE.PUBLIC"
    )
    cursor.execute(future_grant.create_sql())

    result = safe_fetch(cursor, future_grant.urn)
    assert result is not None
    assert result["on_type"] == "GIT REPOSITORY"
    assert result["in_type"] == "SCHEMA"
    assert result["in_name"] == "STATIC_DATABASE.PUBLIC"
    assert result["priv"] == "READ"
    assert_resource_dicts_eq_ignore_nulls(result, future_grant.to_dict())
