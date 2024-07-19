import pytest
from titan import resources as res
from titan.resource_name import ResourceName
from titan.identifiers import parse_URN

from tests.helpers import safe_fetch

pytestmark = pytest.mark.requires_snowflake


def test_user_name_defaults(cursor, suffix, marked_for_cleanup):
    user_name = f"user{suffix}_name_defaults"
    user = res.User(name=user_name)
    assert user.name == user_name
    assert user._data.login_name == user_name.upper()
    assert user._data.display_name == user_name
    cursor.execute(user.create_sql())
    marked_for_cleanup.append(user)
    data = safe_fetch(cursor, user.urn)
    assert data is not None
    assert data["name"] == user.name
    assert data["login_name"] == user._data.login_name
    assert data["display_name"] == user._data.display_name


def test_user_naked_quoted_name(cursor, suffix, marked_for_cleanup):
    user_name = f"~user{suffix}_naked_quoted_name"
    user = res.User(name=user_name)
    assert user.name == user_name
    assert user._data.login_name == user_name.upper()
    assert user._data.display_name == user_name
    cursor.execute(user.create_sql())
    marked_for_cleanup.append(user)
    data = safe_fetch(cursor, user.urn)
    assert data is not None
    assert data["name"] == user.name
    assert data["login_name"] == user._data.login_name
    assert data["display_name"] == user._data.display_name


def test_user_quoted_name(cursor, suffix, marked_for_cleanup):
    user_name = f'"user{suffix}_quoted_name"'
    user = res.User(name=user_name)
    assert user.name == user_name
    assert user._data.login_name == ResourceName(user_name)._name.upper()
    assert user._data.display_name == ResourceName(user_name)._name
    cursor.execute(user.create_sql())
    marked_for_cleanup.append(user)
    data = safe_fetch(cursor, user.urn)
    assert data is not None
    assert data["name"] == user.name
    assert data["login_name"] == user._data.login_name
    assert data["display_name"] == user._data.display_name


def test_user_name_intentionally_left_blank(cursor, suffix, marked_for_cleanup):
    user_name = f"user{suffix}_intentionally_left_blank"
    user = res.User(name=user_name, display_name="", login_name="")
    assert user.name == user_name
    assert user._data.login_name == user_name.upper()
    assert user._data.display_name == ""
    cursor.execute(user.create_sql())
    marked_for_cleanup.append(user)
    data = safe_fetch(cursor, user.urn)
    assert data is not None
    assert data["name"] == user.name
    assert data["login_name"] == user._data.login_name
    assert data["display_name"] == user._data.display_name


def test_grant_on_all(cursor, suffix, marked_for_cleanup):
    test_db = f"GRANT_ON_ALL_{suffix}"
    database = res.Database(name=test_db)
    cursor.execute(database.create_sql())
    marked_for_cleanup.append(database)
    schemas = ["schema_1", "schema_2", "schema_3"]
    for schema in schemas:
        schema = res.Schema(name=schema, database=database)
        cursor.execute(schema.create_sql())
        marked_for_cleanup.append(schema)

    grant = res.GrantOnAll(
        priv="USAGE",
        on_all_schemas_in=database,
        to="STATIC_ROLE",
    )
    cursor.execute(grant.create_sql())

    schema_1_usage_grant = safe_fetch(
        cursor, parse_URN(f"urn:::grant/STATIC_ROLE?priv=USAGE&on=schema/{test_db}.SCHEMA_1")
    )
    assert schema_1_usage_grant is not None
    assert schema_1_usage_grant["priv"] == "USAGE"
    assert schema_1_usage_grant["to"] == "STATIC_ROLE"
    assert schema_1_usage_grant["on"] == f"{test_db}.SCHEMA_1"
    assert schema_1_usage_grant["on_type"] == "SCHEMA"

    schema_2_usage_grant = safe_fetch(
        cursor, parse_URN(f"urn:::grant/STATIC_ROLE?priv=USAGE&on=schema/{test_db}.SCHEMA_2")
    )
    assert schema_2_usage_grant is not None
    assert schema_2_usage_grant["priv"] == "USAGE"
    assert schema_2_usage_grant["to"] == "STATIC_ROLE"
    assert schema_2_usage_grant["on"] == f"{test_db}.SCHEMA_2"
    assert schema_2_usage_grant["on_type"] == "SCHEMA"

    schema_3_usage_grant = safe_fetch(
        cursor, parse_URN(f"urn:::grant/STATIC_ROLE?priv=USAGE&on=schema/{test_db}.SCHEMA_3")
    )
    assert schema_3_usage_grant is not None
    assert schema_3_usage_grant["priv"] == "USAGE"
    assert schema_3_usage_grant["to"] == "STATIC_ROLE"
    assert schema_3_usage_grant["on"] == f"{test_db}.SCHEMA_3"
    assert schema_3_usage_grant["on_type"] == "SCHEMA"
