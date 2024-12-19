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
        cursor, parse_URN(f"urn:::grant/GRANT?priv=USAGE&on=schema/{test_db}.SCHEMA_1&to=role/STATIC_ROLE")
    )
    assert schema_1_usage_grant is not None
    assert schema_1_usage_grant["priv"] == "USAGE"
    assert schema_1_usage_grant["to"] == "STATIC_ROLE"
    assert schema_1_usage_grant["on"] == f"{test_db}.SCHEMA_1"
    assert schema_1_usage_grant["on_type"] == "SCHEMA"

    schema_2_usage_grant = safe_fetch(
        cursor, parse_URN(f"urn:::grant/GRANT?priv=USAGE&on=schema/{test_db}.SCHEMA_2&to=role/STATIC_ROLE")
    )
    assert schema_2_usage_grant is not None
    assert schema_2_usage_grant["priv"] == "USAGE"
    assert schema_2_usage_grant["to"] == "STATIC_ROLE"
    assert schema_2_usage_grant["on"] == f"{test_db}.SCHEMA_2"
    assert schema_2_usage_grant["on_type"] == "SCHEMA"

    schema_3_usage_grant = safe_fetch(
        cursor, parse_URN(f"urn:::grant/GRANT?priv=USAGE&on=schema/{test_db}.SCHEMA_3&to=role/STATIC_ROLE")
    )
    assert schema_3_usage_grant is not None
    assert schema_3_usage_grant["priv"] == "USAGE"
    assert schema_3_usage_grant["to"] == "STATIC_ROLE"
    assert schema_3_usage_grant["on"] == f"{test_db}.SCHEMA_3"
    assert schema_3_usage_grant["on_type"] == "SCHEMA"


@pytest.mark.enterprise
def test_fetch_warehouse_snowpark_optimized(cursor, suffix, marked_for_cleanup):
    warehouse = res.Warehouse(
        name=f"TEST_FETCH_WAREHOUSE_SNOWPARK_OPTIMIZED_{suffix}",
        warehouse_type="SNOWPARK-OPTIMIZED",
        warehouse_size="MEDIUM",
        initially_suspended=True,
    )

    cursor.execute(warehouse.create_sql())
    marked_for_cleanup.append(warehouse)
    data = safe_fetch(cursor, warehouse.urn)
    assert data is not None
    assert data["warehouse_type"] == "SNOWPARK-OPTIMIZED"


def test_snowflake_builtin_database_role_grant(cursor, suffix, marked_for_cleanup):
    drg = res.DatabaseRoleGrant(database_role="SNOWFLAKE.CORTEX_USER", to_role="STATIC_ROLE")
    marked_for_cleanup.append(drg)
    cursor.execute(drg.create_sql())

    dbr = res.DatabaseRole(name=f"TEST_GRANT_DATABASE_ROLE_{suffix}", database="STATIC_DATABASE")
    drg = res.DatabaseRoleGrant(database_role=dbr, to_database_role="STATIC_DATABASE.STATIC_DATABASE_ROLE")
    marked_for_cleanup.append(dbr)
    marked_for_cleanup.append(drg)
    cursor.execute(dbr.create_sql())
    cursor.execute(drg.create_sql())
