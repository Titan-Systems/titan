import os

import pytest

from tests.helpers import safe_fetch
from titan import resources as res

pytestmark = pytest.mark.requires_snowflake

TEST_ROLE = os.environ.get("TEST_SNOWFLAKE_ROLE")
TEST_USER = os.environ.get("TEST_SNOWFLAKE_USER")


def test_fetch_owner(cursor, suffix, test_db, marked_for_cleanup):
    database_role = res.DatabaseRole(
        name=f"TEST_FETCH_OWNER_DATABASE_ROLE_{suffix}",
        database=test_db,
    )
    schema = res.Schema(
        name="SOME_SCHEMA",
        database=test_db,
    )
    cursor.execute(database_role.create_sql())
    cursor.execute(schema.create_sql())
    marked_for_cleanup.append(database_role)
    marked_for_cleanup.append(schema)

    result = safe_fetch(cursor, schema.urn)
    assert result is not None
    assert result["owner"] == TEST_ROLE

    cursor.execute(f"GRANT OWNERSHIP ON SCHEMA {test_db}.SOME_SCHEMA TO DATABASE ROLE {database_role.name}")

    result = safe_fetch(cursor, schema.urn)
    assert result is not None
    assert result["owner"] == str(database_role.urn.fqn)

    cursor.execute(f"GRANT OWNERSHIP ON SCHEMA {test_db}.SOME_SCHEMA TO ROLE {TEST_ROLE}")
