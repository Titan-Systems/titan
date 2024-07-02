import os

import pytest

from tests.helpers import safe_fetch
from titan.enums import ResourceType
from titan.identifiers import FQN, URN
from titan.resource_name import ResourceName


pytestmark = pytest.mark.requires_snowflake

TEST_ROLE = os.environ.get("TEST_SNOWFLAKE_ROLE")
TEST_USER = os.environ.get("TEST_SNOWFLAKE_USER")


@pytest.mark.skip("very slow")
def test_fetch_over_1000_objects(cursor, test_db):
    for i in range(1005):
        cursor.execute(f"CREATE SCHEMA {test_db}_schema_{i}")

    schema = safe_fetch(
        cursor,
        URN(
            ResourceType.SCHEMA,
            fqn=FQN(database=test_db, name=f"{test_db}_schema_1004"),
            account_locator="",
        ),
    )
    assert schema is not None
    assert schema["name"] == f"{test_db}_SCHEMA_1004"


def test_fetch_quoted_identifier(cursor, test_db):
    cursor.execute(f'CREATE SCHEMA {test_db}."multiCaseString"')
    schema = safe_fetch(
        cursor, URN(ResourceType.SCHEMA, fqn=FQN(database=test_db, name=ResourceName('"multiCaseString"')))
    )
    assert schema is not None
    assert schema["name"] == '"multiCaseString"'
