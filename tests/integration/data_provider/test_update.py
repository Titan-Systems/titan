import uuid

import pytest
from titan.data_provider import DataProvider, remove_none_values
from titan.client import get_session
from titan.enums import Scope
from titan.identifiers import FQN
from titan.resources import Database


@pytest.fixture(scope="session")
def suffix():
    return str(uuid.uuid4())[:8]


@pytest.fixture(scope="session")
def marked_for_cleanup():
    """List to keep track of resources created during tests."""
    return []


@pytest.fixture(scope="session")
def cursor(suffix, marked_for_cleanup):
    session = get_session()
    with session.cursor() as cur:
        cur.execute(f"ALTER SESSION set query_tag='titan_package:test::{suffix}'")
        yield cur
        for res in marked_for_cleanup:
            cur.execute(res.drop_sql())


def test_update_database(cursor, marked_for_cleanup):
    db = Database(name="test_db", max_data_extension_time_in_days=10)
    cursor.execute(db.create_sql())
    marked_for_cleanup.append(db)
    provider = DataProvider(cursor.connection)
    result = provider.fetch_database(FQN(name="test_db"))
    assert result["max_data_extension_time_in_days"] == 10
    provider.update_database(db.fqn, {"max_data_extension_time_in_days": (10, 9)})
    result = provider.fetch_database(FQN(name="test_db"))
    assert result["max_data_extension_time_in_days"] == 9
