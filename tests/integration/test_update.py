import pytest

from tests.helpers import safe_fetch
from titan import lifecycle
from titan.resources import Schema


@pytest.mark.requires_snowflake
def test_update_schema(cursor, test_db, marked_for_cleanup):
    sch = Schema(name="TEST_SCHEMA", database=test_db, max_data_extension_time_in_days=10)
    cursor.execute(sch.create_sql())
    marked_for_cleanup.append(sch)
    result = safe_fetch(cursor, sch.urn)
    assert result["max_data_extension_time_in_days"] == 10
    cursor.execute(lifecycle.update_resource(sch.urn, {"max_data_extension_time_in_days": 9}))
    result = safe_fetch(cursor, sch.urn)
    assert result["max_data_extension_time_in_days"] == 9
