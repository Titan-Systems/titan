import pytest

from titan import data_provider, lifecycle
from titan.resources import Schema


@pytest.mark.requires_snowflake
def test_update_schema(cursor, test_db, marked_for_cleanup):
    sch = Schema(name="TEST_SCHEMA", database=test_db, max_data_extension_time_in_days=10)
    cursor.execute(sch.create_sql())
    marked_for_cleanup.append(sch)
    result = data_provider.fetch_schema(cursor, sch.fqn)
    assert result["max_data_extension_time_in_days"] == 10
    cursor.execute(lifecycle.update_resource(sch.urn, {"max_data_extension_time_in_days": 9}, sch.props))
    result = data_provider.fetch_schema(cursor, sch.fqn)
    assert result["max_data_extension_time_in_days"] == 9
