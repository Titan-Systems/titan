import pytest

from titan.operations.export import export_resources

pytestmark = pytest.mark.requires_snowflake


def test_export_all(cursor):
    assert export_resources(session=cursor.connection)
