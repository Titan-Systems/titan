import pytest

from titan.operations.export import export_resources


def test_export_all(cursor):
    assert export_resources(session=cursor.connection)
