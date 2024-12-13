import pytest

from titan.identifiers import URN, parse_FQN
from titan.operations.export import export_resources, _format_resource_config
from titan.enums import ResourceType
from titan.data_provider import fetch_resource

pytestmark = pytest.mark.requires_snowflake


def test_export_all(cursor):
    assert export_resources(session=cursor.connection)


def test_export_schema(cursor):
    urn = URN(ResourceType.SCHEMA, parse_FQN("STATIC_DATABASE.STATIC_SCHEMA", is_db_scoped=True))
    resource = fetch_resource(cursor, urn)
    assert resource
    resource_cfg = _format_resource_config(urn, resource, ResourceType.SCHEMA)
    assert resource_cfg
    assert "database" in resource_cfg
    assert resource_cfg["database"] == "STATIC_DATABASE"
