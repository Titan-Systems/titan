import pytest

from titan import resources as res
from titan.enums import ResourceType
from titan.resources.resource import ResourcePointer


def test_from_sql_fqn_parsing():
    grant = res.Grant.from_sql('GRANT USAGE ON SCHEMA "My_databasE".my_schema TO ROLE my_role')
    assert isinstance(grant.to, ResourcePointer)
    assert grant.to.resource_type == ResourceType.ROLE
    assert grant.to.name == "my_role"
    assert grant.on == '"My_databasE".MY_SCHEMA'
    schema_ref = None
    for ref in grant.refs:
        if ref.resource_type == ResourceType.SCHEMA and ref.name == "my_schema":
            schema_ref = ref
    assert schema_ref
    assert schema_ref.container.name == '"My_databasE"'
