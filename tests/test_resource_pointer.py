import pytest

import titan.resources as res

from titan.enums import ResourceType
from titan.resources.resource import ResourcePointer


def test_fqn_construction():
    ptr = ResourcePointer(name="my_db.my_schema.my_table", resource_type=ResourceType.TABLE)
    assert ptr.name == "my_table"
    assert ptr.container.name == "my_schema"
    assert ptr.container.container.name == "my_db"


def test_resource_pointer_type_checking():
    invalid_pointer = ResourcePointer(name="my_network_rule", resource_type=ResourceType.DATABASE)
    with pytest.raises(TypeError):
        res.NetworkPolicy(name="my_network_policy", allowed_network_rule_list=[invalid_pointer])

    with pytest.raises(TypeError):
        res.NetworkPolicy(name="my_network_policy", allowed_network_rule_list=[111])
