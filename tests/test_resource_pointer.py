from titan.enums import ResourceType
from titan.resources.resource import ResourcePointer


def test_fqn_construction():
    ptr = ResourcePointer(name="my_db.my_schema.my_table", resource_type=ResourceType.TABLE)
    assert ptr.name == "my_table"
    assert ptr.container.name == "my_schema"
    assert ptr.container.container.name == "my_db"
