import pytest

from tests.helpers import get_json_fixtures
from titan import resources as res
from titan.enums import ResourceType
from titan.resources.resource import convert_role_ref
from titan.resources.resource import ResourcePointer

JSON_FIXTURES = list(get_json_fixtures())


@pytest.fixture(
    params=JSON_FIXTURES,
    ids=[resource_cls.__name__ for resource_cls, _ in JSON_FIXTURES],
    scope="function",
)
def json_fixture(request):
    resource_cls, data = request.param
    yield resource_cls, data


def test_resource_requires(json_fixture):
    role = res.Role(name="dummy")
    resource_cls, data = json_fixture
    resource = resource_cls(**data)
    assert role.requires(resource) is None
    assert resource in role.refs


def test_convert_role_ref():
    role_ref = convert_role_ref("dummy")
    assert isinstance(role_ref, ResourcePointer)
    assert role_ref.resource_type == ResourceType.ROLE
    assert role_ref.name == "dummy"

    database_role_ref = convert_role_ref("dummy.database_role")
    assert isinstance(database_role_ref, ResourcePointer)
    assert database_role_ref.resource_type == ResourceType.DATABASE_ROLE
    assert database_role_ref.name == "database_role"
    assert database_role_ref.container.name == "dummy"

    role = res.Role(name="dummy")
    assert convert_role_ref(role) == role
    database_role = res.DatabaseRole(name="dummy", database="dummy_db")
    assert convert_role_ref(database_role) == database_role
    role_ptr = ResourcePointer(name="dummy", resource_type=ResourceType.ROLE)
    assert convert_role_ref(role_ptr) == role_ptr
    database_role_ptr = ResourcePointer(name="dummy", resource_type=ResourceType.DATABASE_ROLE)
    assert convert_role_ref(database_role_ptr) == database_role_ptr

    with pytest.raises(TypeError):
        convert_role_ref(None)

    with pytest.raises(TypeError):
        convert_role_ref(111)

    with pytest.raises(TypeError):
        convert_role_ref(ResourcePointer(name="dummy", resource_type=ResourceType.DATABASE))
