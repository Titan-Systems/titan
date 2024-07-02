import pytest

from tests.helpers import get_json_fixtures
from titan import resources as res

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
