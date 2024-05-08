import pytest

from tests.helpers import get_json_fixtures

from titan.resource_name import ResourceName

JSON_FIXTURES = list(get_json_fixtures())


@pytest.fixture(
    params=JSON_FIXTURES,
    ids=[resource_cls.__name__ for resource_cls, _ in JSON_FIXTURES],
    scope="function",
)
def resource(request):
    resource_cls, data = request.param
    yield resource_cls, data


def test_data_identity(resource):
    resource_cls, data = resource
    instance = resource_cls(**data)
    assert instance.to_dict() == data


def test_sql_identity(resource):
    resource_cls, data = resource
    instance = resource_cls(**data)
    sql = instance.create_sql()
    new = resource_cls.from_sql(sql)
    new_dict = new.to_dict()
    instance_dict = instance.to_dict()
    if "name" in new_dict:
        assert ResourceName(new_dict.pop("name")) == ResourceName(instance_dict.pop("name"))

    assert new_dict == instance_dict
