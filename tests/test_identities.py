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
    data: dict = data.copy()
    instance = resource_cls(**data)
    serialized: dict = instance.to_dict()
    if "name" in serialized:
        assert ResourceName(serialized.pop("name")) == ResourceName(data.pop("name"))
    if "columns" in serialized:
        lhs_cols = serialized.pop("columns", [])
        rhs_cols = data.pop("columns", [])
        assert len(lhs_cols) == len(rhs_cols)
        for lhs, rhs in zip(lhs_cols, rhs_cols):
            if "name" in lhs:
                assert ResourceName(lhs.pop("name")) == ResourceName(rhs.pop("name"))
            assert lhs == rhs
    assert serialized == data


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
