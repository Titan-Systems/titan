import copy

import pytest

from dataclasses import fields
from typing import get_args, get_origin

from tests.helpers import get_json_fixtures

from titan.resources import Resource
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


def _resource_field_type_is_resource(field):
    if isinstance(field.type, str) and field.name == "owner" and field.type == "Role":
        return True
    elif issubclass(field.type, Resource):
        return True
    elif get_origin(field.type) == list and issubclass(get_args(field.type)[0], Resource):
        return True

    return False


def _resource_names_are_eq(lhs, rhs):
    if lhs is None and rhs is None:
        return True
    if isinstance(lhs, list):
        return [ResourceName(item) for item in lhs] == [ResourceName(item) for item in rhs]
    else:
        return ResourceName(lhs) == ResourceName(rhs)


def test_data_identity(resource):
    resource_cls, data = resource
    data: dict = copy.deepcopy(data)
    instance = resource_cls(**data)

    serialized: dict = instance.to_dict()
    if "name" in serialized:
        assert _resource_names_are_eq(serialized.pop("name"), data.pop("name"))
    if "columns" in serialized:
        lhs_cols = serialized.pop("columns", []) or []
        rhs_cols = data.pop("columns", []) or []
        assert len(lhs_cols) == len(rhs_cols)
        for lhs, rhs in zip(lhs_cols, rhs_cols):
            if "name" in lhs:
                assert _resource_names_are_eq(lhs.pop("name"), rhs.pop("name"))
            assert lhs == rhs
    if "args" in serialized:
        lhs_args = serialized.pop("args", []) or []
        rhs_args = data.pop("args", []) or []
        assert len(lhs_args) == len(rhs_args)
        for lhs, rhs in zip(lhs_args, rhs_args):
            if "name" in lhs:
                assert _resource_names_are_eq(lhs.pop("name"), rhs.pop("name"))
            assert lhs == rhs
    for field in fields(instance._data):
        if field.name in ["name", "columns", "args"]:
            continue
        if _resource_field_type_is_resource(field):
            assert _resource_names_are_eq(serialized.pop(field.name), data.pop(field.name))

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
