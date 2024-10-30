import copy

import pytest

from dataclasses import fields
from typing import Any, get_args, get_origin

from tests.helpers import get_json_fixtures
from titan.data_types import convert_to_canonical_data_type
from titan.enums import AccountEdition
from titan.resources import Resource
from titan.resource_name import ResourceName
from titan.role_ref import RoleRef

JSON_FIXTURES = list(get_json_fixtures())


def remove_none_values(d):
    new_dict = {}
    for k, v in d.items():
        if isinstance(v, dict):
            new_dict[k] = remove_none_values(v)
        elif isinstance(v, list) and len(v) > 0 and isinstance(v[0], dict):
            new_dict[k] = [remove_none_values(item) for item in v if item is not None]
        elif v is not None:
            new_dict[k] = v
    return new_dict


@pytest.fixture(
    params=JSON_FIXTURES,
    ids=[resource_cls.__name__ for resource_cls, _ in JSON_FIXTURES],
    scope="function",
)
def resource(request):
    resource_cls, data = request.param
    yield resource_cls, data


def _field_type_is_serialized_as_resource_name(field):
    if field.type is Any:
        return False
    if field.type is RoleRef:
        return True
    if field.type is ResourceName:
        return True
    elif isinstance(field.type, str) and field.name == "owner" and field.type == "Role":
        return True
    elif issubclass(field.type, Resource):
        return field.type.serialize_inline is False
    elif get_origin(field.type) is list:
        field_item_type = get_args(field.type)[0]
        if issubclass(field_item_type, Resource):
            return field_item_type.serialize_inline is False

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
            if "data_type" in lhs and "data_type" in rhs:
                assert convert_to_canonical_data_type(lhs.pop("data_type")) == convert_to_canonical_data_type(
                    rhs.pop("data_type")
                )
            assert remove_none_values(lhs) == remove_none_values(rhs)
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
        if _field_type_is_serialized_as_resource_name(field):
            assert _resource_names_are_eq(serialized.pop(field.name), data.pop(field.name))
        if isinstance(serialized.get(field.name, None), list):
            qqq = serialized.pop(field.name)
            www = data.pop(field.name)
            assert len(qqq) == len(www)
            for lhs, rhs in zip(qqq, www):
                assert lhs == rhs
    assert serialized == data


def test_sql_identity(resource: tuple[type[Resource], dict]):
    resource_cls, data = resource
    if resource_cls.__name__ == "ScannerPackage":
        pytest.skip("Skipping scanner package")
    instance = resource_cls(**data)
    sql = instance.create_sql(AccountEdition.ENTERPRISE)
    new = resource_cls.from_sql(sql)
    new_dict = new.to_dict(AccountEdition.ENTERPRISE)
    instance_dict = instance.to_dict(AccountEdition.ENTERPRISE)
    if "name" in new_dict:
        assert ResourceName(new_dict.pop("name")) == ResourceName(instance_dict.pop("name"))

    assert new_dict == instance_dict
