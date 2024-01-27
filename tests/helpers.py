import json
import os

import titan.resources as resources

from titan.resources import Resource
from titan.enums import ResourceType
from titan.parse import _split_statements


FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")

STATIC_RESOURCES = {
    ResourceType.DATABASE: resources.Database(name="static_database"),
    ResourceType.ROLE: resources.Role(name="static_role"),
    ResourceType.TABLE: resources.Table(name="static_table", columns=[{"name": "id", "data_type": "INT"}]),
    ResourceType.TAG: resources.Tag(name="static_tag"),
    ResourceType.USER: resources.User(name="static_user"),
    ResourceType.WAREHOUSE: resources.Warehouse(name="static_warehouse"),
}


def _get_resource_cls(resource_name):
    resource_name = resource_name.replace("_", "")
    for resource_cls in Resource.__subclasses__():
        if resource_cls.__name__.lower() == resource_name:
            return resource_cls
    raise ValueError(f"Resource class {resource_name} not found")


def get_json_fixture(resource_name):
    with open(os.path.join(FIXTURES_DIR, "json", f"{resource_name}.json"), "r") as file:
        content = file.read()
        return json.loads(content)


def get_json_fixtures():
    files = os.listdir(os.path.join(FIXTURES_DIR, "json"))
    for f in files:
        if f.endswith(".json"):
            resource_name = f.split(".")[0]
            try:
                resource_cls = _get_resource_cls(resource_name)
            except ValueError as e:
                print(f"Error reading {f}: {e}")
                continue
            try:
                data = get_json_fixture(resource_name)
            except Exception as e:
                print(f"Error reading {f}: {e}")
                continue
            yield (resource_cls, data)


def list_sql_fixtures():
    files = os.listdir(os.path.join(FIXTURES_DIR, "sql"))
    for f in files:
        if f.endswith(".sql"):
            yield f


def load_sql_fixtures(filename, lines=False):
    with open(os.path.join(FIXTURES_DIR, filename), encoding="utf-8") as f:
        if lines:
            yield from f.read().splitlines()
        else:
            yield from _split_statements(f.read())
