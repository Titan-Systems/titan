import json
import os

import titan.resources as resources

from titan.resources import Resource
from titan.enums import ResourceType
from titan.parse import _split_statements


FIXTURES_DIR = os.path.join(os.path.dirname(__file__), "fixtures")

STATIC_RESOURCES = {
    ResourceType.DATABASE: resources.Database(name="static_database"),
    ResourceType.NETWORK_RULE: resources.NetworkRule(
        name="static_network_rule",
        type=resources.network_rule.NetworkIdentifierType.HOST_PORT,
        value_list=["example.com:443"],
        mode=resources.network_rule.NetworkRuleMode.EGRESS,
    ),
    ResourceType.ROLE: resources.Role(name="static_role"),
    ResourceType.SECRET: resources.Secret(
        name="static_secret", type=resources.secret.SecretType.PASSWORD, username="someuser", password="somepass"
    ),
    ResourceType.SCHEMA: resources.Schema(name="static_schema"),
    ResourceType.STAGE: resources.InternalStage(
        name="static_stage", directory={"enable": True, "refresh_on_create": True}
    ),
    ResourceType.STREAM: resources.TableStream(name="static_stream", on_table="static_table"),
    ResourceType.TABLE: resources.Table(name="static_table", columns=[{"name": "id", "data_type": "INT"}]),
    ResourceType.TAG: resources.Tag(name="static_tag"),
    ResourceType.USER: resources.User(name="static_user"),
    ResourceType.VIEW: resources.View(
        name="static_view", columns=[{"name": "id", "data_type": "INT"}], as_="SELECT id FROM static_table"
    ),
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
    for f in sorted(files):
        if f.endswith(".json"):
            resource_name = f.split(".")[0]
            try:
                resource_cls = _get_resource_cls(resource_name)
            except ValueError:
                continue
            try:
                data = get_json_fixture(resource_name)
                yield (resource_cls, data)
            except Exception:
                continue


def get_sql_fixtures():
    files = os.listdir(os.path.join(FIXTURES_DIR, "sql"))
    for f in sorted(files):
        if f.endswith(".sql"):
            resource_name = f.split(".")[0]
            try:
                resource_cls = _get_resource_cls(resource_name)
            except ValueError:
                continue
            try:
                idx = 1
                for fixture in get_sql_fixture(f):
                    yield (resource_cls, fixture, idx)
                    idx += 1
            except Exception:
                continue


def get_sql_fixture(filename, lines=False):
    with open(os.path.join(FIXTURES_DIR, "sql", filename), encoding="utf-8") as f:
        if lines:
            yield from f.read().splitlines()
        else:
            yield from _split_statements(f.read())
