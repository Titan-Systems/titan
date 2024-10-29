import pytest
from inflection import pluralize

from tests.helpers import get_json_fixtures
from titan.gitops import collect_blueprint_config
from titan.identifiers import resource_label_for_type

JSON_FIXTURES = list(get_json_fixtures())


@pytest.fixture
def database_config() -> dict:
    return {
        "databases": [
            {
                "name": "test_database",
                "comment": "test database",
                "schemas": [
                    {
                        "name": "test_schema",
                        "comment": "test schema",
                    }
                ],
            }
        ]
    }


@pytest.fixture
def resource_config() -> dict:
    config = {}
    for resource_cls, resource_config in JSON_FIXTURES:
        config[pluralize(resource_label_for_type(resource_cls.resource_type))] = [resource_config]

    return config


def test_database_config(database_config):
    blueprint_config = collect_blueprint_config(database_config)
    assert len(blueprint_config.resources) == 2


@pytest.mark.skip(reason="JSON_FIXTURES doesn't include things like role grants yet")
def test_resource_config(resource_config):
    resources = collect_blueprint_config(resource_config)
    resource_types = set([resource.resource_type for resource in resources])
    expected_resource_types = set([resource_cls.resource_type for resource_cls, _ in JSON_FIXTURES])
    assert resource_types == expected_resource_types


def test_grant_on_all_alias():
    config_base = {
        "grant_on_alls": [
            {
                "priv": "SELECT",
                "on_all_tables_in_schema": "sch",
                "to": "somerole",
            }
        ]
    }
    config_aliased = {
        "grants_on_all": [
            {
                "priv": "SELECT",
                "on_all_tables_in_schema": "sch",
                "to": "somerole",
            }
        ]
    }
    blueprint_config = collect_blueprint_config(config_base)
    blueprint_config_aliased = collect_blueprint_config(config_aliased)
    assert len(blueprint_config.resources) == 1
    assert len(blueprint_config_aliased.resources) == 1
    assert blueprint_config.resources[0]._data == blueprint_config_aliased.resources[0]._data


def test_vars_type_validation(database_config):
    yaml_config = {
        "vars": [{"name": "foo", "type": "string"}],
        **database_config,
    }
    cli_config = {
        "vars": {"foo": 42},
    }
    with pytest.raises(TypeError):
        collect_blueprint_config(yaml_config, cli_config)

    yaml_config = {
        "vars": [{"name": "foo", "type": "int", "default": 0}],
        **database_config,
    }
    cli_config = {
        "vars": {"foo": "bar"},
    }
    with pytest.raises(TypeError):
        collect_blueprint_config(yaml_config, cli_config)


def test_vars_defaults(database_config):
    config = {
        "vars": [{"name": "foo", "default": "bar", "type": "string"}],
        **database_config,
    }
    blueprint_config = collect_blueprint_config(config)
    assert blueprint_config.vars["foo"] == "bar"


def test_for_each():
    config = {
        "vars": [{"name": "some_list_var", "default": ["bar", "baz"], "type": "list"}],
        "roles": [
            {
                "for_each": "var.some_list_var",
                "name": "role_{{ each.value}}",
            }
        ],
    }
    blueprint_config = collect_blueprint_config(config)
    assert blueprint_config.resources is not None
    assert len(blueprint_config.resources) == 2
    assert [resource.urn.fqn.name for resource in blueprint_config.resources] == ["role_bar", "role_baz"]
