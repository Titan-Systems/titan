import pytest

from inflection import pluralize

from titan.gitops import collect_resources_from_config
from titan.identifiers import resource_label_for_type

from tests.helpers import get_json_fixtures

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
    resources = collect_resources_from_config(database_config)
    assert len(resources) == 2


# JSON_FIXTURES doesn't include things like role grants yet
# def test_resource_config(resource_config):
#     resources = collect_resources_from_config(resource_config)
#     resource_types = set([resource.resource_type for resource in resources])
#     expected_resource_types = set([resource_cls.resource_type for resource_cls, _ in JSON_FIXTURES])
#     assert resource_types == expected_resource_types
