import os

import pytest
from inflection import pluralize

from tests.helpers import get_json_fixtures
from titan import data_provider
from titan.identifiers import resource_label_for_type
from titan.resources import Database
from titan.scope import AccountScope, DatabaseScope, SchemaScope

pytestmark = pytest.mark.requires_snowflake

TEST_ROLE = os.environ.get("TEST_SNOWFLAKE_ROLE")
TEST_USER = os.environ.get("TEST_SNOWFLAKE_USER")

JSON_FIXTURES = list(get_json_fixtures())


@pytest.fixture(
    params=JSON_FIXTURES,
    ids=[resource_cls.__name__ for resource_cls, _ in JSON_FIXTURES],
    scope="function",
)
def resource(request, suffix):
    resource_cls, data = request.param
    if "name" in data:
        data["name"] += f"_{suffix}_list_resources"
    if "login_name" in data:
        data["login_name"] += f"_{suffix}_list_resources"
    res = resource_cls(**data)

    yield res


@pytest.fixture(scope="session")
def list_resources_database(cursor, suffix, marked_for_cleanup):
    db = Database(name=f"list_resources_test_database_{suffix}")
    cursor.execute(db.create_sql(if_not_exists=True))
    marked_for_cleanup.append(db)
    yield db


def test_list_resource(cursor, list_resources_database, resource, marked_for_cleanup):
    if isinstance(resource.scope, DatabaseScope):
        list_resources_database.add(resource)
    elif isinstance(resource.scope, SchemaScope):
        list_resources_database.public_schema.add(resource)

    if not hasattr(data_provider, f"list_{pluralize(resource_label_for_type(resource.resource_type))}"):
        pytest.skip(f"{resource.resource_type} is not supported")

    cursor.execute(resource.create_sql(if_not_exists=True))
    marked_for_cleanup.append(resource)
    list_resources = data_provider.list_resource(cursor, resource_label_for_type(resource.resource_type))
    assert len(list_resources) > 0
    assert resource.fqn in list_resources
