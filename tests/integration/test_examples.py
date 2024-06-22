import pytest
import yaml

from tests.helpers import get_examples_yml

from titan.blueprint import Blueprint
from titan.gitops import collect_resources_from_config

EXAMPLES_YML = list(get_examples_yml())


@pytest.fixture(
    params=EXAMPLES_YML,
    ids=[example_name for example_name, _ in EXAMPLES_YML],
    scope="function",
)
def example(request):
    _, example_content = request.param
    yield yaml.safe_load(example_content)


@pytest.mark.skip("TODO: This test is failing")
@pytest.mark.requires_snowflake
def test_example(example, cursor, marked_for_cleanup):
    cursor.execute("USE WAREHOUSE CI")

    resources = collect_resources_from_config(example)
    for resource in resources:
        marked_for_cleanup.append(resource)
    blueprint = Blueprint(
        name="test-example",
        resources=resources,
        dry_run=False,
        ignore_ownership=False,
    )
    plan = blueprint.plan(cursor.connection)
    blueprint.apply(cursor.connection, plan)
