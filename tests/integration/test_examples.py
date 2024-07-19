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


@pytest.mark.enterprise
@pytest.mark.requires_snowflake
def test_example(example, cursor, marked_for_cleanup):
    cursor.execute("USE WAREHOUSE CI")

    resources = collect_resources_from_config(example)
    for resource in resources:
        marked_for_cleanup.append(resource)
    blueprint = Blueprint(
        name="test-example",
        resources=resources,
    )
    plan = blueprint.plan(cursor.connection)
    cmds = blueprint.apply(cursor.connection, plan)
    assert cmds

    blueprint = Blueprint(
        name="check-drift",
        resources=collect_resources_from_config(example),
    )
    plan = blueprint.plan(cursor.connection)
    assert not plan
