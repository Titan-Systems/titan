import pytest
import yaml

from tests.helpers import get_examples_yml

from titan.blueprint import Blueprint
from titan.enums import ResourceType
from titan.gitops import collect_blueprint_config

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

    blueprint_config = collect_blueprint_config(example)
    for resource in blueprint_config["resources"]:
        marked_for_cleanup.append(resource)
    blueprint = Blueprint(**blueprint_config)
    plan = blueprint.plan(cursor.connection)
    cmds = blueprint.apply(cursor.connection, plan)
    assert cmds

    blueprint = Blueprint(**blueprint_config)
    plan = blueprint.plan(cursor.connection)
    unexpected_drift = [change for change in plan if not change_is_expected(change)]
    assert len(unexpected_drift) == 0


def change_is_expected(change):
    return change.urn.resource_type == ResourceType.GRANT and change.after.get("priv", "") == "ALL"
