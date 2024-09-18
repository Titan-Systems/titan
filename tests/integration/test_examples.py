import pytest
import yaml

from tests.helpers import dump_resource_change, get_examples_yml
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
def test_example(example, cursor, marked_for_cleanup, blueprint_vars):
    cursor.execute("USE WAREHOUSE CI")

    blueprint_config = collect_blueprint_config(example, {"vars": blueprint_vars})
    for resource in blueprint_config.resources:
        marked_for_cleanup.append(resource)
    blueprint = Blueprint.from_config(blueprint_config)
    plan = blueprint.plan(cursor.connection)
    cmds = blueprint.apply(cursor.connection, plan)
    assert cmds

    blueprint_config = collect_blueprint_config(example, {"vars": blueprint_vars})
    blueprint = Blueprint.from_config(blueprint_config)
    plan = blueprint.plan(cursor.connection)
    unexpected_drift = [change for change in plan if not change_is_expected(change)]
    if len(unexpected_drift) > 0:
        debug = "\n".join([dump_resource_change(change) for change in unexpected_drift])
        assert False, f"Unexpected drift:\n{debug}"


def change_is_expected(change):
    return change.urn.resource_type == ResourceType.GRANT and change.after.get("priv", "") == "ALL"
