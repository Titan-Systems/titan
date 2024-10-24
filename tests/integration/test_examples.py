import pytest
import yaml

from tests.helpers import dump_resource_change, get_examples_yml
from titan.blueprint import Blueprint
from titan.enums import ResourceType
from titan.gitops import collect_blueprint_config

EXAMPLES_YML = list(get_examples_yml())
VARS = {
    "for-each-example": {
        "schemas": [
            "schema1",
            "schema2",
            "schema3",
        ]
    },
}


@pytest.fixture(
    params=EXAMPLES_YML,
    ids=[example_name for example_name, _ in EXAMPLES_YML],
    scope="function",
)
def example(request):
    example_name, example_content = request.param
    yield example_name, yaml.safe_load(example_content)


@pytest.mark.enterprise
@pytest.mark.requires_snowflake
def test_example(example, cursor, marked_for_cleanup, blueprint_vars):
    example_name, example_content = example
    blueprint_vars = VARS.get(example_name, blueprint_vars)

    if example_name == "dbt-with-schema-access-role-tree":
        pytest.skip("Skipping until issues are resolved")

    cursor.execute("USE WAREHOUSE CI")
    blueprint_config = collect_blueprint_config(example_content.copy(), {"vars": blueprint_vars})
    assert blueprint_config.resources is not None
    for resource in blueprint_config.resources:
        marked_for_cleanup.append(resource)
    blueprint = Blueprint.from_config(blueprint_config)
    plan = blueprint.plan(cursor.connection)
    cmds = blueprint.apply(cursor.connection, plan)
    assert cmds

    blueprint_config = collect_blueprint_config(example_content.copy(), {"vars": blueprint_vars})
    blueprint = Blueprint.from_config(blueprint_config)
    plan = blueprint.plan(cursor.connection)
    unexpected_drift = [change for change in plan if not change_is_expected(change)]
    if len(unexpected_drift) > 0:
        debug = "\n".join([dump_resource_change(change) for change in unexpected_drift])
        assert False, f"Unexpected drift:\n{debug}"


def change_is_expected(change):
    return change.urn.resource_type == ResourceType.GRANT and change.after.get("priv", "") == "ALL"
