from typing import Any

from titan.blueprint import Blueprint
from titan.blueprint import plan_from_dict
from titan.blueprint_config import BlueprintConfig

from titan.gitops import collect_blueprint_config
from titan.operations.connector import connect


def blueprint_plan(yaml_config: dict, cli_config: dict[str, Any]):
    blueprint_config = collect_blueprint_config(yaml_config, cli_config)
    blueprint = Blueprint.from_config(blueprint_config)
    session = connect()
    plan_obj = blueprint.plan(session)
    return plan_obj


def blueprint_apply(yaml_config: dict, cli_config: dict):
    blueprint_config = collect_blueprint_config(yaml_config, cli_config)
    blueprint = Blueprint.from_config(blueprint_config)
    session = connect()
    blueprint.apply(session)


def blueprint_apply_plan(plan_dict: dict, cli_config: dict):
    blueprint_config = BlueprintConfig(**cli_config)
    blueprint = Blueprint.from_config(blueprint_config)
    plan = plan_from_dict(plan_dict)
    session = connect()
    blueprint.apply(session, plan)
