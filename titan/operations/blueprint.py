from titan.blueprint import Blueprint
from titan.gitops import collect_blueprint_config
from titan.operations.connector import connect


def blueprint_plan(config, run_mode):
    session = connect()
    blueprint_config = collect_blueprint_config(config)
    blueprint = Blueprint.from_config(blueprint_config)
    plan_obj = blueprint.plan(session)
    return plan_obj


def blueprint_apply(plan, run_mode, dry_run):
    session = connect()
    blueprint = Blueprint(
        run_mode=run_mode,
        dry_run=dry_run,
    )
    blueprint.apply(session, plan)
