from titan.blueprint import Blueprint
from titan.gitops import collect_resources_from_config
from titan.operations.connector import connect


def blueprint_plan(config, run_mode):
    session = connect()
    resources = collect_resources_from_config(config)
    blueprint = Blueprint(
        resources=resources,
        run_mode=run_mode,
    )
    plan_obj = blueprint.plan(session)
    return plan_obj


def blueprint_apply(plan, run_mode, dry_run):
    session = connect()
    blueprint = Blueprint(
        run_mode=run_mode,
        dry_run=dry_run,
    )
    blueprint.apply(session, plan)
