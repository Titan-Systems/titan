import os
import pathlib

import click
import snowflake.connector

from titan.blueprint import Blueprint, print_plan
from titan.blueprint_config import print_blueprint_config
from titan.data_provider import fetch_session
from titan.enums import AccountEdition, AccountCloud, ResourceType
from titan.gitops import collect_blueprint_config, collect_vars_from_environment, merge_configs, read_config

SCRIPT_DIR = pathlib.Path(__file__).parent.resolve()


def get_connection():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ["SNOWFLAKE_ROLE"],
    )


def read_test_account_config(config_path: str):
    config = read_config(f"{SCRIPT_DIR}/test_account_configs/{config_path}")
    return config or {}


def get_config(session_ctx):
    config = read_test_account_config("base.yml")

    if session_ctx["account_edition"] == AccountEdition.ENTERPRISE:
        config = merge_configs(config, read_test_account_config("enterprise.yml"))
    elif session_ctx["account_edition"] == AccountEdition.BUSINESS_CRITICAL:
        config = merge_configs(config, read_test_account_config("business_critical.yml"))

    if session_ctx["cloud"] == AccountCloud.AWS:
        config = merge_configs(config, read_test_account_config("aws.yml"))
    elif session_ctx["cloud"] == AccountCloud.GCP:
        config = merge_configs(config, read_test_account_config("gcp.yml"))
    elif session_ctx["cloud"] == AccountCloud.AZURE:
        config = merge_configs(config, read_test_account_config("azure.yml"))
    else:
        raise ValueError(f"Unknown cloud: {session_ctx['cloud']}")
    return config


def reset_test_account():
    conn = get_connection()
    session_ctx = fetch_session(conn)
    config = get_config(session_ctx)
    titan_vars = collect_vars_from_environment()
    blueprint_config = collect_blueprint_config(config, {"vars": titan_vars})
    print_blueprint_config(blueprint_config)

    bp = Blueprint.from_config(blueprint_config)
    plan = bp.plan(conn)
    print_plan(plan)
    bp.apply(conn, plan)


def teardown_test_account():
    conn = get_connection()
    session_ctx = fetch_session(conn)
    config = get_config(session_ctx)
    titan_vars = collect_vars_from_environment()
    blueprint_config = collect_blueprint_config(config, {"vars": titan_vars})
    # will break when BlueprintConfig is frozen
    blueprint_config.resources = []
    blueprint_config.allowlist = [
        item
        for item in blueprint_config.allowlist
        if item not in [ResourceType.USER, ResourceType.ROLE_GRANT, ResourceType.WAREHOUSE]
    ]
    print_blueprint_config(blueprint_config)

    bp = Blueprint.from_config(blueprint_config)
    plan = bp.plan(conn)
    print_plan(plan)
    bp.apply(conn, plan)


@click.group()
def main():
    pass


@main.command()
def reset():
    reset_test_account()


@main.command()
def teardown():
    teardown_test_account()


@main.command("teardown-and-reset")
def teardown_and_reset():
    teardown_test_account()
    reset_test_account()


if __name__ == "__main__":
    main()
