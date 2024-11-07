import os
import pathlib

import snowflake.connector

from titan.blueprint import Blueprint, print_plan
from titan.data_provider import fetch_session
from titan.enums import AccountEdition, AccountCloud
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
    return read_config(f"{SCRIPT_DIR}/test_account_configs/{config_path}")


def reset_test_account():
    conn = get_connection()
    session_ctx = fetch_session(conn)
    config = read_test_account_config("base.yml")
    # titan_vars = collect_vars_from_environment()
    from dotenv import dotenv_values

    titan_vars = dotenv_values("env/.vars.test_account")
    print("\n".join([f"{k}={v}" for k, v in titan_vars.items()]))

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

    blueprint_config = collect_blueprint_config(config, {"vars": titan_vars})

    bp = Blueprint.from_config(blueprint_config)
    plan = bp.plan(conn)
    print_plan(plan)
    bp.apply(conn, plan)


if __name__ == "__main__":
    reset_test_account()
