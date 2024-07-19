import os
import yaml

import snowflake.connector
from titan.blueprint import Blueprint, print_plan
from titan.data_provider import fetch_session
from titan.gitops import collect_resources_from_config

connection_params = {
    "account": os.environ["SNOWFLAKE_ACCOUNT"],
    "user": os.environ["SNOWFLAKE_USER"],
    "password": os.environ["SNOWFLAKE_PASSWORD"],
    "role": os.environ["SNOWFLAKE_ROLE"],
}


def read_config(file):
    config_path = os.path.join(os.path.dirname(__file__), file)
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    return config


def main():
    # "test_account.yml"

    conn = snowflake.connector.connect(**connection_params)
    session_ctx = fetch_session(conn)

    is_enterprise = session_ctx["tag_support"]

    # check if account is standard/enterprise, probably by checking tag status, then dynamically pop tag/secret if standard

    allowlist = [
        "catalog integration",
        "compute pool",
        "database role",
        "database",
        "grant",
        "network policy",
        "network rule",
        "resource monitor",
        "role grant",
        "role",
        "schema",
        "security integration",
        "share",
        "stage",
        "storage integration",
        "stream",
        "table",
        "user",
        "view",
        "warehouse",
    ]

    config = read_config("test_account.yml")

    if is_enterprise:
        allowlist.extend(
            [
                "secret",
                "tag",
                "tag reference",
            ]
        )
    else:
        config.pop("tags")
        config.pop("secrets")
        config.pop("tag_references")

    resources = collect_resources_from_config(config)

    bp = Blueprint(
        name="reset-test-account",
        run_mode="SYNC-ALL",
        allowlist=allowlist,
        resources=resources,
    )
    plan = bp.plan(conn)
    print_plan(plan)
    bp.apply(conn, plan)


if __name__ == "__main__":
    main()
