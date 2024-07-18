import os
import yaml

import snowflake.connector
from titan.blueprint import Blueprint, print_plan
from titan.gitops import collect_resources_from_config

connection_params = {
    "account": os.environ["SNOWFLAKE_ACCOUNT"],
    "user": os.environ["SNOWFLAKE_USER"],
    "password": os.environ["SNOWFLAKE_PASSWORD"],
    "role": os.environ["SNOWFLAKE_ROLE"],
}


def load_and_run_config(conn, file, run_mode, allowlist):
    config_path = os.path.join(os.path.dirname(__file__), file)
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    bp = Blueprint(
        name="reset-test-account",
        run_mode=run_mode,
        allowlist=allowlist,
        resources=collect_resources_from_config(config),
        # dry_run=True,
    )
    plan = bp.plan(conn)
    print_plan(plan)
    bp.apply(conn, plan)


def main():
    # "test_account.yml"

    conn = snowflake.connector.connect(**connection_params)

    # check if account is standard/enterprise, probably by checking tag status, then dynamically pop tag/secret if standard

    load_and_run_config(
        conn,
        "test_account.yml",
        "SYNC-ALL",
        [
            "catalog integration",
            "compute pool",
            "database role",
            "database",
            "grant",
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
            "tag",
            "user",
            "view",
            "warehouse",
        ],
    )

    # load_and_run_config(
    #     conn,
    #     "test_account_enterprise.yml",
    #     "SYNC",
    #     ["tag"],
    # )


if __name__ == "__main__":
    main()
