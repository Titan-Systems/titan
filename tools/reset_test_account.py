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


def load_and_run_config(conn, file, run_mode, valid_resource_types):
    config_path = os.path.join(os.path.dirname(__file__), file)
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    bp = Blueprint(
        name="reset-test-account",
        run_mode=run_mode,
        valid_resource_types=valid_resource_types,
        resources=collect_resources_from_config(config),
    )
    plan = bp.plan(conn)
    print_plan(plan)
    bp.apply(conn, plan)


def main():
    # "test_account.yml"

    conn = snowflake.connector.connect(**connection_params)

    load_and_run_config(
        conn,
        "test_account_users.yml",
        "CREATE-OR-UPDATE",
        ["user"],
    )
    load_and_run_config(
        conn,
        "test_account.yml",
        "FULLY-MANAGED",
        [
            "compute pool",
            "database",
            "grant",
            "network rule",
            "role grant",
            "role",
            "schema",
            "security integration",
            "stage",
            "stream",
            "table",
            "view",
            "warehouse",
        ],
    )

    load_and_run_config(
        conn,
        "test_account_enterprise.yml",
        "FULLY-MANAGED",
        ["tag"],
    )


if __name__ == "__main__":
    main()
