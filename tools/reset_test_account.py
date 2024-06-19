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


def main():
    config_path = os.path.join(os.path.dirname(__file__), "test_account.yml")
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    conn = snowflake.connector.connect(**connection_params)
    resources = collect_resources_from_config(config)
    bp = Blueprint(
        name="reset-test-account",
        run_mode="FULLY-MANAGED",
        valid_resource_types=[
            "database",
            "role",
            "network rule",
            # "secret",
            "schema",
            "stage",
            # "table stream",
            "table",
            # "tag",
            # "user",
            "grant",
            "role grant",
            "view",
            "warehouse",
        ],
        resources=resources,
    )
    plan = bp.plan(conn)
    print_plan(plan)
    bp.apply(conn, plan)


if __name__ == "__main__":
    main()
