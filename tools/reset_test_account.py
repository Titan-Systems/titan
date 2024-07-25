import os
import time

import snowflake.connector
import yaml
from dotenv import dotenv_values

from titan import resources as res
from titan.blueprint import Blueprint, print_plan
from titan.data_provider import fetch_session
from titan.gitops import collect_resources_from_config


def read_config(file):
    config_path = os.path.join(os.path.dirname(__file__), file)
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    return config


def configure_test_account(conn):
    # "test_account.yml"
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


def configure_aws_heavy(conn):
    bp = Blueprint(
        name="reset-test-account",
        run_mode="CREATE-OR-UPDATE",
    )

    roles = [res.Role(name=f"ROLE_{i}") for i in range(50)]
    databases = []
    for i in range(10):
        database = res.Database(name=f"DATABASE_{i}")
        bp.add(database)
        databases.append(database)
        for role in roles:
            # bp.add(res.Grant(priv="USAGE", to=role, on=database))
            pass
        for j in range(10):
            schema = res.Schema(name=f"SCHEMA_{j}", database=database)
            bp.add(schema)
            for role in roles:
                # bp.add(res.Grant(priv="USAGE", to=role, on=schema))
                pass
            for k in range(5):
                table = res.Table(
                    name=f"TABLE_{k}", columns=[{"name": "ID", "data_type": "NUMBER(38, 0)"}], schema=schema
                )
                bp.add(table)
                for role in roles:
                    # bp.add(res.Grant(priv="SELECT", to=role, on=table))
                    pass

    bp.add(roles)

    staged_count = len(bp._staged)

    plan = bp.plan(conn)
    print_plan(plan[:10])
    print("Changes in plan:", len(plan))
    print("Staged resources:", staged_count)
    bp.apply(conn, plan)

    bp = Blueprint(
        name="reset-test-account",
        run_mode="CREATE-OR-UPDATE",
    )
    for database in databases:
        for role in roles:
            bp.add(res.Grant(priv="USAGE", to=role.name, on_database=database.name))
            bp.add(res.GrantOnAll(priv="USAGE", to=role.name, on_all_schemas_in_database=database.name))
            bp.add(res.GrantOnAll(priv="SELECT", to=role.name, on_all_tables_in_database=database.name))
    bp.apply(conn)


def get_connection(env_vars):
    return snowflake.connector.connect(
        account=env_vars["SNOWFLAKE_ACCOUNT"],
        user=env_vars["SNOWFLAKE_USER"],
        password=env_vars["SNOWFLAKE_PASSWORD"],
        role=env_vars["SNOWFLAKE_ROLE"],
        warehouse=env_vars["SNOWFLAKE_WAREHOUSE"],
    )


def configure_test_accounts():

    # for account in ["aws.standard", "aws.enterprise"]:
    #     env_vars = dotenv_values(f".env.{account}")
    #     conn = get_connection(env_vars)
    #     try:
    #         configure_test_account(conn)
    #     except Exception as e:
    #         print(f"Error configuring {account}: {e}")
    #     finally:
    #         conn.close()

    now = time.time()
    configure_aws_heavy(get_connection(dotenv_values(".env.aws.heavy")))
    print(f"done in {time.time() - now:.2f}s")


if __name__ == "__main__":
    configure_test_accounts()
