import os

import snowflake.connector
import yaml
from dotenv import dotenv_values

from titan import resources as res
from titan.blueprint import Blueprint, print_plan
from titan.data_provider import fetch_session
from titan.enums import AccountEdition
from titan.gitops import collect_blueprint_config


def read_config(file) -> dict:
    config_path = os.path.join(os.path.dirname(__file__), file)
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    return config


def merge_configs(config1: dict, config2: dict) -> dict:
    merged = config1.copy()
    for key, value in config2.items():
        if key in merged:
            if isinstance(merged[key], list):
                merged[key] = merged[key] + value
            elif merged[key] is None:
                merged[key] = value
        else:
            merged[key] = value
    return merged


def configure_test_account(conn):
    session_ctx = fetch_session(conn)
    config = read_config("test_account.yml")
    vars = dotenv_values("env/.vars.test_account")
    print(vars)

    if session_ctx["account_edition"] == AccountEdition.ENTERPRISE:
        config = merge_configs(config, read_config("test_account_enterprise.yml"))

    blueprint_config = collect_blueprint_config(config, {"vars": vars})

    bp = Blueprint.from_config(blueprint_config)
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
                    name=f"TABLE_{k}", columns=[{"name": "ID", "data_type": "NUMBER(38,0)"}], schema=schema
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
        # warehouse=env_vars["SNOWFLAKE_WAREHOUSE"],
    )


def configure_test_accounts():

    for account in ["aws.standard", "aws.enterprise"]:
        print(">>>>>>>>>>>>>>>>", account)
        env_vars = dotenv_values(f"env/.env.{account}")
        conn = get_connection(env_vars)
        try:
            configure_test_account(conn)
        # except Exception as e:
        #     print(f"Error configuring {account}: {e}")
        finally:
            conn.close()

    # now = time.time()
    # configure_aws_heavy(get_connection(dotenv_values(".env.aws.heavy")))
    # print(f"done in {time.time() - now:.2f}s")


if __name__ == "__main__":
    configure_test_accounts()
