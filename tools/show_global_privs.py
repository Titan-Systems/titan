import os

import snowflake.connector
import yaml
from dotenv import dotenv_values


def get_connection(env_vars):
    return snowflake.connector.connect(
        account=env_vars["SNOWFLAKE_ACCOUNT"],
        user=env_vars["SNOWFLAKE_USER"],
        password=env_vars["SNOWFLAKE_PASSWORD"],
        role="ACCOUNTADMIN",
    )


def main():
    env_vars = dotenv_values("env/.env.aws.enterprise")
    conn = get_connection(env_vars)
    cursor = conn.cursor()
    for role in ["ACCOUNTADMIN", "SYSADMIN", "SECURITYADMIN", "USERADMIN"]:
        cursor.execute(f"SHOW GRANTS TO ROLE {role}")
        grants = cursor.fetchall()
        print(f"{role}:")
        for grant in grants:
            priv = grant[1]
            granted_on = grant[2]
            name = grant[3]
            granted_by = grant[7]
            if granted_by != "":
                continue
            print(f" - [{granted_on} : {name}] {priv}")
        print()


if __name__ == "__main__":
    main()
