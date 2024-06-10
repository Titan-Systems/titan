import os

import snowflake.connector
from titan.blueprint import Blueprint


connection_params = {
    "account": os.environ["TEST_SNOWFLAKE_ACCOUNT"],
    "user": os.environ["TEST_SNOWFLAKE_USER"],
    "password": os.environ["TEST_SNOWFLAKE_PASSWORD"],
    "role": os.environ["TEST_SNOWFLAKE_ROLE"],
}


def main():
    conn = snowflake.connector.connect(**connection_params)
    bp = Blueprint(name="reset-test-account", run_mode="FULLY-MANAGED")
    bp.apply(conn)


if __name__ == "__main__":
    main()
