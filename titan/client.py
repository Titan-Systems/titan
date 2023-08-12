import os
import snowflake.connector

connection_params = {
    "account": os.environ.get("SNOWFLAKE_ACCOUNT"),
    "user": os.environ.get("SNOWFLAKE_USER"),
    "password": os.environ.get("SNOWFLAKE_PASSWORD"),
}


def get_session():
    # TODO: make this snowpark-compatible
    return snowflake.connector.connect(**connection_params)
