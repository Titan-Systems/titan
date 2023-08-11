import os
import snowflake.connector

connection_params = {
    "account": os.environ["SNOWFLAKE_ACCOUNT"],
    "user": os.environ["SNOWFLAKE_USER"],
    "password": os.environ["SNOWFLAKE_PASSWORD"],
}


def get_session():
    return snowflake.connector.connect(**connection_params)


# from snowflake.snowpark import Session

# connection_params = {}


# def get_session(account):
#     return Session.builder.configs(account=account, **connection_params).create()
