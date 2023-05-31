import os


# import snowflake.connector
# conn = snowflake.connector.connect(**connection_params)


from snowflake.snowpark import Session

connection_params = {}


def get_session(account):
    return Session.builder.configs(account=account, **connection_params).create()
