import os

import snowflake.connector
from snowflake.snowpark import Session

connection_params = {}

conn = snowflake.connector.connect(**connection_params)


def get_session():
    return Session.builder.configs(connection_params).create()
