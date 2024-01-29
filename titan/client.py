import os
import time

from typing import Union

import snowflake.connector

from snowflake.connector.cursor import SnowflakeCursor
from snowflake.connector.connection import SnowflakeConnection
from snowflake.connector.errors import ProgrammingError

from .builder import SQL

UNSUPPORTED_FEATURE = 2
ACCESS_CONTROL_ERR = 3001
DOEST_NOT_EXIST_ERR = 2003
ALREADY_EXISTS_ERR = 3041  # Not sure this is correct

connection_params = {
    "account": os.environ.get("SNOWFLAKE_ACCOUNT"),
    "user": os.environ.get("SNOWFLAKE_USER"),
    "password": os.environ.get("SNOWFLAKE_PASSWORD"),
}


def get_session():
    # TODO: make this snowpark-compatible
    return snowflake.connector.connect(**connection_params)


def _execute(conn_or_cursor: Union[SnowflakeConnection, SnowflakeCursor], sql, use_role=None) -> list:
    if isinstance(sql, str):
        sql_text = sql
    elif isinstance(sql, SQL):
        sql_text = str(sql)
        use_role = use_role or sql.use_role

    if isinstance(conn_or_cursor, SnowflakeConnection):
        session = conn_or_cursor
        cur = session.cursor(snowflake.connector.DictCursor)
    elif isinstance(conn_or_cursor, SnowflakeCursor):
        session = conn_or_cursor.connection
        cur = conn_or_cursor
        cur._use_dict_result = True
    else:
        # Undocumented snowpark-specific type snowflake.connector.connection.StoredProcConnection
        # raise Exception(f"Unknown connection type: {type(conn_or_cursor)}, {conn_or_cursor}")
        session = conn_or_cursor
        cur = session.cursor(snowflake.connector.DictCursor)

    try:
        if use_role:
            print(f"[{session.user}:{session.role}] >", f"USE ROLE {use_role}")
            cur.execute(f"USE ROLE {use_role}")
        print(f"[{session.user}:{session.role}] >", sql_text, end="")
        start = time.time()
        result = cur.execute(sql_text).fetchall()
        print(f"    \033[94m({len(result)} rows, {time.time() - start:.2f}s)\033[0m", flush=True)
        return result
    except ProgrammingError as err:
        # if err.errno == ACCESS_CONTROL_ERR:
        #     raise Exception(f"Access control error: {err.msg}")
        print(f"    \033[31m(err {err.errno}, {time.time() - start:.2f}s)\033[0m", flush=True)
        raise ProgrammingError(f"failed to execute sql, [{sql_text}]", errno=err.errno) from err


# TODO: fix this
# @lru_cache
def _execute_cached(session, sql, use_role=None) -> list:
    return _execute(session, sql, use_role)


def execute(session, sql, use_role=None, cacheable=False) -> list:
    if cacheable:
        return _execute_cached(session, sql, use_role)
    return _execute(session, sql, use_role)
