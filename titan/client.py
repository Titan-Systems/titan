import logging
import os
import time

from typing import Union

import snowflake.connector

from snowflake.connector.cursor import SnowflakeCursor
from snowflake.connector.connection import SnowflakeConnection
from snowflake.connector.errors import ProgrammingError

logger = logging.getLogger("titan")

UNSUPPORTED_FEATURE = 2
OBJECT_ALREADY_EXISTS_ERR = 2002
DOES_NOT_EXIST_ERR = 2003
OBJECT_DOES_NOT_EXIST_ERR = 2043
ACCESS_CONTROL_ERR = 3001
ALREADY_EXISTS_ERR = 3041  # Not sure this is correct
INVALID_GRANT_ERR = 3042
FEATURE_NOT_ENABLED_ERR = 3078  # Unsure if this is just Replication Groups or not

connection_params = {
    "account": os.environ.get("SNOWFLAKE_ACCOUNT"),
    "user": os.environ.get("SNOWFLAKE_USER"),
    "password": os.environ.get("SNOWFLAKE_PASSWORD"),
}

_EXECUTION_CACHE = {}


def reset_cache():
    global _EXECUTION_CACHE
    _EXECUTION_CACHE = {}


def execute(
    conn_or_cursor: Union[SnowflakeConnection, SnowflakeCursor],
    sql: str,
    cacheable: bool = False,
) -> list:
    if isinstance(sql, str):
        sql_text = sql
    else:
        raise Exception(f"Unknown sql type: {type(sql)}, {sql}")

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

    if sql.startswith("USE ROLE"):
        desired_role = sql.split(" ")[-1]
        if desired_role == session.role:
            return

    if cacheable and session.role in _EXECUTION_CACHE and sql_text in _EXECUTION_CACHE[session.role]:
        print(f"[{session.user}:{session.role}] >", sql_text, end="")
        result = _EXECUTION_CACHE[session.role][sql_text]
        print(f"    \033[94m({len(result)} rows, cached)\033[0m", flush=True)
        return result

    try:
        print(f"[{session.user}:{session.role}] >", sql_text, end="")
        start = time.time()
        result = cur.execute(sql_text).fetchall()
        print(f"    \033[94m({len(result)} rows, {time.time() - start:.2f}s)\033[0m", flush=True)
        if cacheable:
            if session.role not in _EXECUTION_CACHE:
                _EXECUTION_CACHE[session.role] = {}
            _EXECUTION_CACHE[session.role][sql_text] = result
        return result
    except ProgrammingError as err:
        # if err.errno == ACCESS_CONTROL_ERR:
        #     raise Exception(f"Access control error: {err.msg}")
        print(f"    \033[31m(err {err.errno}, {time.time() - start:.2f}s)\033[0m", flush=True)
        raise ProgrammingError(f"failed to execute sql, [{sql_text}]", errno=err.errno) from err
