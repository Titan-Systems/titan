import os
import snowflake.connector

from snowflake.connector.errors import ProgrammingError

from .builder import SQL

connection_params = {
    "account": os.environ.get("SNOWFLAKE_ACCOUNT"),
    "user": os.environ.get("SNOWFLAKE_USER"),
    "password": os.environ.get("SNOWFLAKE_PASSWORD"),
}


def get_session():
    # TODO: make this snowpark-compatible
    return snowflake.connector.connect(**connection_params)


def _execute(session, sql, use_role=None) -> list:
    if isinstance(sql, str):
        sql_text = sql
    elif isinstance(sql, SQL):
        sql_text = str(sql)
        use_role = use_role or sql.use_role

    with session.cursor(snowflake.connector.DictCursor) as cur:
        try:
            if use_role:
                print(f"[{session.user}:{session.role}] >>>", f"USE ROLE {use_role}")
                cur.execute(f"USE ROLE {use_role}")
            print(f"[{session.user}:{session.role}] >>>", sql_text)
            result = cur.execute(sql_text).fetchall()
            return result
        except ProgrammingError as err:
            # if err.errno == ACCESS_CONTROL_ERR:
            #     raise Exception(f"Access control error: {err.msg}")
            raise ProgrammingError(f"failed to execute sql, [{sql_text}]", errno=err.errno) from err


# TODO: fix this
# @lru_cache
def _execute_cached(session, sql, use_role=None) -> list:
    return _execute(session, sql, use_role)


def execute(session, sql, use_role=None, cacheable=False) -> list:
    if cacheable:
        return _execute_cached(session, sql, use_role)
    return _execute(session, sql, use_role)
