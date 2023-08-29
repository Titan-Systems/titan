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


def _execute(session, sql, use_role=None) -> list:
    with session.cursor(snowflake.connector.DictCursor) as cur:
        try:
            if use_role:
                cur.execute(f"USE ROLE {use_role}")
            print(f"[{session.role}] >>>", sql)
            result = cur.execute(sql).fetchall()
            return result
        except ProgrammingError as err:
            # if err.errno == ACCESS_CONTROL_ERR:
            #     raise Exception(f"Access control error: {err.msg}")
            raise ProgrammingError(f"failed to execute sql, [{sql}]", errno=err.errno) from err


# TODO: fix this
# @lru_cache
def _execute_cached(session, sql, use_role=None) -> list:
    return _execute(session, sql, use_role)


def execute(session, sql, use_role=None, cacheable=False) -> list:
    if cacheable:
        return _execute_cached(session, sql, use_role)
    return _execute(session, sql, use_role)
