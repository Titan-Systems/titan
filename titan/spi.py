# Stored Procedure Interface (spi)

from yaml import safe_load

from snowflake.snowpark.exceptions import SnowparkSQLException

import titan.data_provider as dp
import titan.resource_props as props

from .builder import tidy_sql
from .diff import diff
from .identifiers import FQN, URN


def _execute(sp_session, sql: list):
    for sql_text in sql:
        try:
            sp_session.sql(sql_text).collect()
        except SnowparkSQLException as err:
            raise SnowparkSQLException(f"failed to execute sql, [{sql_text}]", error_code=err.error_code) from err


def _create_schema_sql(fqn, data, or_replace=False, if_not_exists=False):
    return tidy_sql(
        "CREATE",
        "OR REPLACE" if or_replace else "",
        "SCHEMA",
        "IF NOT EXISTS" if if_not_exists else "",
        fqn,
        props.schema_props.render(data),
    )


def _update_schema_sql(fqn, change):
    attr, new_value = change.popitem()
    attr = attr.lower()
    if new_value is None:
        return tidy_sql("ALTER SCHEMA", fqn, "UNSET", attr)
    elif attr == "name":
        return tidy_sql("ALTER SCHEMA", fqn, "RENAME TO", new_value)
    elif attr == "owner":
        raise NotImplementedError
    elif attr == "transient":
        raise Exception("Cannot change transient property of schema")
    elif attr == "with_managed_access":
        return tidy_sql("ALTER SCHEMA", fqn, "ENABLE" if new_value else "DISABLE", "MANAGED ACCESS")
    else:
        new_value = f"'{new_value}'" if isinstance(new_value, str) else new_value
        return tidy_sql("ALTER SCHEMA", fqn, "SET", attr, "=", new_value)


def create_or_update_schema(sp_session, config: dict = None, yaml: str = None, dry_run: bool = False):
    """
    Create or update a schema in Snowflake.

    Parameters
    ----------
    config : OBJECT
        A dictionary containing the schema configuration
    yaml : VARCHAR
        A YAML string containing the schema configuration
    dry_run : BOOLEAN
        If True, do not execute any SQL

    Schema Config
    -------------
    name : STRING
        The name of the schema
    database : STRING
        The name of the database in which to create the schema
    comment : STRING
        A comment to attach to the schema
    data_retention_time_in_days : INTEGER
        The number of days to retain data in the schema
    default_ddl_collation : STRING
        The default collation for the schema
    max_data_extension_time_in_days : INTEGER
        The maximum number of days to extend data in the schema
    owner : STRING
        The name of the user or role that owns the schema
    transient : BOOLEAN
        If True, the schema is transient
    with_managed_access : BOOLEAN
        If True, the schema is managed
    """
    if yaml and config is None:
        config = safe_load(yaml)
    sf_session = sp_session.connection
    db = config.get("database", sp_session.get_current_database())
    fqn = FQN(database=db, name=config["name"])
    urn = str(URN(resource_key="schema", fqn=fqn))
    res = {urn: dp.remove_none_values(dp.fetch_schema(sf_session, fqn))}
    data = {urn: config}
    sql = []
    dd = []
    if res:
        for action, urn_str, change in diff(res, data):
            dd.append((str(action), urn_str, change.copy()))
            sql.append(_update_schema_sql(fqn, change))
    else:
        sql = [_create_schema_sql(fqn, config, if_not_exists=True)]
    if not dry_run:
        _execute(sp_session, sql)
    return {"sql": sql, "diff": dd, "data": data, "res": res}


def fetch_schema(sp_session, name) -> dict:
    """
    Fetch a schema's configuration from Snowflake.
    """
    fqn = FQN.from_str(name, resource_key="schema")
    if fqn.database is None:
        fqn.database = sp_session.get_current_database()
    return dp.fetch_schema(sp_session.connection, fqn)
