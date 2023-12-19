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
    elif attr == "managed_access":
        return tidy_sql("ALTER SCHEMA", fqn, "ENABLE" if new_value else "DISABLE", "MANAGED ACCESS")
    else:
        new_value = f"'{new_value}'" if isinstance(new_value, str) else new_value
        return tidy_sql("ALTER SCHEMA", fqn, "SET", attr, "=", new_value)


_schema_defaults = {
    "database": None,
    "transient": False,
    "owner": "SYSADMIN",
    "managed_access": False,
    "data_retention_time_in_days": None,
    "max_data_extension_time_in_days": 14,
    "default_ddl_collation": None,
    "tags": None,
    "comment": None,
}


def create_or_update_schema(sp_session, config: dict = None, yaml: str = None, dry_run: bool = False):
    """
    Takes configuration (either as an OBJECT or a YAML string) and creates or updates a schema.
    Use the `dry_run` parameter to test the operation without executing any SQL.

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
    managed_access : BOOLEAN
        If True, the schema is managed
    """
    if yaml and config is None:
        config = safe_load(yaml)
    db = config.get("database", sp_session.get_current_database())
    fqn = FQN(database=db, name=config["name"])
    urn = str(URN(resource_key="schema", fqn=fqn))
    schema = dp.fetch_schema(sp_session.connection, fqn)
    sql = []
    if schema:
        resource = {urn: dp.remove_none_values(schema)}
        data = {urn: dp.remove_none_values(_schema_defaults | config)}
        # return {"res": resource, "data": data}
        for _, _, change in diff(resource, data):
            sql.append(_update_schema_sql(fqn, change))
    else:
        sql = [_create_schema_sql(fqn, config, if_not_exists=True)]
    if not dry_run:
        _execute(sp_session, sql)
    return {"sql": sql}


def fetch_schema(sp_session, name) -> dict:
    """
    Returns a schema's configuration.
    """
    fqn = FQN.from_str(name, resource_key="schema")
    if fqn.database is None:
        fqn.database = sp_session.get_current_database()
    return dp.fetch_schema(sp_session.connection, fqn)
