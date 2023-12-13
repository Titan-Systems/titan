# Stored Procedure Interface (spi)

from yaml import safe_load

from .builder import tidy_sql
from .data_provider import fetch_schema, remove_none_values
from .diff import diff
from .identifiers import FQN, URN
from .resource_props import schema_props


def create_schema_sql(fqn, data, or_replace=False, if_not_exists=False):
    return tidy_sql(
        "CREATE",
        "OR REPLACE" if or_replace else "",
        "SCHEMA",
        "IF NOT EXISTS" if if_not_exists else "",
        fqn,
        schema_props.render(data),
    )


def update_schema_sql(fqn, change):
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


def create_or_update_schema(sp_session, config: dict = None, yaml: str = None):
    if yaml and config is None:
        config = safe_load(yaml)
    sf_session = sp_session.connection
    db = config.get("database", sp_session.get_current_database())
    fqn = FQN(database=db, name=config["name"])
    urn = str(URN(resource_key="schema", fqn=fqn))
    res = {urn: remove_none_values(fetch_schema(sf_session, fqn))}
    data = {urn: config}
    sql = []
    if res:
        # return {
        #     "action": action,
        #     "res": res,
        #     "config": config,
        #     "diff": [[str(action), urn_str, change] for action, urn_str, change in diff(res, data)],
        # }
        for action, urn_str, change in diff(res, data):
            sql.append(update_schema_sql(fqn, change))
        action = "update"
    else:
        action = "create"
        # sp_session.sql(create_schema_sql(fqn, data, if_not_exists=True)).collect()
        sql = [create_schema_sql(fqn, config, if_not_exists=True)]
    return {"action": action, "res": res, "sql": sql}
