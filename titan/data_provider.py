import json
import sys

from collections import defaultdict
from functools import cache

from inflection import pluralize

from snowflake.connector.errors import ProgrammingError

from .client import execute, DOEST_NOT_EXIST_ERR
from .identifiers import URN, FQN


__this__ = sys.modules[__name__]


def _fail_if_not_granted(result, *args):
    if len(result) == 0:
        raise Exception("Failed to create grant")
    if len(result) == 1 and result[0]["status"] == "Grant not executed: Insufficient privileges.":
        raise Exception(result[0]["status"], *args)


def _filter_result(result, **kwargs):
    filtered = []
    for row in result:
        for key, value in kwargs.items():
            if row[key] != value:
                break
        else:
            filtered.append(row)
    return filtered


def _urn_from_grant(row, session_ctx):
    granted_on = row["granted_on"].lower()
    if granted_on == "account":
        urn = URN.from_session_ctx(session_ctx)
    else:
        fqn = FQN(name=row["name"])
        urn = URN(resource_type=granted_on, account_locator=session_ctx["account_locator"], fqn=fqn)
    return urn


def params_result_to_dict(params_result):
    params = {}
    for param in params_result:
        if param["type"] == "BOOLEAN":
            typed_value = param["value"] == "true"
        elif param["type"] == "NUMBER":
            typed_value = int(param["value"])
        elif param["type"] == "STRING":
            typed_value = str(param["value"]) if param["value"] else None
        params[param["key"].lower()] = typed_value
    return params


def options_result_to_list(options_result):
    return [option.strip(" ") for option in options_result.split(",")]


def remove_none_values(d):
    return {k: v for k, v in d.items() if v is not None}


def fetch_remote_state(session, manifest):
    state = {}
    for urn_str in manifest["_urns"]:
        urn = URN.from_str(urn_str)
        data = fetch_resource(session, urn)
        if urn_str in manifest and data is not None:
            if isinstance(data, list):
                compacted = [remove_none_values(d) for d in data]
            else:
                compacted = remove_none_values(data)
            state[urn_str] = compacted

    return state


def fetch_resource(session, urn):
    return getattr(__this__, f"fetch_{urn.resource_type}")(session, urn.fqn)


def fetch_account_locator(session):
    locator = execute(session, "SELECT CURRENT_ACCOUNT()")[0]
    return locator


def fetch_region(session):
    region = execute(session, "SELECT CURRENT_REGION()")[0]
    return region


@cache
def fetch_session(session):
    session_obj = execute(
        session,
        """
        SELECT
            CURRENT_ACCOUNT_NAME() as account,
            CURRENT_ACCOUNT() as account_locator,
            CURRENT_USER() as user,
            CURRENT_ROLE() as role,
            CURRENT_AVAILABLE_ROLES() as available_roles,
            CURRENT_SECONDARY_ROLES() as secondary_roles,
            CURRENT_DATABASE() as database,
            CURRENT_SCHEMAS() as schemas,
            CURRENT_WAREHOUSE() as warehouse
        """,
    )[0]
    return {
        "account": session_obj["ACCOUNT"],
        "account_locator": session_obj["ACCOUNT_LOCATOR"],
        "user": session_obj["USER"],
        "role": session_obj["ROLE"],
        "available_roles": json.loads(session_obj["AVAILABLE_ROLES"]),
        "secondary_roles": json.loads(session_obj["SECONDARY_ROLES"]),
        "database": session_obj["DATABASE"],
        "schemas": json.loads(session_obj["SCHEMAS"]),
        "warehouse": session_obj["WAREHOUSE"],
    }


def fetch_account(session, fqn: FQN):
    # raise NotImplementedError()
    return {}


def fetch_alert(session, fqn: FQN):
    show_result = execute(session, "SHOW ALERTS", cacheable=True)
    alerts = _filter_result(show_result, name=fqn.name)
    if len(alerts) == 0:
        return None
    if len(alerts) > 1:
        raise Exception(f"Found multiple alerts matching {fqn}")
    data = alerts[0]
    return {
        "name": data["name"],
        "warehouse": data["warehouse"],
        "schedule": data["schedule"],
        "comment": data["comment"] or None,
        "condition": data["condition"],
        "then": data["action"],
        "owner": data["owner"],
    }


def fetch_columns(session, resource_type: str, fqn: FQN):
    desc_result = execute(session, f"DESC {resource_type} {fqn}")
    columns = []
    for col in desc_result:
        if col["kind"] != "COLUMN":
            raise Exception(f"Unexpected kind {col['kind']} in desc result")
        columns.append(
            remove_none_values(
                {
                    "name": col["name"],
                    "data_type": col["type"],
                    "nullable": col["null?"] == "Y",
                    "default": col["default"],
                    "comment": col["comment"] or None,
                }
            )
        )
    return columns


def fetch_database(session, fqn: FQN):
    show_result = execute(session, f"SHOW DATABASES LIKE '{fqn.name}'", cacheable=True)

    if len(show_result) == 0:
        return None
    if len(show_result) > 1:
        raise Exception(f"Found multiple databases matching {fqn}")
    if show_result[0]["kind"] != "STANDARD":
        return None

    options = options_result_to_list(show_result[0]["options"])
    show_params_result = execute(session, f"SHOW PARAMETERS IN DATABASE {fqn.name}")
    params = params_result_to_dict(show_params_result)

    return {
        "name": show_result[0]["name"],
        "data_retention_time_in_days": int(show_result[0]["retention_time"]),
        "comment": show_result[0]["comment"] or None,
        "transient": "TRANSIENT" in options,
        "owner": show_result[0]["owner"],
        "max_data_extension_time_in_days": params["max_data_extension_time_in_days"],
        "default_ddl_collation": params["default_ddl_collation"],
    }


def fetch_javascript_udf(session, fqn: FQN):
    show_result = execute(session, "SHOW USER FUNCTIONS IN ACCOUNT", cacheable=True)
    udfs = _filter_result(show_result, name=fqn.name)
    if len(udfs) == 0:
        return None
    if len(udfs) > 1:
        raise Exception(f"Found multiple roles matching {fqn}")

    data = udfs[0]
    inputs, output = data["arguments"].split(" RETURN ")
    desc_result = execute(session, f"DESC FUNCTION {inputs}", cacheable=True)
    properties = dict([(row["property"], row["value"]) for row in desc_result])

    return {
        "name": data["name"],
        "secure": data["is_secure"] == "Y",
        # "args": data["arguments"],
        "returns": output,
        "language": data["language"],
        "comment": None if data["description"] == "user-defined function" else data["description"],
        "volatility": properties["volatility"],
        "as_": properties["body"],
    }


def fetch_grant(session, fqn: FQN):
    show_result = execute(session, f"SHOW GRANTS TO ROLE {fqn.name}")
    on = fqn.params["on"]
    to = fqn.name
    grants = _filter_result(show_result, granted_on=on, grantee_name=to)

    if len(grants) == 0:
        return []

    return sorted(
        [
            {
                "priv": row["privilege"],
                "on": row["granted_on"],
                "to": row["grantee_name"],
                "grant_option": row["grant_option"] == "true",
                "owner": row["granted_by"],
            }
            for row in grants
        ],
        key=lambda g: (g["priv"], g["owner"]),
    )


def fetch_role_grants(session, role: str):
    show_result = execute(session, f"SHOW GRANTS TO ROLE {role}")
    session_ctx = fetch_session(session)

    priv_map = defaultdict(list)

    for row in show_result:
        urn = _urn_from_grant(row, session_ctx)
        priv_map[str(urn)].append(
            {
                "priv": row["privilege"],
                "grant_option": row["grant_option"] == "true",
                "owner": row["granted_by"],
            }
        )

    return dict(priv_map)


def fetch_procedure(session, fqn: FQN):
    # SHOW PROCEDURES IN SCHEMA {}.{}
    show_result = execute(session, "SHOW PROCEDURES IN SCHEMA", cacheable=True)
    sprocs = _filter_result(show_result, name=fqn.name)
    if len(sprocs) == 0:
        return None
    if len(sprocs) > 1:
        raise Exception(f"Found multiple stored procedures matching {fqn}")

    data = sprocs[0]
    inputs, output = data["arguments"].split(" RETURN ")
    desc_result = execute(session, f"DESC FUNCTION {inputs}", cacheable=True)
    properties = dict([(row["property"], row["value"]) for row in desc_result])

    return {
        "name": data["name"],
        "secure": data["is_secure"] == "Y",
        # "args": data["arguments"],
        "returns": output,
        "language": properties["language"],
        "runtime_version": properties["runtime_version"],
        "null_handling": properties["null_handling"],
        "packages": properties["packages"],
        "comment": None if data["description"] == "user-defined function" else data["description"],
        "handler": properties["handler"],
        "execute_as": properties["execute as"],
        "as_": properties["body"],
    }


def fetch_role(session, fqn: FQN):
    show_result = execute(session, f"SHOW ROLES LIKE '{fqn.name}'", cacheable=True)

    if len(show_result) == 0:
        return None
    if len(show_result) > 1:
        raise Exception(f"Found multiple roles matching {fqn}")

    data = show_result[0]

    return {
        "name": data["name"],
        "comment": data["comment"] or None,
        "owner": data["owner"],
    }


def fetch_role_grant(session, fqn: FQN):
    subject, name = fqn.params.copy().popitem()
    try:
        show_result = execute(session, f"SHOW GRANTS OF ROLE {fqn.name}", cacheable=True)
    except ProgrammingError as err:
        if err.errno == DOEST_NOT_EXIST_ERR:
            return None
        raise

    if len(show_result) == 0:
        return None

    for data in show_result:
        if data["granted_to"] == subject.upper() and data["grantee_name"] == name:
            if data["granted_to"] == "ROLE":
                return {"role": fqn.name, "to_role": data["grantee_name"], "owner": data["granted_by"]}
            elif data["granted_to"] == "USER":
                return {"role": fqn.name, "to_user": data["grantee_name"], "owner": data["granted_by"]}
            else:
                raise Exception(f"Unexpected role grant for role {fqn.name}")

    return None


def fetch_schema(session, fqn: FQN):
    if fqn.database is None:
        raise Exception(f"Schema fqn must have a database {fqn}")
    try:
        show_result = execute(session, f"SHOW SCHEMAS LIKE '{fqn.name}' IN DATABASE {fqn.database}")
    except ProgrammingError:
        return None

    if len(show_result) == 0:
        return None
    if len(show_result) > 1:
        raise Exception(f"Found multiple schemas matching {fqn}")

    data = show_result[0]

    options = options_result_to_list(data["options"])
    show_params_result = execute(session, f"SHOW PARAMETERS IN SCHEMA {fqn}")
    params = params_result_to_dict(show_params_result)

    return {
        "name": data["name"],
        "transient": "TRANSIENT" in options,
        "owner": data["owner"],
        "managed_access": "MANAGED ACCESS" in options,
        "data_retention_time_in_days": int(data["retention_time"]),
        "max_data_extension_time_in_days": params["max_data_extension_time_in_days"],
        "default_ddl_collation": params["default_ddl_collation"],
        "comment": data["comment"] or None,
    }


def fetch_shared_database(session, fqn: FQN):
    show_result = execute(session, "SELECT SYSTEM$SHOW_IMPORTED_DATABASES()", cacheable=True)
    show_result = json.loads(show_result[0]["SYSTEM$SHOW_IMPORTED_DATABASES()"])

    shares = _filter_result(show_result, name=fqn.name)

    if len(shares) == 0:
        return None
    if len(shares) > 1:
        raise Exception(f"Found multiple shares matching {fqn}")

    data = shares[0]
    return {
        "name": data["name"],
        "from_share": data["origin"],
        "owner": data["owner"],
    }


def fetch_table(session, fqn: FQN):
    show_result = execute(session, "SHOW TABLES")

    tables = _filter_result(show_result, name=fqn.name, kind="TABLE")

    if len(tables) == 0:
        return None
    if len(tables) > 1:
        raise Exception(f"Found multiple tables matching {fqn}")

    columns = fetch_columns(session, "TABLE", fqn)

    data = tables[0]
    return {
        "name": data["name"],
        "owner": data["owner"],
        "comment": data["comment"] or None,
        "cluster_by": data["cluster_by"] or None,
        "columns": columns,
    }


def fetch_user(session, fqn: FQN):
    # SHOW USERS requires the MANAGE GRANTS privilege
    show_result = execute(session, "SHOW USERS", cacheable=True, use_role="SECURITYADMIN")

    users = _filter_result(show_result, name=fqn.name)

    if len(users) == 0:
        return None
    if len(users) > 1:
        raise Exception(f"Found multiple users matching {fqn}")

    data = users[0]

    return {
        "name": data["name"],
        "login_name": data["login_name"],
        "display_name": data["display_name"],
        "first_name": data["first_name"] or None,
        "last_name": data["last_name"] or None,
        "email": data["email"] or None,
        "mins_to_unlock": data["mins_to_unlock"] or None,
        "days_to_expiry": data["days_to_expiry"] or None,
        "comment": data["comment"] or None,
        "disabled": data["disabled"] == "true",
        "must_change_password": data["must_change_password"] == "true",
        "default_warehouse": data["default_warehouse"] or None,
        "default_namespace": data["default_namespace"] or None,
        "default_role": data["default_role"] or None,
        "default_secondary_roles": data["default_secondary_roles"] or None,
        "mins_to_bypass_mfa": data["mins_to_bypass_mfa"] or None,
        "owner": data["owner"],
    }


def fetch_view(session, fqn: FQN):
    if fqn.schema is None:
        raise Exception(f"View fqn must have a schema {fqn}")
    try:
        show_result = execute(session, f"SHOW VIEWS LIKE '{fqn.name}' IN SCHEMA {fqn.database}.{fqn.schema}")
    except ProgrammingError:
        return None

    if len(show_result) == 0:
        return None
    if len(show_result) > 1:
        raise Exception(f"Found multiple views matching {fqn}")

    data = show_result[0]

    if data["is_materialized"] == "true":
        return None

    columns = fetch_columns(session, "VIEW", fqn)

    return {
        "name": data["name"],
        "owner": data["owner"],
        "secure": data["is_secure"] == "true",
        "columns": columns,
        "change_tracking": data["change_tracking"] == "ON",
        "comment": data["comment"] or None,
        "as_": data["text"],
    }


def fetch_warehouse(session, fqn: FQN):
    try:
        show_result = execute(session, f"SHOW WAREHOUSES LIKE '{fqn.name}'")
    except ProgrammingError:
        return None

    if len(show_result) == 0:
        return None
    if len(show_result) > 1:
        raise Exception(f"Found multiple warehouses matching {fqn}")

    data = show_result[0]

    show_params_result = execute(session, f"SHOW PARAMETERS FOR WAREHOUSE {fqn}")
    params = params_result_to_dict(show_params_result)

    query_accel = data.get("enable_query_acceleration")
    if query_accel:
        query_accel = query_accel == "true"

    return {
        "name": data["name"],
        "owner": data["owner"],
        "warehouse_type": data["type"],
        "warehouse_size": data["size"].upper(),
        # "max_cluster_count": data["max_cluster_count"],
        # "min_cluster_count": data["min_cluster_count"],
        # "scaling_policy": data["scaling_policy"],
        "auto_suspend": data["auto_suspend"],
        "auto_resume": data["auto_resume"] == "true",
        "comment": data["comment"] or None,
        "enable_query_acceleration": query_accel,
        "max_concurrency_level": params["max_concurrency_level"],
        "statement_queued_timeout_in_seconds": params["statement_queued_timeout_in_seconds"],
        "statement_timeout_in_seconds": params["statement_timeout_in_seconds"],
    }


################ List functions


def list_resource(session, resource_key):
    return getattr(__this__, f"list_{pluralize(resource_key)}")(session)


def list_databases(session):
    show_result = execute(session, "SHOW DATABASES")
    return [row["name"] for row in show_result]


def list_schemas(session):
    show_result = execute(session, "SHOW SCHEMAS")
    return [f"{row['database_name']}.{row['name']}" for row in show_result]
