import json
import sys

from collections import defaultdict
from functools import cache

from inflection import pluralize

from snowflake.connector.errors import ProgrammingError

from .client import execute, OBJECT_DOES_NOT_EXIST_ERR, DOEST_NOT_EXIST_ERR, UNSUPPORTED_FEATURE
from .enums import ResourceType, WarehouseSize
from .identifiers import URN, FQN, resource_type_for_label
from .parse import (
    FullyQualifiedIdentifier,
    _parse_dynamic_table_text,
    _parse_column,
    parse_identifier,
    parse_function_name,
)
from .resource_name import ResourceName


__this__ = sys.modules[__name__]


def _desc_result_to_dict(desc_result):
    return dict([(row["property"], row["value"]) for row in desc_result])


def _desc_type2_result_to_dict(desc_result):
    return dict([(row["property"], row["property_value"]) for row in desc_result])


def _fail_if_not_granted(result, *args):
    if len(result) == 0:
        raise Exception("Failed to create grant")
    if len(result) == 1 and result[0]["status"] == "Grant not executed: Insufficient privileges.":
        raise Exception(result[0]["status"], *args)


def _filter_result(result, **kwargs):
    filtered = []
    for row in result:
        for key, value in kwargs.items():
            # Roughly match any names. `name`, `database_name`, `schema_name`, etc.
            if key.endswith("name"):
                if ResourceName(row[key]) != ResourceName(value):
                    break
            else:
                if row[key] != value:
                    break
        else:
            filtered.append(row)
    return filtered


def _urn_from_grant(row, session_ctx):
    account_scoped_resources = {"user", "role", "warehouse", "database", "task"}
    granted_on = row["granted_on"].lower()
    if granted_on == "account":
        return URN.from_session_ctx(session_ctx)
    else:
        if granted_on == "procedure" or granted_on == "function":
            # This needs a special function because Snowflake gives an incorrect FQN for functions/sprocs
            # eg. TITAN_DEV.PUBLIC."FETCH_DATABASE(NAME VARCHAR):OBJECT"
            # The correct FQN is TITAN_DEV.PUBLIC."FETCH_DATABASE"(VARCHAR)
            id_parts = list(FullyQualifiedIdentifier.parse_string(row["name"], parse_all=True))
            name = parse_function_name(id_parts[-1])
            fqn = FQN(database=id_parts[0], schema=id_parts[1], name=name)
        elif granted_on in account_scoped_resources:
            # This is probably all account-scoped resources
            fqn = FQN(name=ResourceName(row["name"]))
        else:
            # Scoped resources
            fqn = parse_identifier(row["name"], is_db_scoped=(granted_on == "schema"))
        return URN(
            resource_type=ResourceType(granted_on),
            account_locator=session_ctx["account_locator"],
            fqn=fqn,
        )


def _parse_function_arguments_2023_compat(arguments_str: str) -> tuple:
    """
    Input
    -----
        FETCH_DATABASE(OBJECT [, BOOLEAN]) RETURN OBJECT

    Output
    ------
        identifier => FETCH_DATABASE(OBJECT, BOOLEAN)
        returns => OBJECT

    """

    header, returns = arguments_str.split(" RETURN ")
    header = header.replace("[", "").replace("]", "")
    identifier = parse_identifier(header)
    return (identifier, returns)


def _parse_function_arguments(arguments_str: str) -> tuple:
    """
    Input
    -----
        FETCH_DATABASE(VARCHAR) RETURN OBJECT

    Output
    ------
        identifier => FETCH_DATABASE(VARCHAR)
        returns => OBJECT

    """

    header, returns = arguments_str.split(" RETURN ")
    identifier = parse_identifier(header)
    return (identifier, returns)


def _parse_imports(imports: str) -> list:
    if imports is None:
        return []
    imports = imports.strip("[]")
    if imports:
        return [imp.strip(" ") for imp in imports.split(",")]
    return []


def _parse_signature(signature: str) -> list:
    signature = signature.strip("()")

    if signature:
        return [_parse_column(col.strip(" ")) for col in signature.split(",")]
    return []


def _parse_comma_separated_values(values: str) -> list:
    if values is None or values == "":
        return None
    return [value.strip(" ") for value in values.split(",")]


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


def fetch_resource(session, urn: URN):
    return getattr(__this__, f"fetch_{urn.resource_label}")(session, urn.fqn)


def fetch_account_locator(session):
    locator = execute(session, "SELECT CURRENT_ACCOUNT() as account_locator")[0]["ACCOUNT_LOCATOR"]
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
            CURRENT_WAREHOUSE() as warehouse,
            CURRENT_VERSION() as version,
            SYSTEM$BEHAVIOR_CHANGE_BUNDLE_STATUS('2024_01') as release_bundle_2024_01
        """,
    )[0]

    try:
        show_tags = execute(session, "SHOW TAGS IN ACCOUNT")
        tags = [f"{row['database_name']}.{row['schema_name']}.{row['name']}" for row in show_tags]
        tag_support = True
    except ProgrammingError as err:
        if err.errno == UNSUPPORTED_FEATURE:
            tags = []
            tag_support = False
        else:
            raise

    return {
        "account_locator": session_obj["ACCOUNT_LOCATOR"],
        "account": session_obj["ACCOUNT"],
        "available_roles": json.loads(session_obj["AVAILABLE_ROLES"]),
        "database": session_obj["DATABASE"],
        "release_bundle_2024_01": session_obj["RELEASE_BUNDLE_2024_01"],
        "role": session_obj["ROLE"],
        "schemas": json.loads(session_obj["SCHEMAS"]),
        "secondary_roles": json.loads(session_obj["SECONDARY_ROLES"]),
        "tag_support": tag_support,
        "tags": tags,
        "user": session_obj["USER"],
        "version": session_obj["VERSION"],
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

    data = show_result[0]

    is_standard_db = data["kind"] == "STANDARD"
    is_snowflake_builtin = data["kind"] == "APPLICATION" and data["name"] == "SNOWFLAKE"

    if not (is_standard_db or is_snowflake_builtin):
        return None

    options = options_result_to_list(data["options"])
    show_params_result = execute(session, f"SHOW PARAMETERS IN DATABASE {fqn.name}")
    params = params_result_to_dict(show_params_result)

    return {
        "name": data["name"],
        "data_retention_time_in_days": int(data["retention_time"]),
        "comment": data["comment"] or None,
        "transient": "TRANSIENT" in options,
        "owner": data["owner"],
        "max_data_extension_time_in_days": params.get("max_data_extension_time_in_days"),
        "default_ddl_collation": params["default_ddl_collation"],
    }


def fetch_dynamic_table(session, fqn: FQN):
    show_result = execute(session, f"SHOW DYNAMIC TABLES LIKE '{fqn.name}'")

    if len(show_result) == 0:
        return None
    if len(show_result) > 1:
        raise Exception(f"Found multiple dynamic tables matching {fqn}")

    columns = fetch_columns(session, "DYNAMIC TABLE", fqn)

    data = show_result[0]
    refresh_mode, initialize, as_ = _parse_dynamic_table_text(data["text"])
    return {
        "name": data["name"],
        "owner": data["owner"],
        "warehouse": data["warehouse"],
        "refresh_mode": refresh_mode,
        "initialize": initialize,
        "target_lag": data["target_lag"],
        "comment": data["comment"] or None,
        "columns": columns,
        "as_": as_,
    }


def fetch_function(session, fqn: FQN):
    show_result = execute(session, "SHOW USER FUNCTIONS IN ACCOUNT", cacheable=True)
    udfs = _filter_result(show_result, name=fqn.name)
    if len(udfs) == 0:
        return None
    if len(udfs) > 1:
        raise Exception(f"Found multiple roles matching {fqn}")

    data = udfs[0]
    inputs, output = data["arguments"].split(" RETURN ")
    desc_result = execute(session, f"DESC FUNCTION {inputs}", cacheable=True)
    properties = _desc_result_to_dict(desc_result)

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


def fetch_future_grant(session, fqn: FQN):
    try:
        show_result = execute(session, f"SHOW FUTURE GRANTS TO ROLE {fqn.name}", cacheable=True)
        """
        {
            'created_on': datetime.datetime(2024, 2, 5, 19, 39, 50, 146000, tzinfo=<DstTzInfo 'America/Los_Angeles' PST-1 day, 16:00:00 STD>),
            'privilege': 'USAGE',
            'grant_on': 'SCHEMA',
            'name': 'STATIC_DATABASE.<SCHEMA>',
            'grant_to': 'ROLE',
            'grantee_name': 'THATROLE',
            'grant_option': 'false'
        }
        """

    except ProgrammingError as err:
        if err.errno == DOEST_NOT_EXIST_ERR:
            return None
        raise

    in_type, name = fqn.params["on"].split("/")
    in_name = name.split(".")[0]

    grants = _filter_result(
        show_result,
        privilege=fqn.params["priv"],
        name=name,
        grant_to="ROLE",
        grantee_name=fqn.name,
    )

    if len(grants) == 0:
        return None
    elif len(grants) > 1:
        raise Exception(f"Found multiple future grants matching {fqn}")

    data = grants[0]

    return {
        "priv": data["privilege"],
        "on_type": data["grant_on"],
        "in_type": in_type,
        "in_name": in_name,
        "to": data["grantee_name"],
        "grant_option": data["grant_option"] == "true",
    }


def fetch_grant(session, fqn: FQN):
    try:
        show_result = execute(session, f"SHOW GRANTS TO ROLE {fqn.name}", cacheable=True)
        """
        {
            'created_on': datetime.datetime(2024, 2, 28, 20, 5, 32, 166000, tzinfo=<DstTzInfo 'America/Los_Angeles' PST-1 day, 16:00:00 STD>),
            'privilege': 'USAGE',
            'granted_on': 'DATABASE',
            'name': 'STATIC_DATABASE',
            'granted_to': 'ROLE',
            'grantee_name': 'THATROLE',
            'grant_option': 'false',
            'granted_by': 'ACCOUNTADMIN'
        }
        """
    except ProgrammingError as err:
        if err.errno == DOEST_NOT_EXIST_ERR:
            return None
        raise
    on_type, on = fqn.params["on"].split("/")
    on_type = str(resource_type_for_label(on_type))
    grants = _filter_result(
        show_result,
        granted_on=on_type,
        name=on,
        grantee_name=fqn.name,
    )

    if len(grants) == 0:
        return None
    elif len(grants) > 1:
        # This is likely to happen when a grant has been issued by ACCOUNTADMIN
        # and some other role with MANAGE GRANTS or OWNERSHIP. It needs to be properly
        # handled in the future.
        raise Exception(f"Found multiple grants matching {fqn}")

    data = grants[0]

    return {
        "priv": data["privilege"],
        "on": data["name"],
        "on_type": data["granted_on"],
        "to": data["grantee_name"],
        "grant_option": data["grant_option"] == "true",
        "owner": data["granted_by"],
    }


def fetch_grant_on_all(session, fqn: FQN):
    # All grants are expensive to fetch, so we will assume they are always out of date
    return None


def fetch_password_policy(session, fqn: FQN):
    show_result = execute(session, f"SHOW PASSWORD POLICIES IN SCHEMA {fqn.database}.{fqn.schema}")
    policies = _filter_result(show_result, name=fqn.name)
    if len(policies) == 0:
        return None
    if len(policies) > 1:
        raise Exception(f"Found multiple password policies matching {fqn}")

    data = policies[0]
    desc_result = execute(session, f"DESC PASSWORD POLICY {fqn}")
    properties = _desc_result_to_dict(desc_result)

    return {
        "name": data["name"],
        "password_min_length": int(properties["PASSWORD_MIN_LENGTH"]),
        "password_max_length": int(properties["PASSWORD_MAX_LENGTH"]),
        "password_min_upper_case_chars": int(properties["PASSWORD_MIN_UPPER_CASE_CHARS"]),
        "password_min_lower_case_chars": int(properties["PASSWORD_MIN_LOWER_CASE_CHARS"]),
        "password_min_numeric_chars": int(properties["PASSWORD_MIN_NUMERIC_CHARS"]),
        "password_min_special_chars": int(properties["PASSWORD_MIN_SPECIAL_CHARS"]),
        "password_min_age_days": int(properties["PASSWORD_MIN_AGE_DAYS"]),
        "password_max_age_days": int(properties["PASSWORD_MAX_AGE_DAYS"]),
        "password_max_retries": int(properties["PASSWORD_MAX_RETRIES"]),
        "password_lockout_time_mins": int(properties["PASSWORD_LOCKOUT_TIME_MINS"]),
        "password_history": int(properties["PASSWORD_HISTORY"]),
        "comment": properties["COMMENT"] or None,
        "owner": properties["OWNER"],
    }


def fetch_procedure(session, fqn: FQN):
    # SHOW PROCEDURES IN SCHEMA {}.{}
    show_result = execute(session, f"SHOW PROCEDURES IN SCHEMA {fqn.database}.{fqn.schema}", cacheable=True)
    sprocs = _filter_result(show_result, name=fqn.name)
    if len(sprocs) == 0:
        return None
    if len(sprocs) > 1:
        raise Exception(f"Found multiple stored procedures matching {fqn}")

    data = sprocs[0]
    # inputs, output = data["arguments"].split(" RETURN ")
    session_ctx = fetch_session(session)

    if session_ctx["release_bundle_2024_01"] == "ENABLED":
        identifier, returns = _parse_function_arguments(data["arguments"])
    else:
        identifier, returns = _parse_function_arguments_2023_compat(data["arguments"])
    desc_result = execute(session, f"DESC PROCEDURE {fqn.database}.{fqn.schema}.{str(identifier)}", cacheable=True)
    properties = _desc_result_to_dict(desc_result)

    show_grants = execute(session, f"SHOW GRANTS ON PROCEDURE {fqn.database}.{fqn.schema}.{str(identifier)}")
    ownership_grant = _filter_result(show_grants, privilege="OWNERSHIP")

    return {
        "name": data["name"].lower(),
        "args": _parse_signature(properties["signature"]),
        "comment": data["description"],
        "execute_as": properties["execute as"],
        "handler": properties["handler"],
        "imports": _parse_imports(properties["imports"]),
        "language": properties["language"],
        "null_handling": properties["null handling"],
        "owner": ownership_grant[0]["grantee_name"] if len(ownership_grant) > 0 else None,
        "packages": json.loads(properties["packages"].replace("'", '"')),
        "returns": returns,
        "runtime_version": properties["runtime_version"],
        "secure": data["is_secure"] == "Y",
        "as_": properties["body"],
    }


def fetch_role_grants(session, role: str):
    if role in ["ACCOUNTADMIN", "ORGADMIN", "SECURITYADMIN"]:
        return {}
    try:
        show_result = execute(session, f"SHOW GRANTS TO ROLE {role}", cacheable=True)
    except ProgrammingError as err:
        if err.errno == DOEST_NOT_EXIST_ERR:
            return None
        raise
    session_ctx = fetch_session(session)

    priv_map = defaultdict(list)

    for row in show_result:
        try:
            urn = _urn_from_grant(row, session_ctx)
        except ValueError:
            # Grant for a Snowflake resource type that Titan doesn't support yet
            continue
        priv_map[str(urn)].append(
            {
                "priv": row["privilege"],
                "grant_option": row["grant_option"] == "true",
                "owner": row["granted_by"],
            }
        )

    return dict(priv_map)


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
                return {
                    "role": fqn.name,
                    "to_role": data["grantee_name"],
                    # "owner": data["granted_by"],
                }
            elif data["granted_to"] == "USER":
                return {
                    "role": fqn.name,
                    "to_user": data["grantee_name"],
                    # "owner": data["granted_by"],
                }
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
    tags = fetch_resource_tags(session, ResourceType.SCHEMA, fqn)

    return {
        "name": data["name"],
        "transient": "TRANSIENT" in options,
        "owner": data["owner"],
        "managed_access": "MANAGED ACCESS" in options,
        "data_retention_time_in_days": int(data["retention_time"]),
        "max_data_extension_time_in_days": params.get("max_data_extension_time_in_days"),
        "default_ddl_collation": params["default_ddl_collation"],
        "comment": data["comment"] or None,
        "tags": tags,
    }


def fetch_sequence(session, fqn: FQN):
    show_result = execute(session, f"SHOW SEQUENCES LIKE '{fqn.name}' IN SCHEMA {fqn.database}.{fqn.schema}")
    if len(show_result) == 0:
        return None
    if len(show_result) > 1:
        raise Exception(f"Found multiple sequences matching {fqn}")

    data = show_result[0]

    return {
        "name": data["name"],
        "owner": data["owner"],
        "start": data["next_value"],
        "increment": data["interval"],
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


def fetch_stage(session, fqn: FQN):
    raise NotImplementedError


def fetch_storage_integration(session, fqn: FQN):
    show_result = execute(session, "SHOW INTEGRATIONS")
    integrations = _filter_result(show_result, name=fqn.name, category="STORAGE")

    if len(integrations) == 0:
        return None
    if len(integrations) > 1:
        raise Exception(f"Found multiple storage integrations matching {fqn}")

    data = integrations[0]

    desc_result = execute(session, f"DESC INTEGRATION {fqn.name}")
    properties = _desc_type2_result_to_dict(desc_result)

    show_grants = execute(session, f"SHOW GRANTS ON INTEGRATION {fqn.name}")
    ownership_grant = _filter_result(show_grants, privilege="OWNERSHIP")

    return {
        "name": data["name"],
        "type": data["type"],
        "enabled": data["enabled"] == "true",
        "comment": data["comment"] or None,
        "storage_provider": properties["STORAGE_PROVIDER"],
        "storage_aws_role_arn": properties.get("STORAGE_AWS_ROLE_ARN"),
        "storage_allowed_locations": _parse_comma_separated_values(properties.get("STORAGE_ALLOWED_LOCATIONS")),
        "storage_blocked_locations": _parse_comma_separated_values(properties.get("STORAGE_BLOCKED_LOCATIONS")),
        "storage_aws_object_acl": properties.get("STORAGE_AWS_OBJECT_ACL"),
        "owner": ownership_grant[0]["grantee_name"] if len(ownership_grant) > 0 else None,
    }


def fetch_stream(session, fqn: FQN):
    show_result = execute(session, "SHOW STREAMS IN ACCOUNT")

    streams = _filter_result(show_result, name=fqn.name, database_name=fqn.database, schema_name=fqn.schema)

    if len(streams) == 0:
        return None
    if len(streams) > 1:
        raise Exception(f"Found multiple streams matching {fqn}")

    data = streams[0]
    return {
        "name": data["name"],
        "comment": data["comment"] or None,
        "append_only": data["mode"] == "APPEND_ONLY",
        "on_table": data["table_name"] if data["source_type"] == "Table" else None,
    }


def fetch_event_table(session, fqn: FQN):
    show_result = execute(session, "SHOW EVENT TABLES IN ACCOUNT")

    tables = _filter_result(show_result, name=fqn.name, database_name=fqn.database, schema_name=fqn.schema)

    if len(tables) == 0:
        return None
    if len(tables) > 1:
        raise Exception(f"Found multiple tables matching {fqn}")

    data = tables[0]
    return {"name": data["name"], "comment": data["comment"] or None, "cluster_by": data["cluster_by"] or None}


def fetch_task(session, fqn: FQN):
    tasks = execute(session, f"SHOW TASKS LIKE '{fqn.name}' in schema {fqn.database}.{fqn.schema}", cacheable=True)
    if len(tasks) == 0:
        return None
    if len(tasks) > 1:
        raise Exception(f"Found multiple tasks matching {fqn}")

    data = tasks[0]
    task_details_result = execute(session, f"DESC TASK {fqn.database}.{fqn.schema}.{fqn.name}", cacheable=True)
    if len(task_details_result) == 0:
        raise Exception(f"Failed to fetch task details for {fqn}")
    task_details = task_details_result[0]
    return {
        "name": data["name"],
        "warehouse": data["warehouse"],
        "schedule": data["schedule"],
        "state": str(data["state"]).upper(),
        "owner": data["owner"],
        "as_": task_details["definition"],
    }


def fetch_replication_group(session, fqn: FQN):
    show_result = execute(session, f"SHOW REPLICATION GROUPS LIKE '{fqn.name}'", cacheable=True)

    replication_groups = _filter_result(show_result, is_primary="true")
    if len(replication_groups) == 0:
        return None
    if len(replication_groups) > 1:
        raise Exception(f"Found multiple replication groups matching {fqn}")

    data = replication_groups[0]
    show_databases_result = execute(session, f"SHOW DATABASES IN REPLICATION GROUP {fqn.name}")
    databases = [row["name"] for row in show_databases_result]
    return {
        "name": data["name"],
        "object_types": data["object_types"].split(","),
        "allowed_integration_types": (
            None if data["allowed_integration_types"] == "" else data["allowed_integration_types"].split(",")
        ),
        "allowed_accounts": None if data["allowed_accounts"] == "" else data["allowed_accounts"].split(","),
        "allowed_databases": databases,
        "replication_schedule": data["replication_schedule"],
        "owner": data["owner"],
    }


def fetch_table(session, fqn: FQN):
    show_result = execute(session, "SHOW TABLES IN ACCOUNT")

    tables = _filter_result(
        show_result, name=fqn.name, database_name=fqn.database, schema_name=fqn.schema, kind="TABLE"
    )

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
        # This is here because of CTAS type tables.
        # we have no way of getting this back from Snowflake, unless we crammed it as a tag/comment somewhere
        "as_": None,
    }


def fetch_resource_tags(session, resource_type: ResourceType, fqn: FQN):
    session_ctx = fetch_session(session)
    if session_ctx["tag_support"] is False:
        return None

    """
    +----------------------+------------+-------------+-----------+--------+----------------------+---------------+-------------+--------+-------------+
    |     TAG_DATABASE     | TAG_SCHEMA |  TAG_NAME   | TAG_VALUE | LEVEL  |   OBJECT_DATABASE    | OBJECT_SCHEMA | OBJECT_NAME | DOMAIN | COLUMN_NAME |
    +----------------------+------------+-------------+-----------+--------+----------------------+---------------+-------------+--------+-------------+
    | TITAN                | SOMESCH    | TASTY_TREAT | muffin    | SCHEMA | TEST_DB_RUN_13287C56 |               | SOMESCH     | SCHEMA |             |
    | TEST_DB_RUN_13287C56 | PUBLIC     | TRASH       | true      | SCHEMA | TEST_DB_RUN_13287C56 |               | SOMESCH     | SCHEMA |             |
    +----------------------+------------+-------------+-----------+--------+----------------------+---------------+-------------+--------+-------------+

    """

    if resource_type != ResourceType.SCHEMA or not isinstance(resource_type, ResourceType):
        raise NotImplementedError

    tag_refs = execute(
        session,
        f"""
            SELECT *
            FROM table({fqn.database}.information_schema.tag_references(
                '{fqn}', '{str(resource_type)}'
            ))""",
    )

    if len(tag_refs) == 0:
        return None

    tag_map = {}
    for tag_ref in tag_refs:
        in_same_database = tag_ref["TAG_DATABASE"] == tag_ref["OBJECT_DATABASE"]
        in_same_schema = tag_ref["TAG_SCHEMA"] == tag_ref["OBJECT_SCHEMA"]
        tag_in_public_schema = tag_ref["TAG_SCHEMA"] == "PUBLIC"

        if in_same_database and (in_same_schema or tag_in_public_schema):
            tag_name = tag_ref["TAG_NAME"]
        else:
            tag_name = f"{tag_ref['TAG_DATABASE']}.{tag_ref['TAG_SCHEMA']}.{tag_ref['TAG_NAME']}"
        tag_map[tag_name] = tag_ref["TAG_VALUE"]
    return tag_map


def fetch_user(session, fqn: FQN):
    # SHOW USERS requires the MANAGE GRANTS privilege
    # Other roles can see the list of users but don't get access to other metadata such as login_name.
    # This causes incorrect drift
    show_result = execute(session, "SHOW USERS", cacheable=True)

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
    else:
        query_accel = False

    return {
        "name": data["name"],
        "owner": data["owner"],
        "warehouse_type": data["type"],
        "warehouse_size": str(WarehouseSize(data["size"])),
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


def list_schemas(session, database=None):
    db = f" IN DATABASE {database}" if database else ""
    try:
        show_result = execute(session, f"SHOW SCHEMAS{db}")
        return [f"{row['database_name']}.{row['name']}" for row in show_result]
    except ProgrammingError as err:
        if err.errno == OBJECT_DOES_NOT_EXIST_ERR:
            return []
        raise


def list_stages(session):
    show_result = execute(session, "SHOW STAGES IN DATABASE")
    stages = []
    for row in show_result:
        row["fqn"] = f"{row['database_name']}.{row['schema_name']}.{row['name']}"
        stages.append(row)
    return stages
