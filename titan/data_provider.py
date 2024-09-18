import datetime
import logging
import json
import sys
from functools import cache
from typing import Optional, Union, TypedDict

import pytz
from inflection import pluralize
from snowflake.connector.errors import ProgrammingError

from .builtins import (
    SYSTEM_DATABASES,
    SYSTEM_ROLES,
    SYSTEM_SECURITY_INTEGRATIONS,
    SYSTEM_USERS,
)
from .client import (
    DOES_NOT_EXIST_ERR,
    INVALID_IDENTIFIER,
    OBJECT_DOES_NOT_EXIST_ERR,
    UNSUPPORTED_FEATURE,
    execute,
)
from .enums import ResourceType, WarehouseSize
from .identifiers import FQN, URN, parse_FQN, resource_type_for_label
from .parse import (
    _parse_column,
    _parse_dynamic_table_text,
    parse_view_ddl,
    parse_collection_string,
)
from .privs import GrantedPrivilege
from .resource_name import ResourceName, attribute_is_resource_name, resource_name_from_snowflake_metadata

__this__ = sys.modules[__name__]

logger = logging.getLogger("titan")


class SessionContext(TypedDict):
    account_locator: str
    account: str
    available_roles: list[ResourceName]
    database: str
    role: str
    schemas: list[str]
    secondary_roles: list[str]
    tag_support: bool
    tags: list[str]
    user: str
    version: str
    warehouse: str
    role_privileges: dict[ResourceName, list[GrantedPrivilege]]


def _quote_snowflake_identifier(identifier: Union[str, ResourceName]) -> str:
    return str(resource_name_from_snowflake_metadata(identifier))


def _get_owner_identifier(data: dict) -> str:
    if "owner_role_type" not in data:
        return _quote_snowflake_identifier(data["owner"])
    if data["owner"] == "":
        return ""
    if data["owner_role_type"] == "DATABASE_ROLE":
        return _quote_snowflake_identifier(data["database_name"]) + "." + _quote_snowflake_identifier(data["owner"])
    elif data["owner_role_type"] == "ROLE":
        return _quote_snowflake_identifier(data["owner"])
    else:
        raise Exception(f"Unsupported owner role type: {data['owner_role_type']}, {data}")


def _desc_result_to_dict(desc_result, lower_properties=False):
    result = {}
    for row in desc_result:
        property = row["property"]
        if lower_properties:
            property = property.lower()
        result[property] = row["value"]
    return result


def _desc_type2_result_to_dict(desc_result, lower_properties=False):
    result = {}
    for row in desc_result:
        property = row["property"]
        if lower_properties:
            property = property.lower()
        value = row["property_value"]
        if row["property_type"] == "Boolean":
            value = value == "true"
        elif row["property_type"] == "Long":
            value = value or None
        elif row["property_type"] == "Integer":
            value = int(value)
        elif row["property_type"] == "String":
            value = value or None
        elif row["property_type"] == "List":
            value = _parse_list_property(value)
        # Not sure this is correct. External Access Integration uses this
        elif row["property_type"] == "Object":
            value = _parse_list_property(value)
        result[property] = value
    return result


def _desc_type3_result_to_dict(desc_result, lower_properties=False):
    result = {}
    for row in desc_result:
        parent_property = row["parent_property"]
        property = row["property"]
        if lower_properties:
            parent_property = parent_property.lower()
            property = property.lower()
        value = row["property_value"]
        if row["property_type"] == "Boolean":
            value = value == "true"
        elif row["property_type"] == "Long":
            value = value or None
        elif row["property_type"] == "Integer":
            value = int(value)
        elif row["property_type"] == "String":
            value = value or None
        elif row["property_type"] == "List":
            value = _parse_list_property(value)

        if parent_property:
            if parent_property not in result:
                result[parent_property] = {}
            result[parent_property][property] = value
        else:
            result[property] = value
    return result


def _desc_type4_result_to_dict(desc_result, lower_properties=False):
    result = {}
    for row in desc_result:
        property = row["name"]
        if lower_properties:
            property = property.lower()
        result[property] = row["value"]

    return result


def _fail_if_not_granted(result, *args):
    if len(result) == 0:
        raise Exception("Failed to create grant")
    if len(result) == 1 and result[0]["status"] == "Grant not executed: Insufficient privileges.":
        raise Exception(result[0]["status"], *args)


_INDEX = {}


def _fetch_grant_to_role(session, role: ResourceName, granted_on: str, on_name: str, privilege: str):
    grants = _show_grants_to_role(session, role, cacheable=True)
    if id(grants) not in _INDEX:
        local_index = {}
        _INDEX[id(grants)] = local_index
        for grant in grants:
            name = "ACCOUNT" if grant["granted_on"] == "ACCOUNT" else grant["name"]
            index_key = (grant["granted_on"], grant["privilege"], name)
            if index_key not in local_index:
                local_index[index_key] = grant
    else:
        local_index = _INDEX[id(grants)]

    needle = (granted_on, privilege, on_name)
    if needle in local_index:
        return local_index[needle]
    else:
        # print(local_index)
        # raise Exception(needle)
        return None


def _filter_result(result, **kwargs):

    filtered = []
    predicates = {key: value for key, value in kwargs.items() if value is not None}
    for row in result:
        for key, value in predicates.items():
            # Roughly match any names. `name`, `database_name`, `schema_name`, etc.
            if attribute_is_resource_name(key):
                if resource_name_from_snowflake_metadata(row[key]) != ResourceName(value):
                    # if ResourceName(value) != f'"{row[key]}"':
                    break
            else:
                if row[key] != value:
                    break
        else:
            filtered.append(row)
    return filtered


# def _urn_from_grant(row, session_ctx):
#     account_scoped_resources = {"user", "role", "warehouse", "database", "task"}
#     granted_on = row["granted_on"].lower()
#     if granted_on == "account":
#         return URN.from_session_ctx(session_ctx)
#     else:
#         if granted_on == "procedure" or granted_on == "function":
#             # This needs a special function because Snowflake gives an incorrect FQN for functions/sprocs
#             # eg. TITAN_DEV.PUBLIC."FETCH_DATABASE(NAME VARCHAR):OBJECT"
#             # The correct FQN is TITAN_DEV.PUBLIC."FETCH_DATABASE"(VARCHAR)
#             id_parts = list(FullyQualifiedIdentifier.parse_string(row["name"], parse_all=True))
#             name = parse_function_name(id_parts[-1])
#             fqn = FQN(database=id_parts[0], schema=id_parts[1], name=name)
#         elif granted_on in account_scoped_resources:
#             # This is probably all account-scoped resources
#             fqn = FQN(name=ResourceName(row["name"]))
#         else:
#             # Scoped resources
#             fqn = parse_FQN(row["name"], is_db_scoped=(granted_on == "schema"))
#         return URN(
#             resource_type=ResourceType(granted_on),
#             account_locator=session_ctx["account_locator"],
#             fqn=fqn,
#         )


def _convert_to_gmt(dt: datetime.datetime, fmt_str: str = "%Y-%m-%d %H:%M:%S") -> Optional[str]:
    """
    datetime.datetime(2049, 1, 6, 12, 0, tzinfo=<DstTzInfo 'America/Los_Angeles' PST-1 day, 16:00:00 STD>)

    =>

    2049-01-06 20:00
    """
    if not dt:
        return None
    gmt = pytz.timezone("GMT")
    dt_gmt = dt.astimezone(gmt)
    return dt_gmt.strftime(fmt_str)


def _parse_cluster_keys(cluster_keys_str: str) -> Optional[list[str]]:
    """
    Assume cluster key statement is in the form of:
        LINEAR(C1, C3)
        LINEAR(SUBSTRING(C2, 5, 15), CAST(C1 AS DATE))
    """
    if cluster_keys_str is None or cluster_keys_str == "":
        return None
    cluster_keys_str = cluster_keys_str[len("LINEAR") :]
    cluster_keys_str = cluster_keys_str.strip("()")
    return [key.strip(" ") for key in cluster_keys_str.split(",")]


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
    identifier = parse_FQN(header)
    return (identifier, returns)


def _parse_function_arguments(arguments_str: str) -> tuple[FQN, str]:
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
    identifier = parse_FQN(header)
    return (identifier, returns)


def _parse_list_property(property_str: str) -> list:
    if property_str is None:
        return []
    property_str = property_str.strip("[]")
    if property_str:
        return [item.strip(" ") for item in property_str.split(",")]
    return []


def _parse_signature(signature: str) -> list:
    signature = signature.strip("()")

    if signature:
        return [_parse_column(col.strip(" ")) for col in signature.split(",")]
    return []


def _parse_comma_separated_values(values: str) -> Optional[list]:
    if values is None or values == "":
        return None
    return [value.strip(" ") for value in values.split(",")]


def _parse_packages(packages_str: str) -> Optional[list]:
    if packages_str is None or packages_str == "":
        return None
    return json.loads(packages_str.replace("'", '"'))


def _parse_storage_location(storage_location_str: str) -> Optional[dict]:
    if storage_location_str is None or storage_location_str == "":
        return None
    raw_dict = json.loads(storage_location_str)
    storage_location = {}
    for key, value in raw_dict.items():
        key = key.lower()
        if key == "encryption_type":
            storage_location["encryption"] = {"type": value}
        elif key in (
            "name",
            "storage_provider",
            "storage_base_url",
            "storage_aws_role_arn",
            "storage_aws_external_id",
        ):
            storage_location[key] = value
    return storage_location


def params_result_to_dict(params_result):
    params = {}
    for param in params_result:
        if param["type"] == "BOOLEAN":
            typed_value = param["value"] == "true"
        elif param["type"] == "NUMBER":
            typed_value = int(param["value"])
        elif param["type"] == "STRING":
            typed_value = str(param["value"]) if param["value"] else None
        else:
            typed_value = param["value"]
        params[param["key"].lower()] = typed_value
    return params


def options_result_to_list(options_result):
    return [option.strip(" ") for option in options_result.split(",")]


def remove_none_values(d):
    new_dict = {}
    for k, v in d.items():
        if isinstance(v, dict):
            new_dict[k] = remove_none_values(v)
        elif isinstance(v, list) and len(v) > 0 and isinstance(v[0], dict):
            new_dict[k] = [remove_none_values(item) for item in v if item is not None]
        elif v is not None:
            new_dict[k] = v
    return new_dict


def _fetch_columns_for_table(session, fqn: FQN):
    info_schema_result = execute(session, f"SELECT * FROM {fqn.database}.INFORMATION_SCHEMA.COLUMNS", cacheable=True)
    columns = []
    for col in info_schema_result:

        if (
            resource_name_from_snowflake_metadata(col["TABLE_SCHEMA"]) != fqn.schema
            or resource_name_from_snowflake_metadata(col["TABLE_NAME"]) != fqn.name
        ):
            continue

        data_type = None
        default = col["COLUMN_DEFAULT"]

        if col["DATA_TYPE"] == "NUMBER":
            data_type = f"NUMBER({col['NUMERIC_PRECISION']}, {col['NUMERIC_SCALE']})"
        elif col["DATA_TYPE"] == "TEXT":
            data_type = f"VARCHAR({col['CHARACTER_MAXIMUM_LENGTH']})"
            if col["COLUMN_DEFAULT"]:
                default = col["COLUMN_DEFAULT"].strip("'")
        else:
            data_type = col["DATA_TYPE"]

        columns.append(
            {
                "name": col["COLUMN_NAME"],
                "data_type": data_type,
                "not_null": col["IS_NULLABLE"] == "NO",
                "default": default,
                "comment": col["COMMENT"] or None,
                "constraint": None,
                "collate": None,
            }
        )
    return columns


def _fetch_owner(session, type_str: str, fqn: FQN) -> Optional[str]:
    show_grants = execute(session, f"SHOW GRANTS ON {type_str} {fqn}")
    ownership_grant = _filter_result(show_grants, privilege="OWNERSHIP")
    if len(ownership_grant) == 0:
        return None
    return ownership_grant[0]["grantee_name"]


def _show_resources(session, type_str, fqn: FQN, cacheable: bool = True) -> list[dict]:
    try:
        in_account = " IN ACCOUNT"
        if "INTEGRATIONS" in type_str:
            in_account = ""
        initial_fetch = execute(session, f"SHOW {type_str}{in_account}", cacheable=cacheable)
        if len(initial_fetch) == 0:
            return []
        elif len(initial_fetch) < 1000:
            container_kwargs = {}
            show_columns = initial_fetch[0].keys()
            if "database" in show_columns:
                container_kwargs["database"] = fqn.database
            elif "database_name" in show_columns:
                container_kwargs["database_name"] = fqn.database

            if "schema" in show_columns:
                container_kwargs["schema"] = fqn.schema
            elif "schema_name" in show_columns:
                container_kwargs["schema_name"] = fqn.schema
            filtered_fetch = _filter_result(
                initial_fetch,
                name=fqn.name,
                **container_kwargs,
            )
            return filtered_fetch
        else:

            if fqn.database is None and fqn.schema is None:
                return execute(session, f"SHOW {type_str} LIKE '{fqn.name}'", cacheable=cacheable)
            elif fqn.database is None:
                return execute(
                    session, f"SHOW {type_str} LIKE '{fqn.name}' IN SCHEMA {fqn.schema}", cacheable=cacheable
                )
            elif fqn.schema is None:
                return execute(
                    session, f"SHOW {type_str} LIKE '{fqn.name}' IN DATABASE {fqn.database}", cacheable=cacheable
                )
            else:
                return execute(
                    session,
                    f"SHOW {type_str} LIKE '{fqn.name}' IN SCHEMA {fqn.database}.{fqn.schema}",
                    cacheable=cacheable,
                )
    except ProgrammingError as err:
        if err.errno == OBJECT_DOES_NOT_EXIST_ERR or err.errno == DOES_NOT_EXIST_ERR:
            return []
        else:
            raise


def _show_resource_parameters(session, type_str: str, fqn: FQN, cacheable: bool = True) -> dict:
    result = execute(session, f"SHOW PARAMETERS IN {type_str} {fqn}", cacheable=cacheable)
    return params_result_to_dict(result)


def _show_grants_to_role(session, role: ResourceName, cacheable: bool = False) -> list:
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
    grants = execute(
        session,
        f"SHOW GRANTS TO ROLE {role}",
        cacheable=cacheable,
        empty_response_codes=[DOES_NOT_EXIST_ERR],
    )
    return grants


def fetch_resource(session, urn: URN) -> Optional[dict]:
    try:
        return getattr(__this__, f"fetch_{urn.resource_label}")(session, urn.fqn)
    except ProgrammingError as err:
        # This try/catch block fixes a cache-inconsistency issue where _show_resources returns the object as it existed at the start of the cache window,
        # but _show_resource_parameters returns the object as it exists right now. If the object was dropped in between the cache window and the query execution,
        # we should assume the database no longer exists.

        # This is only likely to happen for long-running commands like export
        if err.errno == DOES_NOT_EXIST_ERR:
            return None
        raise


def fetch_account_locator(session):
    locator = execute(session, "SELECT CURRENT_ACCOUNT() as account_locator")[0]["ACCOUNT_LOCATOR"]
    return locator


def fetch_region(session):
    region = execute(session, "SELECT CURRENT_REGION()")[0]
    return region


@cache
def fetch_session(session) -> SessionContext:
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
            CURRENT_VERSION() as version
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

    available_roles = [ResourceName(role) for role in json.loads(session_obj["AVAILABLE_ROLES"])]

    role_privileges = {}
    for role in available_roles:

        # Adds 30+s of latency and we can infer what privs are available
        if role == "ACCOUNTADMIN" or role.startswith("SNOWFLAKE."):
            continue

        role_privileges[role] = []

        grants = _show_grants_to_role(session, role, cacheable=True)
        for grant in grants:
            try:
                granted_priv = GrantedPrivilege.from_grant(
                    privilege=grant["privilege"],
                    granted_on=grant["granted_on"].replace("_", " "),
                    name=grant["name"],
                )
                role_privileges[role].append(granted_priv)
            # If titan isnt aware of the privilege, ignore it
            except ValueError:
                continue

    return {
        "account_locator": session_obj["ACCOUNT_LOCATOR"],
        "account": session_obj["ACCOUNT"],
        "available_roles": available_roles,
        "database": session_obj["DATABASE"],
        "role": session_obj["ROLE"],
        "schemas": json.loads(session_obj["SCHEMAS"]),
        "secondary_roles": json.loads(session_obj["SECONDARY_ROLES"]),
        "tag_support": tag_support,
        "tags": tags,
        "user": session_obj["USER"],
        "version": session_obj["VERSION"],
        "warehouse": session_obj["WAREHOUSE"],
        "role_privileges": role_privileges,
    }


# ------------------------------
# Fetch Resources
# ------------------------------


def fetch_account(session, fqn: FQN):
    # raise NotImplementedError()
    return {}


def fetch_aggregation_policy(session, fqn: FQN):
    show_result = _show_resources(session, "AGGREGATION POLICIES", fqn)
    if len(show_result) == 0:
        return None
    if len(show_result) > 1:
        raise Exception(f"Found multiple aggregation policies matching {fqn}")
    data = show_result[0]
    desc_result = execute(session, f"DESC AGGREGATION POLICY {fqn}")
    properties = desc_result[0]
    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "body": properties["body"],
        "owner": _get_owner_identifier(data),
    }


def fetch_alert(session, fqn: FQN):
    show_result = execute(session, "SHOW ALERTS", cacheable=True)
    alerts = _filter_result(show_result, name=fqn.name)
    if len(alerts) == 0:
        return None
    if len(alerts) > 1:
        raise Exception(f"Found multiple alerts matching {fqn}")
    data = alerts[0]
    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "warehouse": data["warehouse"],
        "schedule": data["schedule"],
        "comment": data["comment"] or None,
        "condition": data["condition"],
        "then": data["action"],
        "owner": _get_owner_identifier(data),
    }


def fetch_api_integration(session, fqn: FQN):
    integrations = _show_resources(session, "API INTEGRATIONS", fqn)
    if len(integrations) == 0:
        return None
    if len(integrations) > 1:
        raise Exception(f"Found multiple api integrations matching {fqn}")
    data = integrations[0]
    desc_result = execute(session, f"DESC API INTEGRATION {fqn}")
    properties = _desc_type2_result_to_dict(desc_result, lower_properties=True)
    owner = _fetch_owner(session, "INTEGRATION", fqn)

    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "api_provider": properties["api_provider"],
        "api_aws_role_arn": properties["api_aws_role_arn"],
        "enabled": properties["enabled"],
        "api_allowed_prefixes": properties["api_allowed_prefixes"],
        "api_blocked_prefixes": properties["api_blocked_prefixes"],
        "owner": owner,
        "comment": data["comment"] or None,
    }


def fetch_authentication_policy(session, fqn: FQN):
    policies = _show_resources(session, "AUTHENTICATION POLICIES", fqn)
    if len(policies) == 0:
        return None
    if len(policies) > 1:
        raise Exception(f"Found multiple authentication policies matching {fqn}")
    data = policies[0]
    desc_result = execute(session, f"DESC AUTHENTICATION POLICY {fqn}")
    properties = _desc_result_to_dict(desc_result, lower_properties=True)

    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "authentication_methods": _parse_list_property(properties["authentication_methods"]),
        "mfa_authentication_methods": _parse_list_property(properties["mfa_authentication_methods"]),
        "mfa_enrollment": properties["mfa_enrollment"],
        "client_types": _parse_list_property(properties["client_types"]),
        "security_integrations": _parse_list_property(properties["security_integrations"]),
        "comment": data["comment"] or None,
        "owner": _get_owner_identifier(data),
    }


def fetch_catalog_integration(session, fqn: FQN):
    integrations = _show_resources(session, "CATALOG INTEGRATIONS", fqn)
    if len(integrations) == 0:
        return None
    if len(integrations) > 1:
        raise Exception(f"Found multiple catalog integrations matching {fqn}")

    data = integrations[0]
    desc_result = execute(session, f"DESC CATALOG INTEGRATION {fqn}")
    properties = _desc_type2_result_to_dict(desc_result, lower_properties=True)
    owner = _fetch_owner(session, "INTEGRATION", fqn)

    if properties["catalog_source"] == "GLUE":
        return {
            "name": _quote_snowflake_identifier(data["name"]),
            "catalog_source": properties["catalog_source"],
            "catalog_namespace": properties["catalog_namespace"],
            "table_format": properties["table_format"],
            "glue_aws_role_arn": properties["glue_aws_role_arn"],
            "glue_catalog_id": properties["glue_catalog_id"],
            "glue_region": properties["glue_region"],
            "enabled": properties["enabled"],
            "owner": owner,
            "comment": data["comment"] or None,
        }
    elif properties["catalog_source"] == "OBJECT_STORE":
        return {
            "name": _quote_snowflake_identifier(data["name"]),
            "catalog_source": properties["catalog_source"],
            "table_format": properties["table_format"],
            "enabled": properties["enabled"],
            "owner": owner,
            "comment": data["comment"] or None,
        }
    else:
        raise Exception(f"Unsupported catalog integration: {properties['catalog_source']}")


def fetch_columns(session, resource_type: str, fqn: FQN):
    desc_result = execute(session, f"DESC {resource_type} {fqn}")
    columns = []
    for col in desc_result:
        if col["kind"] != "COLUMN":
            raise Exception(f"Unexpected kind {col['kind']} in desc result")
        columns.append(
            # remove_none_values(
            {
                "name": col["name"],
                "data_type": col["type"],
                "not_null": col["null?"] == "N",
                "default": col["default"],
                "comment": col["comment"] or None,
                "constraint": None,
                "collate": None,
            }
            # )
        )
    return columns


def fetch_compute_pool(session, fqn: FQN):
    show_result = execute(session, f"SHOW COMPUTE POOLS LIKE '{fqn.name}'", cacheable=True)

    if len(show_result) == 0:
        return None
    if len(show_result) > 1:
        raise Exception(f"Found multiple compute pools matching {fqn}")

    data = show_result[0]

    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "owner": _get_owner_identifier(data),
        "min_nodes": data["min_nodes"],
        "max_nodes": data["max_nodes"],
        "instance_family": data["instance_family"],
        "auto_resume": data["auto_resume"] == "true",
        "auto_suspend_secs": data["auto_suspend_secs"],
        "comment": data["comment"] or None,
    }


def fetch_database(session, fqn: FQN):
    show_result = _show_resources(session, "DATABASES", fqn)

    if len(show_result) == 0:
        return None
    if len(show_result) > 1:
        raise Exception(f"Found multiple databases matching {fqn}")

    data = show_result[0]

    is_standard_db = data["kind"] == "STANDARD"
    is_snowflake_builtin = data["kind"] == "APPLICATION" and data["name"] in SYSTEM_DATABASES

    if not (is_standard_db or is_snowflake_builtin):
        return None

    options = options_result_to_list(data["options"])
    params = _show_resource_parameters(session, "DATABASE", fqn)

    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "data_retention_time_in_days": int(data["retention_time"]),
        "comment": data["comment"] or None,
        "transient": "TRANSIENT" in options,
        "owner": _get_owner_identifier(data),
        "max_data_extension_time_in_days": params.get("max_data_extension_time_in_days"),
        "external_volume": params.get("external_volume"),
        "catalog": params.get("catalog"),
        "default_ddl_collation": params["default_ddl_collation"],
    }


def fetch_database_role(session, fqn: FQN):
    try:
        show_result = execute(session, f"SHOW DATABASE ROLES IN DATABASE {fqn.database}", cacheable=True)
    except ProgrammingError as err:
        if err.errno == DOES_NOT_EXIST_ERR:
            return None
        raise

    roles = _filter_result(show_result, name=fqn.name)
    if len(roles) == 0:
        return None
    if len(roles) > 1:
        raise Exception(f"Found multiple database roles matching {fqn}")
    data = roles[0]
    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "owner": _get_owner_identifier(data),
        "database": fqn.database,
        "comment": data["comment"] or None,
    }


def fetch_dynamic_table(session, fqn: FQN):
    show_result = execute(session, f"SHOW DYNAMIC TABLES LIKE '{fqn.name}'")

    if len(show_result) == 0:
        return None
    if len(show_result) > 1:
        raise Exception(f"Found multiple dynamic tables matching {fqn}")

    columns = fetch_columns(session, "DYNAMIC TABLE", fqn)
    columns = [{"name": col["name"], "comment": col["comment"]} for col in columns]

    data = show_result[0]
    refresh_mode, initialize, as_ = _parse_dynamic_table_text(data["text"])
    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "owner": _get_owner_identifier(data),
        "warehouse": data["warehouse"],
        "refresh_mode": refresh_mode,
        "initialize": initialize,
        "target_lag": data["target_lag"],
        "comment": data["comment"] or None,
        "columns": columns,
        "as_": as_,
    }


def fetch_event_table(session, fqn: FQN):
    show_result = execute(session, "SHOW EVENT TABLES IN ACCOUNT")

    tables = _filter_result(show_result, name=fqn.name, database_name=fqn.database, schema_name=fqn.schema)

    if len(tables) == 0:
        return None
    if len(tables) > 1:
        raise Exception(f"Found multiple tables matching {fqn}")

    data = tables[0]
    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "comment": data["comment"] or None,
        "cluster_by": _parse_cluster_keys(data["cluster_by"]),
        "data_retention_time_in_days": int(data["retention_time"]),
        "change_tracking": data["change_tracking"] == "ON",
        "owner": _get_owner_identifier(data),
    }


def fetch_external_access_integration(session, fqn: FQN):
    integrations = _show_resources(session, "EXTERNAL ACCESS INTEGRATIONS", fqn)
    if len(integrations) == 0:
        return None
    if len(integrations) > 1:
        raise Exception(f"Found multiple external access integrations matching {fqn}")

    data = integrations[0]
    desc_result = execute(session, f"DESC EXTERNAL ACCESS INTEGRATION {fqn}", cacheable=True)
    properties = _desc_type2_result_to_dict(desc_result, lower_properties=True)
    owner = _fetch_owner(session, "INTEGRATION", fqn)
    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "allowed_network_rules": properties["allowed_network_rules"],
        "allowed_api_authentication_integrations": properties["allowed_api_authentication_integrations"] or None,
        "allowed_authentication_secrets": properties["allowed_authentication_secrets"] or None,
        "enabled": data["enabled"] == "true",
        "owner": owner,
        "comment": data["comment"] or None,
    }


def fetch_external_volume(session, fqn: FQN):
    show_result = _show_resources(session, "EXTERNAL VOLUMES", fqn)
    if len(show_result) == 0:
        return None
    if len(show_result) > 1:
        raise Exception(f"Found multiple external volumes matching {fqn}")

    data = show_result[0]
    desc_result = execute(session, f"DESC EXTERNAL VOLUME {fqn}", cacheable=True)
    properties = _desc_type3_result_to_dict(desc_result, lower_properties=True)
    owner = _fetch_owner(session, "VOLUME", fqn)

    storage_locations = []
    index = 1
    while True:
        storage_location = properties["storage_locations"].get(f"storage_location_{index}")
        if storage_location is None:
            break
        storage_locations.append(_parse_storage_location(storage_location))
        index += 1

    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "owner": owner,
        "storage_locations": storage_locations,
        "allow_writes": data["allow_writes"] == "true",
        "comment": data["comment"] or None,
    }


def fetch_file_format(session, fqn: FQN):
    show_result = _show_resources(session, "FILE FORMATS", fqn)
    if len(show_result) == 0:
        return None
    if len(show_result) > 1:
        raise Exception(f"Found multiple file formats matching {fqn}")

    data = show_result[0]
    format_options = json.loads(data["format_options"])

    if data["type"] == "CSV":
        return {
            "name": _quote_snowflake_identifier(data["name"]),
            "type": data["type"],
            "owner": _get_owner_identifier(data),
            "field_delimiter": format_options["FIELD_DELIMITER"],
            "skip_header": format_options["SKIP_HEADER"],
            "null_if": format_options["NULL_IF"],
            "empty_field_as_null": format_options["EMPTY_FIELD_AS_NULL"],
            "compression": format_options["COMPRESSION"],
            "record_delimiter": format_options["RECORD_DELIMITER"],
            "file_extension": format_options["FILE_EXTENSION"],
            "parse_header": format_options["PARSE_HEADER"],
            "skip_blank_lines": format_options["SKIP_BLANK_LINES"],
            "date_format": format_options["DATE_FORMAT"],
            "time_format": format_options["TIME_FORMAT"],
            "timestamp_format": format_options["TIMESTAMP_FORMAT"],
            "binary_format": format_options["BINARY_FORMAT"],
            "escape": format_options["ESCAPE"] if format_options["ESCAPE"] != "NONE" else None,
            "escape_unenclosed_field": format_options["ESCAPE_UNENCLOSED_FIELD"],
            "trim_space": format_options["TRIM_SPACE"],
            "field_optionally_enclosed_by": (
                format_options["FIELD_OPTIONALLY_ENCLOSED_BY"]
                if format_options["FIELD_OPTIONALLY_ENCLOSED_BY"] != "NONE"
                else None
            ),
            "error_on_column_count_mismatch": format_options["ERROR_ON_COLUMN_COUNT_MISMATCH"],
            "replace_invalid_characters": format_options["REPLACE_INVALID_CHARACTERS"],
            "skip_byte_order_mark": format_options["SKIP_BYTE_ORDER_MARK"],
            "encoding": format_options["ENCODING"],
            "comment": data["comment"] or None,
        }
    elif data["type"] == "PARQUET":
        return {
            "name": _quote_snowflake_identifier(data["name"]),
            "type": data["type"],
            "owner": _get_owner_identifier(data),
            "comment": data["comment"] or None,
            "compression": format_options["COMPRESSION"],
            "binary_as_text": format_options["BINARY_AS_TEXT"],
            "trim_space": format_options["TRIM_SPACE"],
            "replace_invalid_characters": format_options["REPLACE_INVALID_CHARACTERS"],
            "null_if": format_options["NULL_IF"],
        }
    elif data["type"] == "JSON":
        return {
            "name": _quote_snowflake_identifier(data["name"]),
            "type": data["type"],
            "owner": _get_owner_identifier(data),
            "comment": data["comment"] or None,
            "compression": format_options["COMPRESSION"],
            "date_format": format_options["DATE_FORMAT"],
            "time_format": format_options["TIME_FORMAT"],
            "timestamp_format": format_options["TIMESTAMP_FORMAT"],
            "binary_format": format_options["BINARY_FORMAT"],
            "trim_space": format_options["TRIM_SPACE"],
            "null_if": format_options["NULL_IF"],
            "file_extension": format_options["FILE_EXTENSION"],
            "enable_octal": format_options["ENABLE_OCTAL"],
            "allow_duplicate": format_options["ALLOW_DUPLICATE"],
            "strip_outer_array": format_options["STRIP_OUTER_ARRAY"],
            "strip_null_values": format_options["STRIP_NULL_VALUES"],
            "replace_invalid_characters": format_options["REPLACE_INVALID_CHARACTERS"],
            "ignore_utf8_errors": format_options["IGNORE_UTF8_ERRORS"],
            "skip_byte_order_mark": format_options["SKIP_BYTE_ORDER_MARK"],
        }
    else:
        raise Exception(f"Unsupported file format type: {data['type']}")


def fetch_function(session, fqn: FQN):
    udfs = _show_resources(session, "USER FUNCTIONS", fqn)
    if len(udfs) == 0:
        return None
    if len(udfs) > 1:
        raise Exception(f"Found multiple functions matching {fqn}")

    data = udfs[0]
    inputs, output = data["arguments"].split(" RETURN ")
    try:
        desc_result = execute(session, f"DESC FUNCTION {inputs}", cacheable=True)
    except ProgrammingError as err:
        if err.errno == DOES_NOT_EXIST_ERR:
            return None
        raise
    properties = _desc_result_to_dict(desc_result)
    owner = _fetch_owner(session, "FUNCTION", fqn)

    if data["language"] == "PYTHON":
        return {
            "name": _quote_snowflake_identifier(data["name"]),
            "secure": data["is_secure"] == "Y",
            "args": _parse_signature(properties["signature"]),
            "returns": output,
            "language": data["language"],
            "comment": None if data["description"] == "user-defined function" else data["description"],
            "volatility": properties["volatility"],
            "as_": properties["body"],
            "owner": owner,
        }
    elif data["language"] == "JAVASCRIPT":
        return {
            "name": _quote_snowflake_identifier(data["name"]),
            "secure": data["is_secure"] == "Y",
            "args": _parse_signature(properties["signature"]),
            "returns": output,
            "language": data["language"],
            "comment": None if data["description"] == "user-defined function" else data["description"],
            "volatility": properties["volatility"],
            "as_": properties["body"],
            "owner": owner,
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
        if err.errno == DOES_NOT_EXIST_ERR:
            return None
        raise

    _, collection_str = fqn.params["on"].split("/")
    collection = parse_collection_string(collection_str)

    # If the resource we want to grant on isn't fully qualified in the FQN for the future grant, this filter step will fail.
    # For example:
    # fetch_future_grant(...,
    #   FQN(name=FUTURE_GRANT_ROLE_CBDBE971?priv=SELECT&on=schema/PUBLIC.<TABLE>)
    # )
    # name = "PUBLIC.<TABLE>"
    #
    # Whereas in SHOW FUTURE GRANTS
    # - name: 'TEST_DB_RUN_CBDBE971.PUBLIC.<TABLE>'

    # 'STATIC_DATABASE.<TABLE>'

    grants = _filter_result(
        show_result,
        privilege=fqn.params["priv"],
        name=collection_str,
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
        "on_type": str(resource_type_for_label(data["grant_on"])),
        "in_type": collection["in_type"].upper(),
        "in_name": collection["in_name"],
        "to": data["grantee_name"],
        "grant_option": data["grant_option"] == "true",
    }


def fetch_grant(session, fqn: FQN):
    priv = fqn.params["priv"]
    on_type, on = fqn.params["on"].split("/")
    on_type = on_type.upper()

    if priv == "ALL":

        filters = {
            "granted_on": on_type,
        }

        if on_type != "ACCOUNT":
            filters["name"] = on

        grants = _show_grants_to_role(session, fqn.name, cacheable=True)
        grants = _filter_result(grants, **filters)

        if len(grants) == 0:
            return None

        data = grants[0]
        privs = sorted([g["privilege"] for g in grants])

    else:
        data = _fetch_grant_to_role(
            session,
            role=fqn.name,
            granted_on=on_type,
            on_name=on,
            privilege=priv,
        )
        if data is None:
            return None
        privs = [priv]

    # elif len(grants) > 1 and priv != "ALL":
    #     # This is likely to happen when a grant has been issued by ACCOUNTADMIN
    #     # and some other role with MANAGE GRANTS or OWNERSHIP. It needs to be properly
    #     # handled in the future.
    #     raise Exception(f"Found multiple grants matching {fqn}")

    return {
        "priv": priv,
        "on": "ACCOUNT" if on_type == "ACCOUNT" else data["name"],
        "on_type": data["granted_on"].replace("_", " "),
        "to": data["grantee_name"],
        "grant_option": data["grant_option"] == "true",
        "owner": data["granted_by"],
        "_privs": privs,
    }


def fetch_grant_on_all(session, fqn: FQN):
    # All grants are expensive to fetch, so we will assume they are always out of date
    return None


def fetch_iceberg_table(session, fqn: FQN):
    tables = _show_resources(session, "ICEBERG TABLES", fqn)
    if len(tables) == 0:
        return None
    if len(tables) > 1:
        raise Exception(f"Found multiple iceberg tables matching {fqn}")

    data = tables[0]
    columns = fetch_columns(session, "ICEBERG TABLE", fqn)
    show_params_result = execute(session, f"SHOW PARAMETERS FOR TABLE {fqn}")
    params = params_result_to_dict(show_params_result)
    return {
        "name": fqn.name,
        "owner": data["owner"],
        "columns": columns,
        "external_volume": data["external_volume_name"],
        "catalog": data["catalog_name"],
        "base_location": data["base_location"].rstrip("/"),
        "catalog_sync": params["catalog_sync"] or None,
        "storage_serialization_policy": params["storage_serialization_policy"],
        "data_retention_time_in_days": params["data_retention_time_in_days"],
        "max_data_extension_time_in_days": params["max_data_extension_time_in_days"],
        # "change_tracking": data["change_tracking"],
        "default_ddl_collation": params["default_ddl_collation"] or None,
        "comment": data["comment"] or None,
    }


def fetch_image_repository(session, fqn: FQN):
    repos = _show_resources(session, "IMAGE REPOSITORIES", fqn)

    if len(repos) == 0:
        return None
    if len(repos) > 1:
        raise Exception(f"Found multiple image repositories matching {fqn}")

    data = repos[0]

    return {"name": fqn.name, "owner": _get_owner_identifier(data)}


def fetch_materialized_view(session, fqn: FQN):
    materialized_views = _show_resources(session, "MATERIALIZED VIEWS", fqn)
    if len(materialized_views) == 0:
        return None
    if len(materialized_views) > 1:
        raise Exception(f"Found multiple materialized views matching {fqn}")

    data = materialized_views[0]
    columns = fetch_columns(session, "VIEW", fqn)

    return {
        "name": fqn.name,
        "owner": _get_owner_identifier(data),
        "secure": data["is_secure"] == "true",
        "columns": columns,
        "cluster_by": _parse_cluster_keys(data["cluster_by"]),
        "comment": data["comment"] or None,
        "as_": parse_view_ddl(data["text"]),
    }


def fetch_network_policy(session, fqn: FQN):
    policies = _show_resources(session, "NETWORK POLICIES", fqn)
    if len(policies) == 0:
        return None
    if len(policies) > 1:
        raise Exception(f"Found multiple network policies matching {fqn}")

    data = policies[0]
    desc_result = execute(session, f"DESC NETWORK POLICY {fqn}", cacheable=True)
    properties = _desc_type4_result_to_dict(desc_result, lower_properties=True)

    allowed_network_rule_list = None
    if "allowed_network_rule_list" in properties:
        allowed_network_rule_list = [
            rule["fullyQualifiedRuleName"] for rule in json.loads(properties["allowed_network_rule_list"])
        ]
    blocked_network_rule_list = None
    if "blocked_network_rule_list" in properties:
        blocked_network_rule_list = [
            rule["fullyQualifiedRuleName"] for rule in json.loads(properties["blocked_network_rule_list"])
        ]
    allowed_ip_list = None
    if "allowed_ip_list" in properties:
        allowed_ip_list = properties["allowed_ip_list"].split(",")
    blocked_ip_list = None
    if "blocked_ip_list" in properties:
        blocked_ip_list = properties["blocked_ip_list"].split(",")

    owner = _fetch_owner(session, "NETWORK POLICY", fqn)

    return {
        "name": data["name"],
        "allowed_network_rule_list": allowed_network_rule_list,
        "blocked_network_rule_list": blocked_network_rule_list,
        "allowed_ip_list": allowed_ip_list,
        "blocked_ip_list": blocked_ip_list,
        "comment": data["comment"] or None,
        "owner": owner,
    }


def fetch_network_rule(session, fqn: FQN):
    show_result = _show_resources(session, "NETWORK RULES", fqn)

    if len(show_result) == 0:
        return None
    if len(show_result) > 1:
        raise Exception(f"Found multiple network rules matching {fqn}")

    desc_result = execute(session, f"DESC NETWORK RULE {fqn}", cacheable=True)
    properties = desc_result[0]

    data = show_result[0]
    return {
        "name": fqn.name,
        "owner": _get_owner_identifier(data),
        "type": data["type"],
        "value_list": _parse_comma_separated_values(properties["value_list"]),
        "mode": data["mode"],
        "comment": data["comment"] or None,
    }


def fetch_notebook(session, fqn: FQN):
    notebooks = _show_resources(session, "NOTEBOOKS", fqn)
    if len(notebooks) == 0:
        return None
    if len(notebooks) > 1:
        raise Exception(f"Found multiple notebooks matching {fqn}")

    data = notebooks[0]
    desc_result = execute(session, f"DESC NOTEBOOK {fqn}", cacheable=True)
    properties = desc_result[0]
    return {
        "name": data["name"],
        "main_file": None if properties["main_file"] == "notebook_app.ipynb" else properties["main_file"],
        "query_warehouse": data["query_warehouse"],
        "comment": data["comment"],
        "owner": _get_owner_identifier(data),
        # "version": data["version"],
    }


def fetch_notification_integration(session, fqn: FQN):
    show_result = execute(session, f"SHOW NOTIFICATION INTEGRATIONS LIKE '{fqn.name}'")
    if len(show_result) == 0:
        return None
    if len(show_result) > 1:
        raise Exception(f"Found multiple notification integrations matching {fqn}")

    data = show_result[0]
    desc_result = execute(session, f"DESC NOTIFICATION INTEGRATION {fqn.name}")
    properties = _desc_type2_result_to_dict(desc_result, lower_properties=True)

    owner = _fetch_owner(session, "INTEGRATION", fqn)

    if data["type"] == "EMAIL":

        return {
            "name": _quote_snowflake_identifier(data["name"]),
            "type": data["type"],
            "enabled": data["enabled"] == "true",
            "allowed_recipients": properties["allowed_recipients"],
            "owner": owner,
            "comment": data["comment"] or None,
        }
    else:
        raise Exception(f"Unsupported notification integration type: {data['type']}")


def fetch_packages_policy(session, fqn: FQN):
    show_result = _show_resources(session, "PACKAGES POLICIES", fqn)
    if len(show_result) == 0:
        return None
    if len(show_result) > 1:
        raise Exception(f"Found multiple packages policies matching {fqn}")

    data = show_result[0]
    desc_result = execute(session, f"DESC PACKAGES POLICY {fqn}")
    properties = desc_result[0]

    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "language": properties["language"],
        "allowlist": _parse_packages(properties["allowlist"]),
        "blocklist": _parse_packages(properties["blocklist"]),
        "additional_creation_blocklist": _parse_packages(properties["additional_creation_blocklist"]),
        "comment": data["comment"] or None,
        "owner": _get_owner_identifier(data),
    }


def fetch_password_policy(session, fqn: FQN):
    policies = _show_resources(session, "PASSWORD POLICIES", fqn)
    if len(policies) == 0:
        return None
    if len(policies) > 1:
        raise Exception(f"Found multiple password policies matching {fqn}")

    data = policies[0]
    desc_result = execute(session, f"DESC PASSWORD POLICY {fqn}")
    properties = _desc_result_to_dict(desc_result)

    comment = properties["COMMENT"] if properties["COMMENT"] != "null" else None

    return {
        "name": _quote_snowflake_identifier(data["name"]),
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
        "comment": comment,
        "owner": properties["OWNER"],
    }


def fetch_pipe(session, fqn: FQN):
    show_result = _show_resources(session, "PIPES", fqn)
    if len(show_result) == 0:
        return None
    if len(show_result) > 1:
        raise Exception(f"Found multiple pipes matching {fqn}")

    data = show_result[0]

    # desc_result = execute(session, f"DESC PIPE {fqn}", cacheable=True)

    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "as_": data["definition"],
        "owner": _get_owner_identifier(data),
        "error_integration": data["error_integration"],
        # "aws_sns_topic": data["aws_sns_topic"],
        "integration": data["integration"],
        "comment": data["comment"],
    }


def fetch_procedure(session, fqn: FQN):
    # SHOW PROCEDURES IN SCHEMA {}.{}
    # FIXME: This will fail if the database doesnt exist
    show_result = execute(session, f"SHOW PROCEDURES IN SCHEMA {fqn.database}.{fqn.schema}", cacheable=True)
    sprocs = _filter_result(show_result, name=fqn.name)
    if len(sprocs) == 0:
        return None
    if len(sprocs) > 1:
        raise Exception(f"Found multiple stored procedures matching {fqn}")

    data = sprocs[0]

    identifier, returns = _parse_function_arguments(data["arguments"])
    desc_result = execute(session, f"DESC PROCEDURE {fqn.database}.{fqn.schema}.{str(identifier)}", cacheable=True)
    properties = _desc_result_to_dict(desc_result)

    # show_grants = execute(session, f"SHOW GRANTS ON PROCEDURE {fqn.database}.{fqn.schema}.{str(identifier)}")
    # ownership_grant = _filter_result(show_grants, privilege="OWNERSHIP")
    owner = _fetch_owner(session, "PROCEDURE", fqn)

    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "args": _parse_signature(properties["signature"]),
        "comment": data["description"],
        "execute_as": properties["execute as"],
        "external_access_integrations": data["external_access_integrations"] or None,
        "handler": properties["handler"],
        "imports": _parse_list_property(properties["imports"]),
        "language": properties["language"],
        "null_handling": properties["null handling"],
        "owner": owner,
        "packages": _parse_packages(properties["packages"]),
        "returns": returns,
        "runtime_version": properties["runtime_version"],
        "secure": data["is_secure"] == "Y",
        "as_": properties["body"],
    }


def fetch_role(session, fqn: FQN):
    roles = _show_resources(session, "ROLES", fqn)

    if len(roles) == 0:
        return None
    if len(roles) > 1:
        raise Exception(f"Found multiple roles matching {fqn}")

    data = roles[0]

    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "comment": data["comment"] or None,
        "owner": _get_owner_identifier(data),
    }


def fetch_role_grant(session, fqn: FQN):
    subject, name = fqn.params.copy().popitem()
    subject = ResourceName(subject)
    name = ResourceName(name)
    try:
        show_result = execute(session, f"SHOW GRANTS OF ROLE {fqn.name}", cacheable=True)
    except ProgrammingError as err:
        if err.errno == DOES_NOT_EXIST_ERR:
            return None
        raise

    if len(show_result) == 0:
        return None

    for data in show_result:
        if (
            resource_name_from_snowflake_metadata(data["granted_to"]) == subject
            and resource_name_from_snowflake_metadata(data["grantee_name"]) == name
        ):
            if data["granted_to"] == "ROLE":
                return {
                    "role": fqn.name,
                    "to_role": _quote_snowflake_identifier(data["grantee_name"]),
                    # "owner": data["granted_by"],
                }
            elif data["granted_to"] == "USER":
                return {
                    "role": fqn.name,
                    "to_user": _quote_snowflake_identifier(data["grantee_name"]),
                    # "owner": data["granted_by"],
                }
            else:
                raise Exception(f"Unexpected role grant for role {fqn.name}")

    return None


def fetch_schema(session, fqn: FQN):
    if fqn.database is None:
        raise Exception(f"Schema {fqn} is missing a database name")
    try:
        # show_result = execute(session, f"SHOW SCHEMAS LIKE '{fqn.name}' IN DATABASE {fqn.database}")
        show_result = _show_resources(session, "SCHEMAS", fqn)
    except ProgrammingError:
        return None

    if len(show_result) == 0:
        return None
    if len(show_result) > 1:
        raise Exception(f"Found multiple schemas matching {fqn}")

    data = show_result[0]

    options = options_result_to_list(data["options"])
    params = _show_resource_parameters(session, "SCHEMA", fqn)

    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "transient": "TRANSIENT" in options,
        "owner": _get_owner_identifier(data),
        "managed_access": "MANAGED ACCESS" in options,
        "data_retention_time_in_days": int(data["retention_time"]),
        "max_data_extension_time_in_days": params.get("max_data_extension_time_in_days"),
        "default_ddl_collation": params["default_ddl_collation"],
        "comment": data["comment"] or None,
    }


def fetch_secret(session, fqn: FQN):
    show_result = _show_resources(session, "SECRETS", fqn)
    if len(show_result) == 0:
        return None
    if len(show_result) > 1:
        raise Exception(f"Found multiple secrets matching {fqn}")
    data = show_result[0]
    desc_result = execute(session, f"DESC SECRET {fqn}")
    properties = desc_result[0]
    if data["secret_type"] == "PASSWORD":
        return {
            "name": _quote_snowflake_identifier(data["name"]),
            "secret_type": data["secret_type"],
            "username": properties["username"],
            "comment": data["comment"] or None,
            "owner": _get_owner_identifier(data),
        }
    elif data["secret_type"] == "GENERIC_STRING":
        return {
            "name": _quote_snowflake_identifier(data["name"]),
            "secret_type": data["secret_type"],
            "comment": data["comment"] or None,
            "owner": _get_owner_identifier(data),
        }
    elif data["secret_type"] == "OAUTH2":
        return {
            "name": _quote_snowflake_identifier(data["name"]),
            "api_authentication": properties["integration_name"],
            "secret_type": data["secret_type"],
            "oauth_scopes": data["oauth_scopes"],
            "oauth_refresh_token_expiry_time": _convert_to_gmt(properties["oauth_refresh_token_expiry_time"]),
            "comment": data["comment"] or None,
            "owner": _get_owner_identifier(data),
        }
    else:
        raise NotImplementedError(f"Unsupported secret type {data['secret_type']}")


def fetch_security_integration(session, fqn: FQN):
    show_result = execute(session, "SHOW SECURITY INTEGRATIONS", cacheable=True)

    show_result = _filter_result(show_result, name=fqn.name)

    if len(show_result) == 0:
        return None
    if len(show_result) > 1:
        raise Exception(f"Found multiple security integrations matching {fqn}")

    data = show_result[0]
    desc_result = execute(session, f"DESC SECURITY INTEGRATION {fqn.name}")
    properties = _desc_type2_result_to_dict(desc_result, lower_properties=True)
    owner = _fetch_owner(session, "INTEGRATION", fqn)

    if data["type"] == "API_AUTHENTICATION":
        return {
            "name": _quote_snowflake_identifier(data["name"]),
            "type": data["type"],
            "auth_type": properties["auth_type"],
            "enabled": data["enabled"] == "true",
            "oauth_token_endpoint": properties["oauth_token_endpoint"],
            "oauth_client_auth_method": properties["oauth_client_auth_method"],
            "oauth_client_id": properties["oauth_client_id"],
            "oauth_grant": properties["oauth_grant"],
            "oauth_access_token_validity": properties["oauth_access_token_validity"],
            "oauth_allowed_scopes": properties["oauth_allowed_scopes"],
            "comment": data["comment"] or None,
            "owner": owner,
        }

    elif data["type"].startswith("OAUTH"):
        type_, oauth_client = data["type"].split(" - ")
        if oauth_client == "SNOWSERVICES_INGRESS":
            return {
                "name": _quote_snowflake_identifier(data["name"]),
                "type": type_,
                "oauth_client": oauth_client,
                "enabled": data["enabled"] == "true",
                "owner": owner,
            }
    raise Exception(f"Unsupported security integration type {data['type']}")

    # return {
    #     "name": _quote_snowflake_identifier(data["name"]),
    #     "type": type_,
    #     "enabled": data["enabled"] == "true",
    #     "oauth_client": oauth_client,
    #     # "oauth_client_secret": None,
    #     # "oauth_redirect_uri": None,
    #     "oauth_issue_refresh_tokens": properties["oauth_issue_refresh_tokens"] == "true",
    #     "oauth_refresh_token_validity": properties["oauth_refresh_token_validity"],
    #     "comment": data["comment"] or None,
    # }


def fetch_sequence(session, fqn: FQN):
    show_result = execute(session, f"SHOW SEQUENCES LIKE '{fqn.name}' IN SCHEMA {fqn.database}.{fqn.schema}")
    if len(show_result) == 0:
        return None
    if len(show_result) > 1:
        raise Exception(f"Found multiple sequences matching {fqn}")

    data = show_result[0]

    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "owner": _get_owner_identifier(data),
        "start": data["next_value"],
        "increment": data["interval"],
        "comment": data["comment"] or None,
    }


def fetch_service(session, fqn: FQN):
    show_result = execute(
        session, f"SHOW SERVICES LIKE '{fqn.name}' IN SCHEMA {fqn.database}.{fqn.schema}", cacheable=True
    )

    if len(show_result) == 0:
        return None
    if len(show_result) > 1:
        raise Exception(f"Found multiple services matching {fqn}")

    data = show_result[0]

    return {
        "name": fqn.name,
        "compute_pool": data["compute_pool"],
        "external_access_integrations": None,
        "auto_resume": data["auto_resume"] == "true",
        "min_instances": data["min_instances"],
        "max_instances": data["max_instances"],
        "query_warehouse": data["query_warehouse"],
        "comment": data["comment"] or None,
        "owner": _get_owner_identifier(data),
    }


def fetch_share(session, fqn: FQN):
    show_result = execute(session, f"SHOW SHARES LIKE '{fqn.name}'")
    shares = _filter_result(show_result, kind="OUTBOUND")

    if len(shares) == 0:
        return None
    if len(shares) > 1:
        raise Exception(f"Found multiple shares matching {fqn}")

    data = shares[0]
    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "owner": _get_owner_identifier(data),
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
        "name": _quote_snowflake_identifier(data["name"]),
        "from_share": data["origin"],
        "owner": _get_owner_identifier(data),
    }


def fetch_stage(session, fqn: FQN):
    show_result = _show_resources(session, "STAGES", fqn)
    stages = _filter_result(show_result, name=fqn.name)

    if len(stages) == 0:
        return None
    if len(stages) > 1:
        raise Exception(f"Found multiple stages matching {fqn}")

    data = stages[0]
    # desc_result = execute(session, f"DESC STAGE {fqn}")
    # properties = _desc_type3_result_to_dict(desc_result, lower_properties=True)

    if data["type"] == "EXTERNAL":
        return {
            "name": _quote_snowflake_identifier(data["name"]),
            "url": data["url"],
            "owner": _get_owner_identifier(data),
            "type": data["type"],
            "storage_integration": data["storage_integration"],
            "directory": {"enable": data["directory_enabled"] == "Y"},
            "comment": data["comment"] or None,
        }
    elif data["type"] in ("INTERNAL", "INTERNAL NO CSE"):
        return {
            "name": _quote_snowflake_identifier(data["name"]),
            "owner": _get_owner_identifier(data),
            "type": "INTERNAL",
            "directory": {"enable": data["directory_enabled"] == "Y"},
            "comment": data["comment"] or None,
        }
    else:
        raise Exception(f"Unsupported stage type {data['type']}")


def fetch_storage_integration(session, fqn: FQN):
    show_result = execute(session, "SHOW INTEGRATIONS")
    integrations = _filter_result(show_result, name=fqn.name, category="STORAGE")

    if len(integrations) == 0:
        return None
    if len(integrations) > 1:
        raise Exception(f"Found multiple storage integrations matching {fqn}")

    data = integrations[0]

    desc_result = execute(session, f"DESC INTEGRATION {fqn.name}")
    properties = _desc_type2_result_to_dict(desc_result, lower_properties=True)

    owner = _fetch_owner(session, "INTEGRATION", fqn)

    if properties["storage_provider"] == "S3":
        return {
            "name": _quote_snowflake_identifier(data["name"]),
            "type": data["type"],
            "enabled": data["enabled"] == "true",
            "comment": data["comment"] or None,
            "owner": owner,
            "storage_provider": properties["storage_provider"],
            "storage_aws_role_arn": properties.get("storage_aws_role_arn"),
            "storage_allowed_locations": properties.get("storage_allowed_locations") or None,
            "storage_blocked_locations": properties.get("storage_blocked_locations") or None,
            "storage_aws_object_acl": properties.get("storage_aws_object_acl"),
        }
    elif properties["storage_provider"] == "GCS":
        return {
            "name": _quote_snowflake_identifier(data["name"]),
            "type": data["type"],
            "enabled": data["enabled"] == "true",
            "comment": data["comment"] or None,
            "owner": owner,
            "storage_provider": properties["storage_provider"],
            "storage_allowed_locations": properties.get("storage_allowed_locations") or None,
            "storage_blocked_locations": properties.get("storage_blocked_locations") or None,
        }
    elif properties["storage_provider"] == "AZURE":
        return {
            "name": _quote_snowflake_identifier(data["name"]),
            "type": data["type"],
            "enabled": data["enabled"] == "true",
            "comment": data["comment"] or None,
            "owner": owner,
            "storage_provider": properties["storage_provider"],
            "storage_allowed_locations": properties.get("storage_allowed_locations") or None,
            "azure_tenant_id": properties["azure_tenant_id"],
        }
    else:
        raise Exception(f"Unsupported storage provider {properties['storage_provider']}")


def fetch_stream(session, fqn: FQN):
    show_result = execute(session, "SHOW STREAMS IN ACCOUNT", cacheable=True)

    streams = _filter_result(show_result, name=fqn.name, database_name=fqn.database, schema_name=fqn.schema)

    if len(streams) == 0:
        return None
    if len(streams) > 1:
        raise Exception(f"Found multiple streams matching {fqn}")

    data = streams[0]
    if data["source_type"] == "Table":
        return {
            "name": _quote_snowflake_identifier(data["name"]),
            "comment": data["comment"] or None,
            "append_only": data["mode"] == "APPEND_ONLY",
            "on_table": data["table_name"],
            "owner": _get_owner_identifier(data),
        }
    elif data["source_type"] == "View":
        return {
            "name": _quote_snowflake_identifier(data["name"]),
            "comment": data["comment"] or None,
            "append_only": data["mode"] == "APPEND_ONLY",
            "on_view": data["table_name"],
            "owner": _get_owner_identifier(data),
        }
    elif data["source_type"] == "Stage":
        return {
            "name": _quote_snowflake_identifier(data["name"]),
            "on_stage": data["table_name"],
            "owner": _get_owner_identifier(data),
            "comment": data["comment"] or None,
        }
    else:
        raise NotImplementedError(f"Unsupported stream source type {data['source_type']}")


def fetch_tag(session, fqn: FQN):
    try:
        show_result = execute(session, "SHOW TAGS IN ACCOUNT", cacheable=True)
    except ProgrammingError as err:
        if err.errno == UNSUPPORTED_FEATURE:
            return None
        raise
    tags = _filter_result(show_result, name=fqn.name, database_name=fqn.database, schema_name=fqn.schema)
    if len(tags) == 0:
        return None
    if len(tags) > 1:
        raise Exception(f"Found multiple tags matching {fqn}")
    data = tags[0]
    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "owner": _get_owner_identifier(data),
        "comment": data["comment"] or None,
        "allowed_values": json.loads(data["allowed_values"]) if data["allowed_values"] else None,
    }


def fetch_task(session, fqn: FQN):
    show_result = execute(session, f"SHOW TASKS IN SCHEMA {fqn.database}.{fqn.schema}", cacheable=True)

    if len(show_result) == 0:
        return None
    if len(show_result) > 1:
        raise Exception(f"Found multiple tasks matching {fqn}")

    data = show_result[0]
    task_details_result = execute(session, f"DESC TASK {fqn.database}.{fqn.schema}.{fqn.name}", cacheable=True)
    if len(task_details_result) == 0:
        raise Exception(f"Failed to fetch task details for {fqn}")
    task_details = task_details_result[0]
    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "warehouse": data["warehouse"],
        "schedule": data["schedule"],
        "state": str(data["state"]).upper(),
        "owner": _get_owner_identifier(data),
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
        "name": _quote_snowflake_identifier(data["name"]),
        "object_types": data["object_types"].split(","),
        "allowed_integration_types": (
            None if data["allowed_integration_types"] == "" else data["allowed_integration_types"].split(",")
        ),
        "allowed_accounts": None if data["allowed_accounts"] == "" else data["allowed_accounts"].split(","),
        "allowed_databases": databases,
        "replication_schedule": data["replication_schedule"],
        "owner": _get_owner_identifier(data),
    }


def fetch_resource_monitor(session, fqn: FQN):
    show_result = execute(session, f"SHOW RESOURCE MONITORS LIKE '{fqn.name}'")
    resource_monitors = _filter_result(show_result)
    if len(resource_monitors) == 0:
        return None
    if len(resource_monitors) > 1:
        raise Exception(f"Found multiple resource monitors matching {fqn}")
    data = resource_monitors[0]
    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "owner": _get_owner_identifier(data),
        "credit_quota": int(float(data["credit_quota"])) if data["credit_quota"] else None,
        "frequency": data["frequency"],
        "start_timestamp": _convert_to_gmt(data["start_time"], "%Y-%m-%d %H:%M"),
        "end_timestamp": _convert_to_gmt(data["end_time"], "%Y-%m-%d %H:%M"),
        "notify_users": data["notify_users"] or None,
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

    database = f"{fqn.database}." if fqn.database else ""

    tag_refs = execute(
        session,
        f"""
            SELECT *
            FROM table({database}information_schema.tag_references(
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


def fetch_table(session, fqn: FQN):
    show_result = execute(session, "SHOW TABLES IN ACCOUNT", cacheable=True)

    tables = _filter_result(
        show_result,
        name=fqn.name,
        database_name=fqn.database,
        schema_name=fqn.schema,
    )

    if len(tables) == 0:
        return None
    if len(tables) > 1:
        raise Exception(f"Found multiple tables matching {fqn}")

    columns = fetch_columns(session, "TABLE", fqn)

    data = tables[0]
    show_params_result = execute(session, f"SHOW PARAMETERS FOR TABLE {fqn}")
    params = params_result_to_dict(show_params_result)

    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "columns": columns,
        "cluster_by": _parse_cluster_keys(data["cluster_by"]),
        "transient": data["kind"] == "TRANSIENT",
        "owner": _get_owner_identifier(data),
        "comment": data["comment"] or None,
        "enable_schema_evolution": data["enable_schema_evolution"] == "Y",
        # "data_retention_time_in_days": int(data["retention_time"]),
        # "max_data_extension_time_in_days": params.get("max_data_extension_time_in_days", None),
        "default_ddl_collation": params.get("default_ddl_collation", None),
        "change_tracking": data["change_tracking"] == "ON",
    }


def fetch_tag_reference(session, fqn: FQN):
    session_ctx = fetch_session(session)
    if session_ctx["tag_support"] is False:
        return None

    object_domain = fqn.params["domain"]
    # TODO: this is a hacky fix
    name = str(fqn).split("?")[0]
    resource_fqn = parse_FQN(name, is_db_scoped=(object_domain == "SCHEMA"))

    tag_db = resource_fqn.database if resource_fqn.database else resource_fqn

    # Another hacky fix
    if str(resource_fqn) == "DATABASE":
        resource_fqn = '"DATABASE"'

    try:
        tag_refs = execute(
            session,
            f"""
                SELECT *
                FROM table({tag_db}.information_schema.tag_references(
                    '{resource_fqn}', '{object_domain}'
                ))""",
        )
    except ProgrammingError as err:
        if err.errno == INVALID_IDENTIFIER:
            return None
        raise

    if len(tag_refs) == 0:
        return None

    tag_map = {}
    for tag_ref in tag_refs:
        tag_name = f"{tag_ref['TAG_DATABASE']}.{tag_ref['TAG_SCHEMA']}.{tag_ref['TAG_NAME']}"
        tag_map[tag_name] = tag_ref["TAG_VALUE"]
    return {
        "object_name": name,
        "object_domain": object_domain,
        "tags": tag_map,
    }


def fetch_user(session, fqn: FQN) -> Optional[dict]:
    # SHOW USERS requires the MANAGE GRANTS privilege
    # Other roles can see the list of users but don't get access to other metadata such as login_name.
    # This causes incorrect drift
    users = _show_resources(session, "USERS", fqn)

    if len(users) == 0:
        return None
    if len(users) > 1:
        raise Exception(f"Found multiple users matching {fqn}")

    data = users[0]
    desc_result = execute(session, f"DESC USER {fqn}")
    properties = _desc_result_to_dict(desc_result, lower_properties=True)

    user_type = properties["type"].upper()

    display_name = None
    login_name = None
    must_change_password = None
    if user_type != "SERVICE":
        display_name = data["display_name"]
        login_name = data["login_name"]
        must_change_password = data["must_change_password"] == "true"

    rsa_public_key = properties["rsa_public_key"] if properties["rsa_public_key"] != "null" else None

    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "login_name": login_name,
        "display_name": display_name,
        "first_name": data["first_name"] or None,
        "last_name": data["last_name"] or None,
        "email": data["email"] or None,
        "mins_to_unlock": data["mins_to_unlock"] or None,
        "days_to_expiry": data["days_to_expiry"] or None,
        "comment": data["comment"] or None,
        "disabled": data["disabled"] == "true",
        "must_change_password": must_change_password,
        "default_warehouse": data["default_warehouse"] or None,
        "default_namespace": data["default_namespace"] or None,
        "default_role": data["default_role"] or None,
        "default_secondary_roles": data["default_secondary_roles"] or None,
        "mins_to_bypass_mfa": data["mins_to_bypass_mfa"] or None,
        "type": user_type,
        "rsa_public_key": rsa_public_key,
        "owner": _get_owner_identifier(data),
    }


def fetch_view(session, fqn: FQN):
    if fqn.schema is None:
        raise Exception(f"View fqn must have a schema {fqn}")
    try:
        views = _show_resources(session, "VIEWS", fqn)
    except ProgrammingError:
        return None

    if len(views) == 0:
        return None
    if len(views) > 1:
        raise Exception(f"Found multiple views matching {fqn}")

    data = views[0]

    if data["is_materialized"] == "true":
        return None

    columns = fetch_columns(session, "VIEW", fqn)

    return {
        "name": _quote_snowflake_identifier(data["name"]),
        "owner": _get_owner_identifier(data),
        "secure": data["is_secure"] == "true",
        "columns": columns,
        "change_tracking": data["change_tracking"] == "ON",
        "comment": data["comment"] or None,
        "as_": parse_view_ddl(data["text"]),
    }


def fetch_warehouse(session, fqn: FQN):
    try:
        show_result = _show_resources(session, "WAREHOUSES", fqn)
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
        "name": _quote_snowflake_identifier(data["name"]),
        "owner": _get_owner_identifier(data),
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

######## List helpers


def list_resource(session, resource_label: str) -> list[FQN]:
    return getattr(__this__, f"list_{pluralize(resource_label)}")(session)


def list_account_scoped_resource(session, resource) -> list[FQN]:
    show_result = execute(session, f"SHOW {resource}")
    resources = []
    for row in show_result:
        resources.append(FQN(name=resource_name_from_snowflake_metadata(row["name"])))
    return resources


def list_schema_scoped_resource(session, resource) -> list[FQN]:
    show_result = execute(session, f"SHOW {resource} IN ACCOUNT")
    resources = []
    for row in show_result:
        resources.append(
            FQN(
                database=resource_name_from_snowflake_metadata(row["database_name"]),
                schema=resource_name_from_snowflake_metadata(row["schema_name"]),
                name=resource_name_from_snowflake_metadata(row["name"]),
            )
        )
    return resources


######## List functions by resource


def list_alerts(session) -> list[FQN]:
    return list_schema_scoped_resource(session, "ALERTS")


def list_api_integrations(session) -> list[FQN]:
    return list_account_scoped_resource(session, "API INTEGRATIONS")


def list_authentication_policies(session) -> list[FQN]:
    return list_schema_scoped_resource(session, "AUTHENTICATION POLICIES")


def list_catalog_integrations(session) -> list[FQN]:
    return list_account_scoped_resource(session, "CATALOG INTEGRATIONS")


def list_compute_pools(session) -> list[FQN]:
    try:
        show_result = execute(session, "SHOW COMPUTE POOLS")
    except ProgrammingError as err:
        logger.warning(f"Error listing compute pools: {err}")
        return []
    return [FQN(name=resource_name_from_snowflake_metadata(row["name"])) for row in show_result]


def _list_databases(session) -> list[ResourceName]:
    show_result = execute(session, "SHOW DATABASES", cacheable=True)
    databases = []
    for row in show_result:
        # Exclude system databases like SNOWFLAKE
        if row["name"] in SYSTEM_DATABASES:
            continue
        # Exclude database shares
        if row["kind"] != "STANDARD":
            continue
        databases.append(resource_name_from_snowflake_metadata(row["name"]))
    return databases


def list_databases(session) -> list[FQN]:
    databases = _list_databases(session)
    return [FQN(name=database) for database in databases]


def list_database_roles(session) -> list[FQN]:
    databases = _list_databases(session)
    roles = []
    for database_name in databases:
        try:
            # A rare case where we need to always quote the identifier. Snowflake chokes if the database name
            # is DATABASE, but this will work if quoted
            if database_name == "DATABASE":
                database_name._quoted = True
            database_roles = execute(session, f"SHOW DATABASE ROLES IN DATABASE {database_name}")
        except ProgrammingError as err:
            if err.errno == DOES_NOT_EXIST_ERR:
                continue
            raise
        for role in database_roles:
            roles.append(
                FQN(
                    name=resource_name_from_snowflake_metadata(role["name"]),
                    database=database_name,
                )
            )
    return roles


def list_dynamic_tables(session) -> list[FQN]:
    return list_schema_scoped_resource(session, "DYNAMIC TABLES")


def list_external_volumes(session) -> list[FQN]:
    return list_account_scoped_resource(session, "EXTERNAL VOLUMES")


# def list_future_grants(session) -> list[FQN]:
#     grants = []
#     for role in roles:
#         role_name = resource_name_from_snowflake_metadata(role["name"])
#         if role_name in SYSTEM_ROLES:
#             continue
#         show_result = execute(session, f"SHOW GRANTS OF ROLE {role_name}")
#         for data in show_result:
#             subject = "user" if data["granted_to"] == "USER" else "role"
#             grants.append(FQN(name=role_name, params={subject: data["grantee_name"]}))
#     return grants


def list_functions(session) -> list[FQN]:
    show_result = execute(session, "SHOW USER FUNCTIONS IN ACCOUNT")
    functions = []
    for row in show_result:
        if row["catalog_name"] in SYSTEM_DATABASES:
            continue
        fqn, returns = _parse_function_arguments(row["arguments"])
        fqn.database = row["catalog_name"]
        fqn.schema = row["schema_name"]
        functions.append(fqn)
    return functions


def list_grants(session) -> list[FQN]:
    roles = execute(session, "SHOW ROLES", cacheable=True)
    grants = []
    for role in roles:
        role_name = resource_name_from_snowflake_metadata(role["name"])
        if role_name in SYSTEM_ROLES:
            continue
        grant_data = _show_grants_to_role(session, role_name, cacheable=True)
        for data in grant_data:
            if data["granted_on"] == "ROLE":
                # raise Exception(f"Role grants are not supported yet: {data}")
                continue

            # Titan Grants don't support OWNERSHIP privilege
            if data["privilege"] == "OWNERSHIP":
                continue

            # Skip this undocumented new priv because it's unrevokable
            if data["privilege"] == "CREATE CORTEX SEARCH SERVICE":
                continue
            name = data["name"]
            if data["granted_on"] == "ACCOUNT":
                name = "ACCOUNT"
            on = f"{data['granted_on'].lower()}/{name}"
            grants.append(
                FQN(
                    name=role_name,
                    params={
                        "priv": data["privilege"],
                        "on": on,
                    },
                )
            )
    return grants


def list_iceberg_tables(session) -> list[FQN]:
    return list_schema_scoped_resource(session, "ICEBERG TABLES")


def list_image_repositories(session) -> list[FQN]:
    return list_schema_scoped_resource(session, "IMAGE REPOSITORIES")


def list_network_policies(session) -> list[FQN]:
    return list_account_scoped_resource(session, "NETWORK POLICIES")


def list_network_rules(session) -> list[FQN]:
    return list_schema_scoped_resource(session, "NETWORK RULES")


def list_pipes(session) -> list[FQN]:
    return list_schema_scoped_resource(session, "PIPES")


def list_resource_monitors(session) -> list[FQN]:
    return list_account_scoped_resource(session, "RESOURCE MONITORS")


def list_roles(session) -> list[FQN]:
    show_result = execute(session, "SHOW ROLES")
    return [
        FQN(name=resource_name_from_snowflake_metadata(row["name"]))
        for row in show_result
        if row["name"] not in SYSTEM_ROLES
    ]


def list_role_grants(session) -> list[FQN]:
    roles = execute(session, "SHOW ROLES")
    grants = []
    for role in roles:
        role_name = resource_name_from_snowflake_metadata(role["name"])
        if role_name in SYSTEM_ROLES:
            continue
        try:
            show_result = execute(session, f"SHOW GRANTS OF ROLE {role_name}")
        except ProgrammingError as err:
            if err.errno == DOES_NOT_EXIST_ERR:
                continue
            raise
        for data in show_result:
            subject = "user" if data["granted_to"] == "USER" else "role"
            grants.append(FQN(name=role_name, params={subject: data["grantee_name"]}))
    return grants


def list_schemas(session, database=None) -> list[FQN]:
    if database:
        db = f" IN DATABASE {database}"
        user_databases = None
    else:
        db = ""
        user_databases = _list_databases(session)
    try:
        show_result = execute(session, f"SHOW SCHEMAS{db}")
        schemas = []
        for row in show_result:
            # Skip system databases
            if row["database_name"] in SYSTEM_DATABASES:
                continue
            # Skip system schemas
            if row["name"] == "INFORMATION_SCHEMA":
                continue
            # Skip database shares
            if database is None and row["database_name"] not in user_databases:
                continue
            schemas.append(
                FQN(
                    database=resource_name_from_snowflake_metadata(row["database_name"]),
                    name=resource_name_from_snowflake_metadata(row["name"]),
                )
            )
        return schemas
    except ProgrammingError as err:
        if err.errno == OBJECT_DOES_NOT_EXIST_ERR:
            return []
        raise


def list_secrets(session) -> list[FQN]:
    return list_schema_scoped_resource(session, "SECRETS")


def list_security_integrations(session) -> list[FQN]:
    show_result = execute(session, "SHOW SECURITY INTEGRATIONS")
    integrations = []
    for row in show_result:
        if row["name"] in SYSTEM_SECURITY_INTEGRATIONS:
            continue
        integrations.append(FQN(name=resource_name_from_snowflake_metadata(row["name"])))
    return integrations


def list_shares(session) -> list[FQN]:
    show_result = execute(session, "SHOW SHARES")
    shares = []
    for row in show_result:
        if row["kind"] == "INBOUND":
            continue
        shares.append(FQN(name=resource_name_from_snowflake_metadata(row["name"])))
    return shares


def list_stages(session) -> list[FQN]:
    show_result = execute(session, "SHOW STAGES IN ACCOUNT")
    stages = []
    for row in show_result:
        if row["database_name"] in SYSTEM_DATABASES:
            continue
        if row["type"] not in ("EXTERNAL", "INTERNAL", "INTERNAL NO CSE"):
            continue
        stages.append(
            FQN(
                database=resource_name_from_snowflake_metadata(row["database_name"]),
                schema=resource_name_from_snowflake_metadata(row["schema_name"]),
                name=resource_name_from_snowflake_metadata(row["name"]),
            )
        )
    return stages


def list_storage_integrations(session) -> list[FQN]:
    return list_account_scoped_resource(session, "STORAGE INTEGRATIONS")


def list_streams(session) -> list[FQN]:
    return list_schema_scoped_resource(session, "STREAMS")


def list_tables(session) -> list[FQN]:
    show_result = execute(session, "SHOW TABLES IN ACCOUNT")
    user_databases = _list_databases(session)
    tables = []
    for row in show_result:
        if row["database_name"] in SYSTEM_DATABASES:
            continue
        if row["schema_name"] == "INFORMATION_SCHEMA":
            continue
        if row["database_name"] not in user_databases:
            continue
        if (
            row["is_external"] == "Y"
            or row["is_hybrid"] == "Y"
            or row["is_iceberg"] == "Y"
            or row["is_dynamic"] == "Y"
            or row["is_event"] == "Y"
        ):
            continue
        tables.append(
            FQN(
                database=resource_name_from_snowflake_metadata(row["database_name"]),
                schema=resource_name_from_snowflake_metadata(row["schema_name"]),
                name=resource_name_from_snowflake_metadata(row["name"]),
            )
        )
    return tables


def list_tag_references(session) -> list[FQN]:
    try:
        show_result = execute(session, "SHOW TAGS IN ACCOUNT")
        tag_references = []
        for tag in show_result:
            if tag["database_name"] in SYSTEM_DATABASES or tag["schema_name"] == "INFORMATION_SCHEMA":
                continue

            tag_refs = execute(
                session,
                f"""
                    SELECT *
                    FROM table(snowflake.account_usage.tag_references_with_lineage(
                        '{tag['database_name']}.{tag['schema_name']}.{tag['name']}'
                    ))
                """,
            )

            for ref in tag_refs:
                if ref["OBJECT_DELETED"] is not None:
                    continue
                print(ref)
            # raise
        return tag_references

        #     tags.append(
        #         FQN(
        #             database=resource_name_from_snowflake_metadata(row["database_name"]),
        #             schema=resource_name_from_snowflake_metadata(tag["schema_name"]),
        #             name=resource_name_from_snowflake_metadata(tag["name"]),
        #         )
        #     )
        # return tags
    except ProgrammingError as err:
        if err.errno == UNSUPPORTED_FEATURE:
            return []
        else:
            raise

    tag_map = {}
    for tag_ref in tag_refs:
        tag_name = f"{tag_ref['TAG_DATABASE']}.{tag_ref['TAG_SCHEMA']}.{tag_ref['TAG_NAME']}"
        tag_map[tag_name] = tag_ref["TAG_VALUE"]
    return tag_map


def list_tags(session) -> list[FQN]:
    try:
        show_result = execute(session, "SHOW TAGS IN ACCOUNT")
        tags = []
        for row in show_result:
            if row["database_name"] in SYSTEM_DATABASES or row["schema_name"] == "INFORMATION_SCHEMA":
                continue
            tags.append(
                FQN(
                    database=resource_name_from_snowflake_metadata(row["database_name"]),
                    schema=resource_name_from_snowflake_metadata(row["schema_name"]),
                    name=resource_name_from_snowflake_metadata(row["name"]),
                )
            )
        return tags
    except ProgrammingError as err:
        if err.errno == UNSUPPORTED_FEATURE:
            return []
        else:
            raise


def list_tasks(session) -> list[FQN]:
    return list_schema_scoped_resource(session, "TASKS")


def list_users(session) -> list[FQN]:
    show_result = execute(session, "SHOW USERS")
    users = []
    for row in show_result:
        if row["name"] in SYSTEM_USERS:
            continue
        users.append(FQN(name=resource_name_from_snowflake_metadata(row["name"])))
    return users


def list_views(session) -> list[FQN]:
    show_result = execute(session, "SHOW VIEWS IN ACCOUNT")
    views = []
    for row in show_result:
        if row["database_name"] in SYSTEM_DATABASES or row["schema_name"] == "INFORMATION_SCHEMA":
            continue
        if row["is_materialized"] == "true":
            continue
        views.append(
            FQN(
                database=resource_name_from_snowflake_metadata(row["database_name"]),
                schema=resource_name_from_snowflake_metadata(row["schema_name"]),
                name=resource_name_from_snowflake_metadata(row["name"]),
            )
        )
    return views


def list_warehouses(session) -> list[FQN]:
    show_result = execute(session, "SHOW WAREHOUSES")
    warehouses = []
    for row in show_result:
        if row["name"].startswith("SYSTEM$"):
            continue
        warehouses.append(FQN(name=resource_name_from_snowflake_metadata(row["name"])))
    return warehouses
