import sys

from .builder import tidy_sql
from .identifiers import URN
from .privs import GlobalPriv, DatabasePriv, RolePriv, SchemaPriv
from .props import Props

__this__ = sys.modules[__name__]


def create_resource(urn: URN, data: dict, props: Props, if_not_exists: bool = False) -> str:
    return getattr(__this__, f"create_{urn.resource_type}", create__default)(urn, data, props, if_not_exists)


def create__default(urn: URN, data: dict, props: Props, if_not_exists: bool = False) -> str:
    return tidy_sql(
        "CREATE",
        "IF NOT EXISTS" if if_not_exists else "",
        urn.resource_type,
        urn.fqn,
        props.render(data),
    )


def create_grant(urn: URN, data: dict, props: Props, if_not_exists: bool):
    return tidy_sql(
        "GRANT",
        data["priv"],
        "ON",
        data["on"],
        props.render(data),
    )


def create_role_grant(urn: URN, data: dict, props: Props, if_not_exists: bool):
    return tidy_sql(
        "GRANT",
        props.render(data),
    )


def update_resource(urn: URN, data: dict, props: Props) -> str:
    return getattr(__this__, f"update_{urn.resource_type}", update__default)(urn, data, props)


def update__default(urn: URN, data: dict, props: Props) -> str:
    return tidy_sql(
        "ALTER",
        urn.resource_type,
        urn.fqn,
        "SET",
        props.render(data),
    )


def update_schema(urn: URN, data: dict, props: Props) -> str:
    attr, new_value = data.popitem()
    attr = attr.lower()
    if new_value is None:
        return tidy_sql("ALTER SCHEMA", urn.fqn, "UNSET", attr)
    elif attr == "name":
        return tidy_sql("ALTER SCHEMA", urn.fqn, "RENAME TO", new_value)
    elif attr == "owner":
        raise NotImplementedError
    elif attr == "transient":
        raise Exception("Cannot change transient property of schema")
    elif attr == "managed_access":
        return tidy_sql("ALTER SCHEMA", urn.fqn, "ENABLE" if new_value else "DISABLE", "MANAGED ACCESS")
    else:
        new_value = f"'{new_value}'" if isinstance(new_value, str) else new_value
        return tidy_sql("ALTER SCHEMA", urn.fqn, "SET", attr, "=", new_value)


def drop_resource(urn: URN, data: dict, if_exists: bool = False) -> str:
    return getattr(__this__, f"drop_{urn.resource_type}", drop__default)(urn, data, if_exists)


def drop__default(urn: URN, data: dict, if_exists: bool) -> str:
    return tidy_sql(
        "DROP",
        urn.resource_type,
        "IF EXISTS" if if_exists else "",
        urn.fqn,
    )


def drop_grant(urn: URN, data: dict, **kwargs):
    return tidy_sql(
        "REVOKE",
        data["priv"],
        "ON",
        data["on"],
        "FROM",
        data["to"],
        # "CASCADE" if cascade else "RESTRICT",
    )


def drop_role_grant(urn: URN, data: dict, **kwargs):
    return tidy_sql(
        "REVOKE ROLE",
        data["role"],
        "FROM",
        "ROLE" if data.get("to_role") else "USER",
        data["to_role"] if data.get("to_role") else data["to_user"],
    )


CREATE_RESOURCE_PRIV_MAP = {
    "database": [GlobalPriv.CREATE_DATABASE],
    "schema": [DatabasePriv.CREATE_SCHEMA],
    "table": [SchemaPriv.CREATE_TABLE, SchemaPriv.USAGE, DatabasePriv.USAGE],
    "procedure": [SchemaPriv.CREATE_PROCEDURE, SchemaPriv.USAGE, DatabasePriv.USAGE],
    "function": [SchemaPriv.CREATE_FUNCTION, SchemaPriv.USAGE, DatabasePriv.USAGE],
    "role_grant": [RolePriv.OWNERSHIP],
}


def privs_for_create(urn: URN, data: dict):
    if urn.resource_type not in CREATE_RESOURCE_PRIV_MAP:
        raise Exception(f"Unsupported resource: {urn}")
        # return []
    return CREATE_RESOURCE_PRIV_MAP[urn.resource_type]
