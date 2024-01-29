import sys

from .builder import tidy_sql
from .identifiers import URN
from .props import Props

__this__ = sys.modules[__name__]


def create_resource(urn: URN, data: dict, props: Props, if_not_exists: bool = False) -> str:
    return getattr(__this__, f"create_{urn.resource_label}", create__default)(urn, data, props, if_not_exists)


def create__default(urn: URN, data: dict, props: Props, if_not_exists: bool = False) -> str:
    return tidy_sql(
        "CREATE",
        urn.resource_type,
        "IF NOT EXISTS" if if_not_exists else "",
        urn.fqn,
        props.render(data),
    )


def create_function(urn: URN, data: dict, props: Props, if_not_exists: bool = False) -> str:
    db = f"{urn.fqn.database}." if urn.fqn.database else ""
    schema = f"{urn.fqn.schema}." if urn.fqn.schema else ""
    name = f"{db}{schema}{data['name']}"
    return tidy_sql(
        "CREATE",
        "IF NOT EXISTS" if if_not_exists else "",
        urn.resource_type,
        name,
        props.render(data),
    )


def create_procedure(urn: URN, data: dict, props: Props, if_not_exists: bool = False) -> str:
    db = f"{urn.fqn.database}." if urn.fqn.database else ""
    schema = f"{urn.fqn.schema}." if urn.fqn.schema else ""
    name = f"{db}{schema}{urn.fqn.name}"
    return tidy_sql(
        "CREATE",
        "IF NOT EXISTS" if if_not_exists else "",
        urn.resource_type,
        name,
        props.render(data),
    )


def create_grant(urn: URN, data: dict, props: Props, if_not_exists: bool):
    return tidy_sql(
        "GRANT",
        data["priv"],
        "ON",
        data["on_type"],
        data["on"],
        props.render(data),
    )


def create_role_grant(urn: URN, data: dict, props: Props, if_not_exists: bool):
    return tidy_sql(
        "GRANT",
        props.render(data),
    )


def create_view(urn: URN, data: dict, props: Props, if_not_exists: bool = False) -> str:
    data = data.copy()
    secure = data.pop("secure", None)
    volatile = data.pop("volatile", None)
    recursive = data.pop("recursive", None)
    return tidy_sql(
        "CREATE",
        "SECURE" if secure else "",
        "VOLATILE" if volatile else "",
        "RECURSIVE" if recursive else "",
        urn.resource_type,
        "IF NOT EXISTS" if if_not_exists else "",
        urn.fqn,
        props.render(data),
    )


def update_resource(urn: URN, data: dict, props: Props) -> str:
    return getattr(__this__, f"update_{urn.resource_label}", update__default)(urn, data, props)


def update__default(urn: URN, data: dict, props: Props) -> str:
    attr, new_value = data.popitem()
    attr = attr.lower()
    if new_value is None:
        return tidy_sql("ALTER", urn.resource_type, urn.fqn, "UNSET", attr)
    elif attr == "name":
        return tidy_sql("ALTER", urn.resource_type, urn.fqn, "RENAME TO", new_value)
    elif attr == "owner":
        return tidy_sql("GRANT OWNERSHIP ON", urn.resource_type, urn.fqn, "TO ROLE", new_value)
    else:
        new_value = f"'{new_value}'" if isinstance(new_value, str) else new_value
        return tidy_sql(
            "ALTER",
            urn.resource_type,
            urn.fqn,
            "SET",
            attr,
            "=",
            new_value,
        )


def update_procedure(urn: URN, data: dict, props: Props) -> str:
    if "execute_as" in data:
        return tidy_sql(
            "ALTER",
            urn.resource_type,
            urn.fqn,
            "EXECUTE AS",
            data["execute_as"],
        )
    else:
        return update__default(urn, data, props)


def update_role_grant(urn: URN, data: dict, props: Props) -> str:
    raise NotImplementedError


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
    return getattr(__this__, f"drop_{urn.resource_label}", drop__default)(urn, data, if_exists=if_exists)


def drop__default(urn: URN, data: dict, if_exists: bool) -> str:
    return tidy_sql(
        "DROP",
        urn.resource_type,
        "IF EXISTS" if if_exists else "",
        urn.fqn,
    )


def drop_function(urn: URN, data: dict, if_exists: bool) -> str:
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
