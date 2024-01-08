import sys

from .builder import tidy_sql
from .identifiers import URN
from .privs import GlobalPriv, Privs, WarehousePriv

lifecycle = sys.modules[__name__]


def create_resource(urn: URN, data: dict):
    return getattr(lifecycle, f"create_{urn.resource_type}", create__default)(urn, data)


def create__default(urn: URN, data: dict):
    return tidy_sql(
        "CREATE",
        urn.resource_type,
        urn.fqn,
        # cls.props.render(data),
    )


def update_resource(urn: URN, data: dict):
    return getattr(lifecycle, f"update_{urn.resource_type}", "update__default")(urn, data)


def update__default(urn: URN, data: dict):
    return tidy_sql(
        "ALTER",
        urn.resource_type,
        urn.fqn,
        # cls.props.render(data),
    )


def drop_resource(urn: URN, data: dict):
    return getattr(lifecycle, f"drop_{urn.resource_type}", "drop__default")(urn, data)


def drop__default(urn: URN, data: dict):
    return tidy_sql(
        "DROP",
        urn.resource_type,
        urn.fqn,
        # cls.props.render(data),
    )


CREATE_RESOURCE_PRIV_MAP = {
    "database": GlobalPriv.CREATE_DATABASE,
}


def create_resource_privs(urn: URN, data: dict):
    if urn.resource_type not in CREATE_RESOURCE_PRIV_MAP:
        # raise Exception(f"Unsupported resource: {urn}")
        return {}
    return {
        "account": str(CREATE_RESOURCE_PRIV_MAP[urn.resource_type]),
    }

    # if urn.resource_type == "DATABASE":
    #     return create_database_privs(urn, data)
