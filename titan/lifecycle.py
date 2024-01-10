import sys

from .builder import tidy_sql
from .identifiers import URN
from .privs import GlobalPriv, DatabasePriv, SchemaPriv
from .props import render_props

__this__ = sys.modules[__name__]


def create_resource(urn: URN, data: dict):
    return getattr(__this__, f"create_{urn.resource_type}", create__default)(urn, data)


def create__default(urn: URN, data: dict):
    return tidy_sql(
        "CREATE",
        urn.resource_type,
        urn.fqn,
        render_props(urn, data),
    )


def update_resource(urn: URN, data: dict):
    return getattr(__this__, f"update_{urn.resource_type}", "update__default")(urn, data)


def update__default(urn: URN, data: dict):
    return tidy_sql(
        "ALTER",
        urn.resource_type,
        urn.fqn,
        # cls.props.render(data),
    )


def drop_resource(urn: URN):
    return getattr(__this__, f"drop_{urn.resource_type}", "drop__default")(urn)


def drop__default(urn: URN):
    return tidy_sql(
        "DROP",
        urn.resource_type,
        urn.fqn,
    )


CREATE_RESOURCE_PRIV_MAP = {
    "database": [GlobalPriv.CREATE_DATABASE],
    "schema": [DatabasePriv.CREATE_SCHEMA],
    "table": [SchemaPriv.CREATE_TABLE, SchemaPriv.USAGE, DatabasePriv.USAGE],
    "procedure": [SchemaPriv.CREATE_PROCEDURE, SchemaPriv.USAGE, DatabasePriv.USAGE],
}


def privs_for_create(urn: URN, data: dict):
    if urn.resource_type not in CREATE_RESOURCE_PRIV_MAP:
        raise Exception(f"Unsupported resource: {urn}")
        # return []
    return CREATE_RESOURCE_PRIV_MAP[urn.resource_type]
