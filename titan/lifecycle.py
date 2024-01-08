import sys

from .builder import tidy_sql
from .identifiers import URN

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
