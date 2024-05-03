from inflection import pluralize

from .identifiers import resource_label_for_type
from .resources import (
    Database,
    Grant,
    RoleGrant,
    Schema,
    Resource,
)


def resources_from_role_grants_config(role_grants_config: list) -> list:
    resources = []
    for role_grant in role_grants_config:
        for user in role_grant.get("users", []):
            resources.append(
                RoleGrant(
                    role=role_grant["role"],
                    to_user=user,
                )
            )
        for to_role in role_grant.get("roles", []):
            resources.append(
                RoleGrant(
                    role=role_grant["role"],
                    to_role=to_role,
                )
            )
    return resources


def resources_from_database_config(databases_config: list) -> list:
    resources = []
    for database in databases_config:
        schemas = database.pop("schemas", [])
        db = Database(**database)
        resources.append(db)
        for schema in schemas:
            sch = Schema(**schema)
            db.add(sch)
            resources.append(sch)
    return resources


def resources_from_grants_config(grants_config: list) -> list:
    resources = []
    for grant in grants_config:
        if isinstance(grant, dict):
            resources.append(Grant(**grant))
        elif isinstance(grant, str):
            resources.append(Grant.from_sql(grant))
    return resources


def collect_resources_from_config(config: dict):
    # TODO: ResourcePointers should get resolved to top-level resource configs when possible

    config = config.copy()
    database_config = config.pop("databases", [])
    role_grants = config.pop("role_grants", [])
    grants = config.pop("grants", [])

    resources = []
    for resource_type in Resource.__types__.keys():
        resource_label = pluralize(resource_label_for_type(resource_type))
        for resource in config.pop(resource_label, []):
            resource_cls = Resource.resolve_resource_cls(resource_type, resource)
            resources.append(resource_cls(**resource))

    if config:
        raise ValueError(f"Unknown keys in config: {config.keys()}")

    resources.extend(resources_from_database_config(database_config))
    resources.extend(resources_from_role_grants_config(role_grants))
    resources.extend(resources_from_grants_config(grants))

    return resources
