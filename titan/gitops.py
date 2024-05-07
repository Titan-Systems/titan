from inflection import pluralize

from .identifiers import resource_label_for_type
from .resource_name import ResourceName
from .resources.resource import ResourcePointer
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

    resource_cache = {}
    for resource in resources:
        if hasattr(resource._data, "name"):
            print("~~caching", resource.resource_type, resource.name)
            resource_cache[(resource.resource_type, resource.name)] = resource
    for resource in resources:
        for ref in resource.refs:
            cache_pointer = (ref.resource_type, ResourceName(ref.name))
            if isinstance(ref, ResourcePointer) and cache_pointer in resource_cache:
                print("~~resolving", ref.resource_type, ref.name, "to", resource_cache[cache_pointer]._container)
                ref._container = resource_cache[cache_pointer]._container

    return resources
