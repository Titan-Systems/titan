from inflection import pluralize

from .enums import ResourceType
from .identifiers import resource_label_for_type
from .resource_name import ResourceName
from .resources.resource import ResourcePointer
from .resources import (
    Database,
    Grant,
    RoleGrant,
    Schema,
    Resource,
    User,
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
            titan_grant = Grant(**grant)
        elif isinstance(grant, str):
            titan_grant = Grant.from_sql(grant)
        resources.append(titan_grant)
    return resources


def resources_from_users_config(users_config: list) -> list:
    resources = []
    for user in users_config:
        if isinstance(user, dict):
            roles = user.pop("roles", [])
            titan_user = User(**user)
            resources.append(titan_user)
            for role in roles:
                resources.append(
                    RoleGrant(
                        role=role,
                        to_user=titan_user,
                    )
                )
        elif isinstance(user, str):
            resources.append(User.from_sql(user))
    return resources


def process_requires(resource: Resource, requires: list):
    for req in requires:
        resource.requires(ResourcePointer(name=req["name"], resource_type=ResourceType(req["resource_type"])))


def collect_resources_from_config(config: dict):
    # TODO: ResourcePointers should get resolved to top-level resource configs when possible

    config = config.copy()

    database_config = config.pop("databases", [])
    role_grants = config.pop("role_grants", [])
    grants = config.pop("grants", [])
    users = config.pop("users", [])

    resources = []

    for resource_type in Resource.__types__.keys():
        resource_label = pluralize(resource_label_for_type(resource_type))
        for data in config.pop(resource_label, []):
            requires = data.pop("requires", [])
            resource_cls = Resource.resolve_resource_cls(resource_type, data)
            resource = resource_cls(**data)
            process_requires(resource, requires)
            resources.append(resource)

    if config:
        raise ValueError(f"Unknown keys in config: {config.keys()}")

    resources.extend(resources_from_database_config(database_config))
    resources.extend(resources_from_role_grants_config(role_grants))
    resources.extend(resources_from_grants_config(grants))
    resources.extend(resources_from_users_config(users))

    resource_cache = {}
    for resource in resources:
        if hasattr(resource._data, "name"):
            resource_cache[(resource.resource_type, ResourceName(resource._data.name))] = resource

    for resource in resources:
        if resource.resource_type == ResourceType.GRANT:
            cache_pointer = (resource.on_type, ResourceName(resource.on))
            if cache_pointer in resource_cache:
                resource._data.on = ResourceName(str(resource_cache[cache_pointer].fqn))

        for ref in resource.refs:
            cache_pointer = (ref.resource_type, ResourceName(ref.name))
            if (
                isinstance(ref, ResourcePointer)
                and cache_pointer in resource_cache
                and resource_cache[cache_pointer]._container is not None
            ):
                ref._container = resource_cache[cache_pointer]._container

    return resources
