import logging
from typing import Optional

from inflection import pluralize

from .blueprint_config import BlueprintConfig
from .enums import ResourceType, RunMode
from .identifiers import resource_label_for_type
from .resource_name import ResourceName
from .resources import (
    Database,
    Grant,
    Resource,
    RoleGrant,
    Schema,
    User,
)
from .resources.resource import ResourcePointer

logger = logging.getLogger("titan")

ALIASES = {
    "grants_on_all": ResourceType.GRANT_ON_ALL,
}


def _resources_from_role_grants_config(role_grants_config: list) -> list:
    if len(role_grants_config) == 0:
        return []
    resources = []
    for role_grant in role_grants_config:

        if "to_role" in role_grant:
            resources.append(
                RoleGrant(
                    role=role_grant["role"],
                    to_role=role_grant["to_role"],
                )
            )
        elif "to_user" in role_grant:
            resources.append(
                RoleGrant(
                    role=role_grant["role"],
                    to_user=role_grant["to_user"],
                )
            )
        else:
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
    if len(resources) == 0:
        raise ValueError(f"No role grants found in config: {role_grants_config}")
    return resources


def _resources_from_database_config(databases_config: list) -> list:
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


def _resources_from_grants_config(grants_config: list) -> list:
    resources = []
    for grant in grants_config:
        if isinstance(grant, dict):
            titan_grant = Grant(**grant)
        elif isinstance(grant, str):
            titan_grant = Grant.from_sql(grant)
        else:
            raise Exception(f"Unsupported grant found: {type(grant)}, {grant}")
        resources.append(titan_grant)
    return resources


def _resources_from_users_config(users_config: list) -> list:
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
    logger.warning("collect_resources_from_config is deprecated, use collect_blueprint_config instead")

    config = config.copy()
    resources = _resources_for_config(config)
    if config:
        raise ValueError(f"Unknown keys in config: {config.keys()}")
    return resources


def _resources_for_config(config: dict):
    # Special cases
    database_config = config.pop("databases", [])
    role_grants = config.pop("role_grants", [])
    grants = config.pop("grants", [])
    users = config.pop("users", [])

    resources = []
    config_blocks = []

    for resource_type in Resource.__types__.keys():
        resource_label = pluralize(resource_label_for_type(resource_type))
        block = config.pop(resource_label, [])
        if block:
            config_blocks.append((resource_type, block))

    for alias, resource_type in ALIASES.items():
        if alias in config:
            config_blocks.append((resource_type, config.pop(alias)))

    for resource_type, block in config_blocks:
        for resource_data in block:
            try:
                requires = resource_data.pop("requires", [])
                resource_cls = Resource.resolve_resource_cls(resource_type, resource_data)
                resource = resource_cls(**resource_data)
                process_requires(resource, requires)
                resources.append(resource)
            except Exception as e:
                print(f"Error processing resource: {resource_data}")
                raise e

    resources.extend(_resources_from_database_config(database_config))
    resources.extend(_resources_from_role_grants_config(role_grants))
    resources.extend(_resources_from_grants_config(grants))
    resources.extend(_resources_from_users_config(users))

    resource_cache = {}
    for resource in resources:
        if hasattr(resource._data, "name"):
            resource_cache[(resource.resource_type, ResourceName(resource._data.name))] = resource

    for resource in resources:
        if resource.resource_type == ResourceType.GRANT:
            cache_pointer = (resource.on_type, ResourceName(resource.on))
            if cache_pointer in resource_cache:
                resource._data.on = ResourceName(str(resource_cache[cache_pointer].fqn))

        # TODO: investigate this
        # for ref in resource.refs:
        #     cache_pointer = (ref.resource_type, ResourceName(ref.name))
        #     if (
        #         isinstance(ref, ResourcePointer)
        #         and cache_pointer in resource_cache
        #         and resource_cache[cache_pointer]._container is not None
        #     ):
        #         ref._container = resource_cache[cache_pointer]._container

    return resources


def collect_blueprint_config(yaml_config: dict, cli_config: Optional[dict] = None) -> BlueprintConfig:

    if cli_config is None:
        cli_config = {}

    config = yaml_config.copy()
    blueprint_args = {}

    allowlist = config.pop("allowlist", None)
    if allowlist:
        blueprint_args["allowlist"] = [ResourceType(resource_type) for resource_type in allowlist]

    dry_run = config.pop("dry_run", None)
    if dry_run:
        if "dry_run" in cli_config:
            raise ValueError("Cannot specify both dry_run in yaml and cli")
        blueprint_args["dry_run"] = dry_run
    elif "dry_run" in cli_config:
        blueprint_args["dry_run"] = cli_config["dry_run"]

    name = config.pop("name", None)
    if name:
        blueprint_args["name"] = name

    run_mode = config.pop("run_mode", None)
    if run_mode:
        if "run_mode" in cli_config:
            raise ValueError("Cannot specify both run_mode in yaml and cli")
        blueprint_args["run_mode"] = RunMode(run_mode)
    elif "run_mode" in cli_config:
        blueprint_args["run_mode"] = cli_config["run_mode"]

    vars_spec = config.pop("vars", None)
    if vars_spec:
        if not isinstance(vars_spec, list):
            raise ValueError("vars config entry must be a list of dicts")
        blueprint_args["vars_spec"] = vars_spec

    if "vars" in cli_config:
        blueprint_args["vars"] = cli_config["vars"]

    resources = _resources_for_config(config)
    if len(resources) == 0:
        raise ValueError("No resources found in config")

    blueprint_args["resources"] = resources

    if config:
        raise ValueError(f"Unknown keys in config: {config.keys()}")

    return BlueprintConfig(**blueprint_args)
