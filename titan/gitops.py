import logging
from typing import Any, Optional

from inflection import pluralize

from .blueprint_config import BlueprintConfig, set_vars_defaults
from .enums import ResourceType, RunMode
from .identifiers import resource_label_for_type
from .resource_name import ResourceName
from .resources import (
    Database,
    Resource,
    RoleGrant,
    Schema,
    User,
)
from .resources.resource import ResourcePointer
from .var import string_contains_var, process_for_each

logger = logging.getLogger("titan")

ALIASES = {
    "grants_on_all": ResourceType.GRANT_ON_ALL,
    "account_parameters": ResourceType.ACCOUNT_PARAMETER,
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
            if "owner" not in schema:
                schema["owner"] = db._data.owner
            sch = Schema(**schema)
            db.add(sch)
            resources.append(sch)
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


def _resources_for_config(config: dict, vars: dict):
    # Special cases
    database_config = config.pop("databases", [])
    role_grants = config.pop("role_grants", [])
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

            if isinstance(resource_data, dict):
                if "for_each" in resource_data:
                    resource_cls = Resource.resolve_resource_cls(resource_type, resource_data)
                    resource_instance = resource_data.copy()
                    for_each = resource_instance.pop("for_each")

                    if isinstance(for_each, str) and for_each.startswith("var."):
                        var_name = for_each.split(".")[1]
                        if var_name not in vars:
                            raise ValueError(f"Var {var_name} not found")
                        for_each_input = vars[var_name]
                    else:
                        raise ValueError(f"for_each must be a var reference. Got: {for_each}")

                    for each_value in for_each_input:
                        for key, value in resource_data.items():
                            if isinstance(value, str) and string_contains_var(value):
                                resource_instance[key] = process_for_each(value, each_value)

                        resource = resource_cls(**resource_instance)
                        resources.append(resource)
                else:
                    requires = resource_data.pop("requires", [])
                    resource_cls = Resource.resolve_resource_cls(resource_type, resource_data)
                    resource = resource_cls(**resource_data)
                    process_requires(resource, requires)
                    resources.append(resource)
            elif isinstance(resource_data, str):
                resource_cls = Resource.resolve_resource_cls(resource_type, {})
                resource = resource_cls.from_sql(resource_data)
                resources.append(resource)
            else:
                raise Exception(f"Unknown resource data type: {resource_data}")

    resources.extend(_resources_from_database_config(database_config))
    resources.extend(_resources_from_role_grants_config(role_grants))
    resources.extend(_resources_from_users_config(users))

    # This code helps resolve grant references to the fully qualified name of the resource.
    # This probably belongs in blueprint as a finalization step.
    resource_cache = {}
    for resource in resources:
        if hasattr(resource._data, "name"):
            resource_cache[(resource.resource_type, ResourceName(resource._data.name))] = resource

    for resource in resources:
        if resource.resource_type == ResourceType.GRANT:
            cache_pointer = (resource.on_type, ResourceName(resource.on))
            if cache_pointer in resource_cache:
                resource._data.on = ResourceName(str(resource_cache[cache_pointer].fqn))

    return resources


def collect_blueprint_config(yaml_config: dict, cli_config: Optional[dict[str, Any]] = None) -> BlueprintConfig:

    if cli_config is None:
        cli_config = {}

    config = yaml_config.copy()
    blueprint_args: dict[str, Any] = {}

    allowlist = config.pop("allowlist", None)
    if allowlist:
        if "allowlist" in cli_config:
            raise ValueError("Cannot specify both allowlist in yaml and cli")
        blueprint_args["allowlist"] = [ResourceType(resource_type) for resource_type in allowlist]
    elif "allowlist" in cli_config:
        blueprint_args["allowlist"] = [ResourceType(resource_type) for resource_type in cli_config["allowlist"]]

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

    vars_spec = config.pop("vars", [])
    if vars_spec:
        if not isinstance(vars_spec, list):
            raise ValueError("vars config entry must be a list of dicts")
        blueprint_args["vars_spec"] = vars_spec

    vars = {}
    if "vars" in cli_config:
        vars = cli_config["vars"]
        blueprint_args["vars"] = vars

    vars = set_vars_defaults(vars_spec, vars)

    resources = _resources_for_config(config, vars)
    if len(resources) == 0:
        raise ValueError("No resources found in config")

    blueprint_args["resources"] = resources

    if config:
        raise ValueError(f"Unknown keys in config: {config.keys()}")

    return BlueprintConfig(**blueprint_args)  # type: ignore[arg-type]
