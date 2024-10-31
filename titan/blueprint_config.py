from dataclasses import dataclass, field
from typing import Optional

from .enums import BlueprintScope, ResourceType, RunMode
from .exceptions import InvalidResourceException, MissingVarException
from .resource_name import ResourceName
from .resources.resource import Resource

_VAR_TYPE_MAP = {
    "bool": bool,
    "boolean": bool,
    "float": float,
    "int": int,
    "integer": int,
    "str": str,
    "string": str,
    "list": list,
}


@dataclass  # (frozen=True)
class BlueprintConfig:
    name: Optional[str] = None
    resources: Optional[list[Resource]] = None
    run_mode: RunMode = RunMode.CREATE_OR_UPDATE
    dry_run: bool = False
    allowlist: Optional[list[ResourceType]] = None
    vars: dict = field(default_factory=dict)
    vars_spec: list[dict] = field(default_factory=list)
    scope: Optional[BlueprintScope] = None
    database: Optional[ResourceName] = None
    schema: Optional[ResourceName] = None

    def __post_init__(self):

        if self.dry_run is None:
            raise ValueError("dry_run must be provided")
        if self.run_mode is None:
            raise ValueError("run_mode must be provided")
        if self.vars is None:
            raise ValueError("vars must be provided")
        if self.vars_spec is None:
            raise ValueError("vars_spec must be provided")

        if not isinstance(self.run_mode, RunMode):
            raise ValueError(f"Invalid run_mode: {self.run_mode}")

        if self.scope is not None and not isinstance(self.scope, BlueprintScope):
            raise ValueError(f"Invalid scope: {self.scope}")

        if self.run_mode == RunMode.SYNC:
            """
            In sync mode, the remote state is not just the resources that were added to the blueprint,
            but all resources that exist in Snowflake. The allowlist is required to limit the scope of
            the sync to a specific set of resources.
            """
            if self.allowlist is None:
                raise ValueError("Sync mode must specify an allowlist")

        if self.allowlist is not None:
            if len(self.allowlist) == 0:
                raise ValueError("Allowlist must have at least one resource type")
            else:
                if self.resources:
                    for resource in self.resources:
                        if resource.resource_type not in self.allowlist:
                            raise InvalidResourceException(
                                f"Resource {resource.urn} of type {resource.resource_type} is not in the allowlist"
                            )

        if self.vars_spec:
            for var in self.vars_spec:
                if var.get("name") is None:
                    raise ValueError("All vars_spec entries must specify a name")
                if var.get("type") is None:
                    raise ValueError("All vars_spec entries must specify a type")
                elif var["type"] not in _VAR_TYPE_MAP:
                    raise ValueError(f"Vars must specify a valid type. Got: {var['type']}")

            # Create a set of all var names in vars_spec for efficient lookup
            spec_names = {var["name"] for var in self.vars_spec}

            # Check each var against its spec
            for var_name, var_value in self.vars.items():
                if var_name not in spec_names:
                    raise ValueError(f"Var '{var_name}' was provided without config")

                spec = next(s for s in self.vars_spec if s["name"] == var_name)
                if not isinstance(var_value, _VAR_TYPE_MAP[spec["type"]]):
                    raise TypeError(f"Var '{var_name}' should be of type {spec['type']}")

            # Check for missing vars and use defaults if available
            # TODO: this causes us to violate frozen=true
            self.vars = set_vars_defaults(self.vars_spec, self.vars)

        if self.scope == BlueprintScope.DATABASE and self.schema is not None:
            raise ValueError("Cannot specify a schema when using DATABASE scope")
        elif self.scope == BlueprintScope.ACCOUNT and (self.database is not None or self.schema is not None):
            raise ValueError(
                f"Cannot specify a database or schema when using ACCOUNT scope (database={repr(self.database)}, schema={repr(self.schema)})"
            )


def set_vars_defaults(vars_spec: list[dict], vars: dict) -> dict:
    new_vars = vars.copy()
    for spec in vars_spec:
        if spec["name"] not in new_vars:
            if "default" in spec:
                new_vars[spec["name"]] = spec["default"]
            else:
                raise MissingVarException(f"Required var '{spec['name']}' is missing and has no default value")
    return new_vars
