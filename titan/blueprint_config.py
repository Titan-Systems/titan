from dataclasses import dataclass, field
from typing import Optional

from .enums import ResourceType, RunMode
from .exceptions import MissingVarException, InvalidResourceException
from .resources.resource import Resource


_VAR_TYPE_MAP = {
    "bool": bool,
    "boolean": bool,
    "float": float,
    "int": int,
    "integer": int,
    "str": str,
    "string": str,
}


@dataclass(frozen=True)
class BlueprintConfig:
    name: Optional[str] = None
    resources: Optional[list[Resource]] = None
    run_mode: RunMode = RunMode.CREATE_OR_UPDATE
    dry_run: bool = False
    allowlist: Optional[list[ResourceType]] = None
    vars: dict = field(default_factory=dict)
    vars_spec: list[dict] = field(default_factory=list)

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
            for spec in self.vars_spec:
                if spec["name"] not in self.vars:
                    if "default" in spec:
                        self.vars[spec["name"]] = spec["default"]
                    else:
                        raise MissingVarException(f"Required var '{spec['name']}' is missing and has no default value")
