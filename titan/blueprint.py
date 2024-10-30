import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from queue import Queue
from typing import Any, Generator, Iterable, Optional, Sequence, TypeVar, Union, cast

import snowflake.connector

from . import data_provider, lifecycle
from .blueprint_config import BlueprintConfig
from .client import (
    ALREADY_EXISTS_ERR,
    DOES_NOT_EXIST_ERR,
    INVALID_GRANT_ERR,
    execute,
    reset_cache,
)
from .data_provider import SessionContext
from .enums import AccountEdition, BlueprintScope, ResourceType, RunMode, resource_type_is_grant
from .exceptions import (
    DuplicateResourceException,
    InvalidResourceException,
    MissingPrivilegeException,
    MissingResourceException,
    NonConformingPlanException,
    OrphanResourceException,
)
from .identifiers import URN, parse_identifier, parse_URN, resource_label_for_type
from .privs import (
    CREATE_PRIV_FOR_RESOURCE_TYPE,
    system_role_for_priv,
)
from .resource_name import ResourceName
from .resource_tags import ResourceTags
from .resources import Database, RoleGrant, Schema
from .resources.database import public_schema_urn
from .resources.resource import (
    RESOURCE_SCOPES,
    NamedResource,
    Resource,
    ResourceContainer,
    ResourceLifecycleConfig,
    ResourcePointer,
    infer_role_type_from_name,
)
from .resources.role import Role
from .resources.tag import Tag, TaggableResource
from .scope import AccountScope, DatabaseScope, OrganizationScope, SchemaScope, TableScope

T = TypeVar("T")
ResourceRef = Union[tuple[ResourceType, str], str]


logger = logging.getLogger("titan")


@dataclass
class ResourceChange(ABC):
    urn: URN

    @abstractmethod
    def to_dict(self) -> dict[str, Any]:
        pass


ResourceOwner = ResourceName
ContainerDescriptor = tuple[URN, ResourceOwner]


@dataclass
class CreateResource(ResourceChange):
    resource_cls: type[Resource]
    container: Optional[ContainerDescriptor]
    after: dict[str, str]

    def to_dict(self) -> dict[str, Union[str, dict[str, str]]]:
        return {
            "action": "CREATE",
            "urn": str(self.urn),
            "resource_cls": self.resource_cls.__name__,
            "after": self.after,
        }


@dataclass
class DropResource(ResourceChange):
    before: dict[str, str]

    def to_dict(self) -> dict[str, Union[str, dict[str, str]]]:
        return {
            "action": "DROP",
            "urn": str(self.urn),
            "before": self.before,
        }


@dataclass
class UpdateResource(ResourceChange):
    resource_cls: type[Resource]
    before: dict[str, str]
    after: dict[str, str]
    delta: dict[str, str]

    def to_dict(self) -> dict[str, Union[str, dict[str, str]]]:
        return {
            "action": "UPDATE",
            "urn": str(self.urn),
            "resource_cls": self.resource_cls.__name__,
            "before": self.before,
            "after": self.after,
            "delta": self.delta,
        }


@dataclass
class TransferOwnership(ResourceChange):
    resource_cls: type[Resource]
    from_owner: str
    to_owner: str

    def to_dict(self) -> dict[str, str]:
        return {
            "action": "TRANSFER",
            "urn": str(self.urn),
            "resource_cls": self.resource_cls.__name__,
            "from_owner": self.from_owner,
            "to_owner": self.to_owner,
        }


State = dict[URN, dict]
Plan = list[ResourceChange]


def plan_from_dict(plan_dict: dict) -> Plan:
    changes: list[ResourceChange] = []
    for change in plan_dict:
        action = change["action"]
        if action == "CREATE":
            container_descriptor: ContainerDescriptor
            for urn, owner in change["container"].items():
                container_descriptor = (parse_URN(urn), ResourceName(owner))
            changes.append(
                CreateResource(
                    urn=parse_URN(change["urn"]),
                    resource_cls=Resource.__classes__[change["resource_cls"]],
                    container=container_descriptor,
                    after=change["after"],
                )
            )
        elif action == "DROP":
            changes.append(
                DropResource(
                    urn=parse_URN(change["urn"]),
                    before=change["before"],
                )
            )
        elif action == "UPDATE":
            changes.append(
                UpdateResource(
                    urn=parse_URN(change["urn"]),
                    resource_cls=Resource.__classes__[change["resource_cls"]],
                    before=change["before"],
                    after=change["after"],
                    delta=change["delta"],
                )
            )
        elif action == "TRANSFER":
            changes.append(
                TransferOwnership(
                    urn=parse_URN(change["urn"]),
                    resource_cls=Resource.__classes__[change["resource_cls"]],
                    from_owner=change["from_owner"],
                    to_owner=change["to_owner"],
                )
            )
        else:
            raise Exception(f"Unsupported action {action}")
    return changes


@dataclass
class ManifestResource:
    urn: URN
    resource_cls: type[Resource]
    data: dict[str, Any]
    implicit: bool
    lifecycle: ResourceLifecycleConfig


class Manifest:
    def __init__(self, account_locator: str = ""):
        self._account_locator = account_locator
        self._resources: dict[URN, Union[ManifestResource, ResourcePointer]] = {}
        self._refs: list[tuple[URN, URN]] = []

    def __getitem__(self, key: URN):
        if isinstance(key, URN):
            return self._resources[key]
        else:
            raise Exception("Manifest keys must be URNs")

    def __contains__(self, key: URN):
        if isinstance(key, URN):
            return key in self._resources
        else:
            raise Exception("Manifest keys must be URNs")

    def add(self, resource: Resource, account_edition: AccountEdition):

        urn = URN.from_resource(
            account_locator=self._account_locator,
            resource=resource,
        )

        if urn in self._resources:
            if not isinstance(resource, ResourcePointer):
                logger.warning(f"Duplicate resource {urn} with conflicting data, discarding {resource}")
            return
        if isinstance(resource, ResourcePointer):
            self._resources[urn] = resource
        else:
            self._resources[urn] = ManifestResource(
                urn,
                resource.__class__,
                resource.to_dict(account_edition),
                resource.implicit,
                resource.lifecycle,
            )
        for ref in resource.refs:
            ref_urn = URN.from_resource(account_locator=self._account_locator, resource=ref)
            self._refs.append((urn, ref_urn))

    def get(self, key: URN, default=None):
        if isinstance(key, URN):
            return self._resources.get(key, default)
        else:
            raise Exception("Manifest keys must be URNs")

    def items(self):
        return self._resources.items()

    def __repr__(self):
        contents = ""
        for urn, resource in self._resources.items():
            contents += f"[{urn}] =>\n"
            contents += f"  {resource}\n"
        return f"Manifest({len(self._resources)} resources)\n{contents}"

    @property
    def urns(self) -> list[URN]:
        return list(self._resources.keys())

    @property
    def refs(self):
        return self._refs

    @property
    def resources(self):
        return list(self._resources.values())


def dump_plan(plan: Plan, format: str = "json"):
    if format == "json":
        return json.dumps([change.to_dict() for change in plan], indent=2)
    elif format == "text":
        """
        account:ABC123

        » role.transformer will be created

        + role "urn::ABC123:role/transformer" {
            + name  = "transformer"
            + owner = "SYSADMIN"
            }

        + warehouse "urn::ABC123:warehouse/transforming" {
            + name           = "transforming"
            + owner          = "SYSADMIN"
            + warehouse_type = "STANDARD"
            + warehouse_size = "LARGE"
            + auto_suspend   = 60
            }

        + grant "urn::ABC123:grant/..." {
            + priv = "USAGE"
            + on   = warehouse "transforming"
            + to   = role "transformer
            }

        + grant "urn::ABC123:grant/..." {
            + priv = "OPERATE"
            + on   = warehouse "transforming"
            + to   = role "transformer
            }
        """
        output = ""

        def _render_value(value):
            if isinstance(value, str):
                return f'"{value}"'
            return str(value)

        # green_start = "\033[36m"
        # color_end = "\033[0m"

        create_count = len([change for change in plan if isinstance(change, CreateResource)])
        update_count = len([change for change in plan if isinstance(change, UpdateResource)])
        transfer_count = len([change for change in plan if isinstance(change, TransferOwnership)])
        drop_count = len([change for change in plan if isinstance(change, DropResource)])

        output += "\n» titan core\n"
        output += f"» Plan: {create_count} to create, {update_count} to update, {transfer_count} to transfer, {drop_count} to drop.\n\n"

        for change in plan:
            action_marker = ""
            items: Iterable[tuple[str, str]] = []
            if isinstance(change, CreateResource):
                action_marker = "+"
                items = change.after.items()
            elif isinstance(change, UpdateResource):
                action_marker = "~"
                items = change.delta.items()
            elif isinstance(change, DropResource):
                action_marker = "-"
                items = []
            elif isinstance(change, TransferOwnership):
                action_marker = "~"
                items = [("owner", change.to_owner)]

            output += f"{action_marker} {change.urn}"
            if not isinstance(change, DropResource):
                output += " {"
            output += "\n"

            key_lengths = [len(key) for key, _ in items]
            max_key_length = max(key_lengths) if len(key_lengths) > 0 else 0
            for key, value in items:
                if key.startswith("_"):
                    continue
                new_value = _render_value(value)
                before_value = ""
                if isinstance(change, UpdateResource) and key in change.before:
                    before_value = _render_value(change.before[key]) + " -> "
                elif isinstance(change, TransferOwnership):
                    before_value = _render_value(change.from_owner) + " -> "
                output += f"  {action_marker} {key:<{max_key_length}} = {before_value}{new_value}\n"
            if not isinstance(change, DropResource):
                output += "}\n"
            output += "\n"

        return output
    else:
        raise Exception(f"Unsupported format {format}")


def print_plan(plan: Plan):
    print(dump_plan(plan, format="text"))


def print_diffs(diffs):
    for action, target, deltas in diffs:
        print(f"[{action}]", target)
        for delta in deltas:
            print("\t", delta)


def _split_by_scope(
    resources: list[Resource],
) -> tuple[list[Resource], list[Resource], list[Resource], list[Resource]]:
    org_scoped: list[Resource] = []
    acct_scoped: list[Resource] = []
    db_scoped: list[Resource] = []
    schema_scoped: list[Resource] = []

    seen = set()

    def route(resource: Resource):
        """The sorting hat"""

        if id(resource) not in seen:
            if isinstance(resource.scope, OrganizationScope):
                org_scoped.append(resource)
            elif isinstance(resource.scope, AccountScope):
                acct_scoped.append(resource)
            elif isinstance(resource.scope, DatabaseScope):
                db_scoped.append(resource)
            elif isinstance(resource.scope, SchemaScope):
                schema_scoped.append(resource)
            else:
                raise Exception(f"Unsupported resource type {type(resource)}")

        seen.add(id(resource))
        if isinstance(resource, ResourceContainer):
            for item in resource.items():
                route(item)

    for resource in resources:
        root = resource
        while getattr(root, "container", None) is not None:
            root = getattr(root, "container")
        route(root)
    return org_scoped, acct_scoped, db_scoped, schema_scoped


def _walk(resource: Resource) -> Generator[Resource, None, None]:
    yield resource
    if isinstance(resource, ResourceContainer):
        for item in resource.items():
            yield from _walk(item)


def _raise_if_plan_would_drop_session_user(session_ctx: SessionContext, plan: Plan):
    for change in plan:
        if change.urn.resource_type == ResourceType.USER and isinstance(change, DropResource):
            if ResourceName(session_ctx["user"]) == ResourceName(change.urn.fqn.name):
                raise Exception("Plan would drop the current session user, which is not allowed")


def _merge_pointers(resources: Sequence[Resource]) -> list[Resource]:
    """
    It is expected in yaml-defined blueprints that all resources are defined with static strings, instead
    of using object references.

    """

    namespace: dict[ResourceRef, Resource] = {}
    # Push pointers to the end
    resources = sorted(resources, key=lambda resource: isinstance(resource, ResourcePointer))

    def _merge(resource: ResourceContainer, pointer: ResourcePointer):
        if pointer.container is not None:
            # # The pointer has a container but the resource does not, merge fails
            # if getattr(resource, "container", None) is None:
            #     raise Exception(f"Cannot merge pointer {pointer} into resource {resource}")
            pointer.container.remove(pointer)

        # Migrate items from pointer to resource
        for item in pointer.items():
            pointer.remove(item)
            resource.add(item)

    for resource_or_pointer in resources:
        # Create a unique identifier for the resource
        resource_id: ResourceRef
        if isinstance(resource_or_pointer, NamedResource):
            resource_id = (resource_or_pointer.resource_type, resource_or_pointer.name)
        else:
            resource_id = str(resource_or_pointer.urn)

        # If the resource is a pointer, attempt to merge it to an existing resource
        if isinstance(resource_or_pointer, ResourcePointer):
            pointer = resource_or_pointer
            if resource_id in namespace:
                primary = cast(ResourceContainer, namespace[resource_id])
                _merge(primary, pointer)
            else:
                namespace[resource_id] = pointer
        else:
            resource = resource_or_pointer
            # We found a potentially conflicting resource
            if resource_id in namespace:

                # Throw away duplicate resources when the object id is the same
                if namespace[resource_id] is resource:
                    continue
                else:
                    raise DuplicateResourceException(
                        f"Duplicate resource found: {resource} and {namespace[resource_id]}"
                    )
            else:
                namespace[resource_id] = resource

    return list(namespace.values())


def _get_databases(resource: ResourceContainer) -> list[Union[Database, ResourcePointer]]:
    return cast(list[Union[Database, ResourcePointer]], resource.items(resource_type=ResourceType.DATABASE))


def _get_schemas(resource: ResourceContainer) -> list[Union[Schema, ResourcePointer]]:
    return cast(list[Union[Schema, ResourcePointer]], resource.items(resource_type=ResourceType.SCHEMA))


def _get_schema_by_name(resource: ResourceContainer, name: Union[ResourceName, str]) -> Union[Schema, ResourcePointer]:
    return cast(Union[Schema, ResourcePointer], resource.find(name=name, resource_type=ResourceType.SCHEMA))


def _get_public_schema(resource: ResourceContainer) -> Union[Schema, ResourcePointer]:
    return _get_schema_by_name(resource, "PUBLIC")


def _get_role_grants(resource: ResourceContainer) -> list[RoleGrant]:
    return cast(list[RoleGrant], resource.items(resource_type=ResourceType.ROLE_GRANT))


def _resource_scope_is_outside_blueprint_scope(resource_type: ResourceType, blueprint_scope: BlueprintScope) -> bool:
    resource_scope = RESOURCE_SCOPES[resource_type]
    if blueprint_scope == BlueprintScope.SCHEMA and (
        resource_type == ResourceType.SCHEMA or isinstance(resource_scope, (SchemaScope, TableScope))
    ):
        return False
    elif blueprint_scope == BlueprintScope.DATABASE and (
        resource_type == ResourceType.DATABASE or isinstance(resource_scope, (DatabaseScope, SchemaScope, TableScope))
    ):
        return False
    elif blueprint_scope == BlueprintScope.ACCOUNT:
        return False
    return True


class Blueprint:

    def __init__(
        self,
        name: Optional[str] = None,
        resources: Optional[list[Resource]] = None,
        run_mode: RunMode = RunMode.CREATE_OR_UPDATE,
        dry_run: bool = False,
        allowlist: Optional[list] = None,
        vars: Optional[dict] = None,
        vars_spec: Optional[list[dict]] = None,
        scope: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
    ) -> None:
        self._config: BlueprintConfig = BlueprintConfig(
            name=name,
            resources=resources,
            run_mode=RunMode(run_mode) if run_mode else RunMode.CREATE_OR_UPDATE,
            dry_run=False if dry_run is None else dry_run,
            allowlist=[ResourceType(item) for item in allowlist] if allowlist else None,
            vars=vars or {},
            vars_spec=vars_spec or [],
            scope=BlueprintScope(scope) if scope else None,
            database=ResourceName(database) if database else None,
            schema=ResourceName(schema) if schema else None,
        )
        self._finalized: bool = False
        self._staged: list[Resource] = []
        self._root: ResourcePointer = ResourcePointer(name="MISSING", resource_type=ResourceType.ACCOUNT)
        self.add(resources or [])

    @classmethod
    def from_config(cls, config: BlueprintConfig):
        blueprint = cls.__new__(cls)
        blueprint._config = config
        blueprint._staged = []
        blueprint._root = ResourcePointer(name="MISSING", resource_type=ResourceType.ACCOUNT)
        blueprint._finalized = False
        blueprint.add(config.resources or [])
        return blueprint

    def _raise_for_nonconforming_plan(self, session_ctx: SessionContext, plan: Plan):
        exceptions = []

        for change in plan:
            # Run Mode exceptions
            if self._config.run_mode == RunMode.CREATE_OR_UPDATE:
                if isinstance(change, DropResource):
                    exceptions.append(
                        f"Create-or-update mode does not allow resources to be removed (ref: {change.urn})"
                    )
            if isinstance(change, UpdateResource):
                if "name" in change.delta:
                    exceptions.append(f"Create-or-update mode does not allow renaming resources (ref: {change.urn})")
                if change.resource_cls.resource_type == ResourceType.GRANT:
                    exceptions.append(f"Grants cannot be updated (ref: {change.urn})")

            # Valid Resource Types exceptions
            if self._config.allowlist:
                if change.urn.resource_type not in self._config.allowlist:
                    exceptions.append(f"Resource type {change.urn.resource_type} not allowed in blueprint")

            # Edition exceptions
            if session_ctx["account_edition"] == AccountEdition.STANDARD:
                if isinstance(change, CreateResource) and AccountEdition.STANDARD not in change.resource_cls.edition:
                    exceptions.append(f"Resource {change.urn} requires enterprise edition or higher")

            # Scope exceptions
            if self._config.scope:
                if _resource_scope_is_outside_blueprint_scope(change.urn.resource_type, self._config.scope):
                    exceptions.append(
                        f"Resource {change.urn} is out of scope ({self._config.scope}) for this blueprint"
                    )

        if exceptions:
            if len(exceptions) > 5:
                exception_block = "\n".join(exceptions[0:5]) + f"\n... and {len(exceptions) - 5} more"
            else:
                exception_block = "\n".join(exceptions)
            raise NonConformingPlanException("Non-conforming actions found in plan:\n" + exception_block)

    def _plan(self, remote_state: State, manifest: Manifest) -> Plan:
        additive_changes: list[ResourceChange] = []
        destructive_changes: list[ResourceChange] = []

        for resource_change in diff(remote_state, manifest):
            if isinstance(resource_change, (CreateResource, UpdateResource, TransferOwnership)):
                additive_changes.append(resource_change)
            elif isinstance(resource_change, DropResource):
                destructive_changes.append(resource_change)

        # Generate a list of all URNs
        resource_set = set(manifest.urns + list(remote_state.keys()))
        for ref in manifest.refs:
            resource_set.add(ref[0])
            resource_set.add(ref[1])
        # Calculate a topological sort order for the URNs
        sort_order = topological_sort(resource_set, set(manifest.refs))
        plan = sorted(additive_changes, key=lambda change: sort_order[change.urn]) + _sort_destructive_changes(
            destructive_changes, sort_order
        )
        return plan

    def fetch_remote_state(self, session, manifest: Manifest) -> State:
        state: State = {}
        session_ctx = data_provider.fetch_session(session)

        data_provider.use_secondary_roles(session, all=True)

        if self._config.run_mode == RunMode.SYNC:
            if self._config.allowlist:
                for resource_type in self._config.allowlist:
                    for fqn in data_provider.list_resource(session, resource_label_for_type(resource_type)):
                        # FIXME
                        if self._config.scope == BlueprintScope.DATABASE and fqn.database != self._config.database:
                            continue
                        elif self._config.scope == BlueprintScope.SCHEMA and fqn.schema != self._config.schema:
                            continue
                        urn = URN(resource_type=resource_type, fqn=fqn, account_locator=session_ctx["account_locator"])
                        # state[urn] = {}  # RemoteResourceStub()
                        data = data_provider.fetch_resource(session, urn)
                        if data is None:
                            raise MissingResourceException(f"Resource could not be found: {urn}")
                        resource_cls = Resource.resolve_resource_cls(urn.resource_type, data)
                        state[urn] = resource_cls.spec(**data).to_dict(session_ctx["account_edition"])
            else:
                raise RuntimeError("Sync mode requires an allowlist")

        for urn, manifest_item in manifest.items():
            data = data_provider.fetch_resource(session, urn)
            if data is not None:
                if isinstance(manifest_item, ResourcePointer):
                    resource_cls = Resource.resolve_resource_cls(urn.resource_type, data)
                else:
                    resource_cls = manifest_item.resource_cls

                state[urn] = resource_cls.spec(**data).to_dict(session_ctx["account_edition"])

        # check for existence of resource refs
        for parent, reference in manifest.refs:
            if reference in manifest:
                continue

            is_public_schema = reference.resource_type == ResourceType.SCHEMA and reference.fqn.name == ResourceName(
                "PUBLIC"
            )

            try:
                data = data_provider.fetch_resource(session, reference)
            except Exception:
                data = None

            if data is None and not is_public_schema:
                # logger.error(manifest.to_dict(session_ctx))
                raise MissingResourceException(
                    f"Resource {reference} required by {parent} not found or failed to fetch"
                )

        return state

    def _resolve_vars(self):
        for resource in self._staged:
            resource._resolve_vars(self._config.vars)

    def _build_resource_graph(self, session_ctx: SessionContext) -> None:
        """
        Convert the staged resources into a directed graph of resources
        """
        org_scoped, acct_scoped, db_scoped, schema_scoped = _split_by_scope(self._staged)
        self._staged = []

        # Create root node of the resource graph
        if len(org_scoped) > 0:
            raise Exception("Blueprint cannot contain an Account resource")
        else:
            self._root = ResourcePointer(name="ACCOUNT", resource_type=ResourceType.ACCOUNT)

        # Merge account scoped pointers into their proper resource
        acct_scoped = _merge_pointers(acct_scoped)

        # Add all databases and other account scoped resources to the root
        for resource in acct_scoped:
            self._root.add(resource)

        if self._config.scope != BlueprintScope.ACCOUNT and self._config.database is not None:
            if len(acct_scoped) > 1:
                raise RuntimeError
            # The user has specified a database and added a resource to the config
            elif len(acct_scoped) == 1:
                scoped_database = acct_scoped[0]
                if scoped_database.resource_type != ResourceType.DATABASE:
                    raise RuntimeError(f"Expected a database, got {scoped_database.resource_type}")
                if scoped_database.name != self._config.database:
                    raise RuntimeError
            # The user has specified a database by name only
            else:
                scoped_database = ResourcePointer(name=self._config.database, resource_type=ResourceType.DATABASE)
                self._root.add(scoped_database)
                if self._config.schema is not None:
                    scoped_database.add(ResourcePointer(name=self._config.schema, resource_type=ResourceType.SCHEMA))

        # List all databases connected to root
        databases = _get_databases(self._root)

        # If the user didn't stage a database, create one from session context
        if len(databases) == 0 and (len(db_scoped) + len(schema_scoped) > 0):
            if session_ctx.get("database") is None:
                raise OrphanResourceException(
                    "Blueprint is missing a database but includes resources that require a database or schema"
                )
            logger.warning(f"No database found in config, using database {session_ctx['database']} from session")
            self._root.add(ResourcePointer(name=session_ctx["database"], resource_type=ResourceType.DATABASE))
            databases = _get_databases(self._root)

        # Attach parentless schemas to the default database, if there is one
        for resource in db_scoped:
            if resource.container is None:
                if len(databases) == 1:
                    databases[0].add(resource)
                else:
                    raise OrphanResourceException(f"Resource {resource} has no database")

        available_scopes = {}
        for database in databases:
            database_resources = list(database.items())
            _merge_pointers(database_resources)
            for schema in _get_schemas(database):
                available_scopes[f"{database.name}.{schema.name}"] = schema

        for resource in schema_scoped:
            if resource.container is None:
                if len(databases) == 1:
                    # When the blueprint is scoped all dangling resources should be assigned to the configured scope
                    if self._config.scope == BlueprintScope.SCHEMA and self._config.schema is not None:
                        scoped_schema = _get_schema_by_name(databases[0], self._config.schema)
                        scoped_schema.add(resource)
                        # TODO: figure out how to handle the case where the schema is already in the blueprint
                    else:
                        logger.warning(f"Resource {resource} has no schema, using {databases[0].name}.PUBLIC")
                        _get_public_schema(databases[0]).add(resource)
                else:
                    raise OrphanResourceException(f"No schema for resource {repr(resource)} found")
            elif isinstance(resource.container, ResourcePointer):
                schema_pointer = resource.container

                # We have a schema-scoped resource (eg a view) that has a resource pointer for the schema. The job is to connect
                # that resource into the tree
                #
                # If the schema pointer has no database, assume it lives in the only database we have
                if schema_pointer.container is None:
                    if len(databases) == 1:
                        databases[0].add(schema_pointer)
                    else:
                        raise OrphanResourceException(
                            f"No database for resource {resource} schema={resource.container}"
                        )
                elif isinstance(schema_pointer.container, ResourcePointer):
                    expected_scope = f"{schema_pointer.container.name}.{schema_pointer.name}"
                    if expected_scope in available_scopes:
                        schema = available_scopes[expected_scope]
                        schema.add(resource)
                    else:
                        self._root.add(schema_pointer.container)

            for ref in resource.refs:
                resource_and_ref_share_scope = isinstance(ref.scope, resource.scope.__class__)
                if ref.container is None and resource.container is not None and resource_and_ref_share_scope:
                    # If a resource requires another, and that secondary resource couldn't be resolved into
                    # an existing scope, then assume it lives in the same container as the original resource
                    resource.container.add(ref)

    def _create_tag_references(self) -> None:
        """
        Tag name resolution in Snowflake is special. Tags can be referenced
        by name only. If that tag name is unique in the account, the tag will be applied.
        If the tag name is not unique, the error "does not exist or not authorized" will be raised.

        To emulate this behavior, Blueprint will attempt to look up any referenced tags by name
        """
        taggables: list[TaggableResource] = []
        tags: list[Tag] = []
        for resource in _walk(self._root):
            if isinstance(resource, TaggableResource):
                taggables.append(resource)
            elif isinstance(resource, Tag):
                tags.append(resource)

        for resource in taggables:
            new_tags = {}
            if resource._tags is None:
                continue
            for tag_name, tag_value in resource._tags.items():
                identifier = parse_identifier(tag_name)
                if "database" in identifier or "schema" in identifier:
                    new_tags[tag_name] = tag_value
                else:
                    for tag in tags:
                        if tag.name == tag_name:
                            new_tags[str(tag.fqn)] = tag_value
                            break
                    else:
                        # We couldn't resolve the tag, so just use the tag name as is
                        new_tags[tag_name] = tag_value
            resource._tags = ResourceTags(new_tags)
            tag_ref = resource.create_tag_reference()
            if tag_ref:
                self._root.add(tag_ref)

    def _create_ownership_refs(self, session_ctx: SessionContext) -> None:
        role_grants: list[RoleGrant] = _get_role_grants(self._root)

        for resource in _walk(self._root):
            if isinstance(resource, ResourcePointer):
                continue
            elif isinstance(resource, RoleGrant):
                # Support ordering for role grants in a role tree
                for role_grant in role_grants:
                    if isinstance(resource.to, Role) and resource.to.name == role_grant.role.name:
                        resource.requires(role_grant)
            elif hasattr(resource._data, "owner"):
                owner = getattr(resource._data, "owner")

                # Misconfigured resource, owner should always be a Role
                if isinstance(owner, str):
                    raise RuntimeError(f"Owner of {resource} is a string, {owner}")

                owner = cast(ResourcePointer, owner)

                # Skip Snowflake-owned system resources (like INFORMATION_SCHEMA) that are owned by blank
                if owner.name == "":
                    continue

                # Require that a resource's owner role exists in remote state or has been added to the blueprint
                resource.requires(owner)

                # If the owner role isn't available in the session, try to find a role grant that can be used to
                # satisfy the requirement.
                if owner.name not in session_ctx["available_roles"]:
                    for role_grant in role_grants:
                        # Only look for role grants that match the owner role
                        if role_grant.role.name != owner.name:
                            continue

                        # Only look for role-to-role grants
                        if role_grant._data.to_role is None:
                            continue
                        resource.requires(role_grant)

                    # It's non-trivial to determine if an owner role is available in the current session because
                    # database roles aren't explicitly available in the session context
                    # else:
                    #     raise InvalidOwnerException(
                    #         f"Blueprint resource {resource} owner {resource._data.owner} must be granted to the current session"
                    #     )

    def _create_grandparent_refs(self) -> None:
        for resource in _walk(self._root):
            if isinstance(resource.scope, SchemaScope):
                resource.requires(resource.container.container)

    def _finalize_resources(self) -> None:
        for resource in _walk(self._root):
            resource._finalized = True

    def _finalize(self, session_ctx: SessionContext) -> None:
        if self._finalized:
            raise RuntimeError("Blueprint already finalized")
        self._finalized = True
        self._resolve_vars()
        self._build_resource_graph(session_ctx)
        self._create_tag_references()
        self._create_ownership_refs(session_ctx)
        self._create_grandparent_refs()
        self._finalize_resources()

    def generate_manifest(self, session_ctx: SessionContext) -> Manifest:
        manifest = Manifest(account_locator=session_ctx["account_locator"])
        self._finalize(session_ctx)
        for resource in _walk(self._root):
            if isinstance(resource, Resource):
                manifest.add(resource, session_ctx["account_edition"])
            else:
                raise RuntimeError(f"Unexpected object found in blueprint: {resource}")

        return manifest

    def plan(self, session) -> Plan:
        reset_cache()
        logger.debug("Using blueprint vars:")
        for key in self._config.vars.keys():
            logger.debug(f"  {key}")
        session_ctx = data_provider.fetch_session(session)
        manifest = self.generate_manifest(session_ctx)
        remote_state = self.fetch_remote_state(session, manifest)
        try:
            finished_plan = self._plan(remote_state, manifest)
        except Exception as e:
            logger.error("~" * 80 + "REMOTE STATE")
            logger.error(remote_state)
            logger.error("~" * 80 + "MANIFEST")
            logger.error(manifest)

            raise e
        self._raise_for_nonconforming_plan(session_ctx, finished_plan)
        return finished_plan

    def apply(self, session, plan: Optional[Plan] = None):
        if plan is None:
            plan = self.plan(session)

        # TODO: cursor setup, including query tag

        """
            At this point, we have a list of actions as a part of the plan. Each action is one of:
                1. ADD action (CREATE command)
                2. CHANGE action (one or many ALTER or SET PARAMETER commands)
                3. REMOVE action (DROP command, REVOKE command, or a rename operation)
                4. TRANSFER action (GRANT OWNERSHIP command)

            Each action requires:
                • a set of privileges necessary to run commands
                • the appropriate role to execute commands

            Once we've determined those things, we can compare the list of required roles and privileges
            against what we have access to in the session and the role tree.
        """

        session_ctx = data_provider.fetch_session(session)

        _raise_if_plan_would_drop_session_user(session_ctx, plan)

        action_queue = compile_plan_to_sql(session_ctx, plan)
        actions_taken = []

        while action_queue:
            sql = action_queue.pop(0)
            actions_taken.append(sql)
            try:
                if not self._config.dry_run:
                    execute(session, sql)
            except snowflake.connector.errors.ProgrammingError as err:
                if err.errno == ALREADY_EXISTS_ERR:
                    logger.error(f"Resource already exists: {sql}, skipping...")
                elif err.errno == INVALID_GRANT_ERR:
                    logger.error(f"Invalid grant: {sql}, skipping...")
                elif err.errno == DOES_NOT_EXIST_ERR and sql.startswith("REVOKE"):
                    logger.error(f"Resource does not exist: {sql}, skipping...")
                elif err.errno == DOES_NOT_EXIST_ERR and sql.startswith("DROP"):
                    logger.error(f"Resource does not exist: {sql}, skipping...")
                else:
                    raise err
        return actions_taken

    def _add(self, resource: Resource):
        if self._finalized:
            raise Exception("Cannot add resources to a finalized blueprint")
        if not isinstance(resource, Resource):
            raise Exception(f"Expected a Resource, got {type(resource)} -> {resource}")
        if resource._finalized:
            raise Exception("Cannot add a finalized resource to a blueprint")
        if self._config.allowlist and resource.resource_type not in self._config.allowlist:
            raise InvalidResourceException(f"Resource {resource} is not in the allowlist")
        self._staged.append(resource)

    def add(self, *resources):
        if isinstance(resources[0], list):
            resources = resources[0]
        for resource in resources:
            self._add(resource)


def owner_for_change(change: ResourceChange) -> Optional[ResourceName]:
    if isinstance(change, CreateResource) and "owner" in change.after:
        return ResourceName(change.after["owner"])
    elif isinstance(change, UpdateResource) and "owner" in change.after:
        # TRANSFER actions occur strictly after CHANGE actions, so we use the before owner
        # as the role for the change
        return ResourceName(change.before["owner"])
    elif isinstance(change, DropResource) and "owner" in change.before:
        return ResourceName(change.before["owner"])
    elif isinstance(change, TransferOwnership):
        return ResourceName(change.from_owner)
    else:
        return None


def execution_strategy_for_change(
    change: ResourceChange,
    available_roles: list[ResourceName],
    default_role: ResourceName,
) -> tuple[ResourceName, bool]:

    change_owner = owner_for_change(change)

    if resource_type_is_grant(change.urn.resource_type):

        # 2024-10-22: maybe the better thing to do is check role privs selectively
        if isinstance(change, CreateResource) and change.urn.resource_type == ResourceType.GRANT:
            execution_role = system_role_for_priv(change.after["priv"])
            if execution_role and execution_role in available_roles:
                return execution_role, False

        if "SECURITYADMIN" in available_roles:
            return ResourceName("SECURITYADMIN"), False

        return default_role, False

    elif change.urn.resource_type == ResourceType.TAG_REFERENCE:
        # There are two ways you can create a tag reference:
        # 1. You have the global APPLY TAGS priv on the account (given to ACCOUNTADMIN by default)
        # 2. You have APPLY privilege on the TAG object AND you have ownership of the tagged object
        if "ACCOUNTADMIN" in available_roles:
            return ResourceName("ACCOUNTADMIN"), False

        return default_role, False

    elif change.urn.resource_type == ResourceType.RESOURCE_MONITOR:
        # For some reason Snowflake chose to not have a priv type for resource monitors.
        # Only ACCOUNTADMIN can create them.
        if "ACCOUNTADMIN" in available_roles:
            return ResourceName("ACCOUNTADMIN"), False
        raise MissingPrivilegeException("ACCOUNTADMIN role is required to work with resource monitors")

    elif change.urn.resource_type == ResourceType.ACCOUNT_PARAMETER:
        if "ACCOUNTADMIN" in available_roles:
            return ResourceName("ACCOUNTADMIN"), False
        raise MissingPrivilegeException("ACCOUNTADMIN role is required to work with account parameters")

    elif change.urn.resource_type == ResourceType.SCANNER_PACKAGE:
        if "ACCOUNTADMIN" in available_roles:
            return ResourceName("ACCOUNTADMIN"), False
        raise MissingPrivilegeException("ACCOUNTADMIN role is required to work with scanner packages")

    elif isinstance(change, (UpdateResource, DropResource, TransferOwnership)):
        if change_owner:
            return change_owner, False
        else:
            raise MissingPrivilegeException(change)
    elif isinstance(change, CreateResource):
        if isinstance(change.resource_cls.scope, AccountScope):
            create_priv = CREATE_PRIV_FOR_RESOURCE_TYPE[change.urn.resource_type]
            system_role = system_role_for_priv(create_priv)
            if system_role and system_role in available_roles:
                transfer_ownership = system_role != change_owner
                return system_role, transfer_ownership
            raise MissingPrivilegeException(f"{system_role} isnt available to execute {change}")
        elif isinstance(change.resource_cls.scope, (DatabaseScope, SchemaScope)) and change.container:
            container_owner = ResourceName(change.container[1])
            if container_owner in available_roles:
                transfer_ownership = container_owner != change_owner
                if transfer_ownership and change.urn.resource_type == ResourceType.NOTEBOOK:
                    raise Exception("Notebook ownership cannot be transferred")
                return container_owner, transfer_ownership
            raise MissingPrivilegeException(f"{container_owner} isnt available to execute {change}")

    raise RuntimeError(f"Unhandled change type: {change}")


def sql_commands_for_change(
    change: ResourceChange,
    available_roles: list[ResourceName],
    default_role: ResourceName,
):
    """
    In Snowflake's RBAC model, a session has an active role, and zero or more secondary roles.

    The active role of a session is set as follows:
    - When a session is started:
        - If the session is configured with a role, that is the active role
        - Otherwise, if the user of the session has a default_role set, and that role exists, that is the active role
        - Otherwise, the PUBLIC role is activated (PUBLIC cannot be revoked)
    - Any time the USE ROLE command is run, the active role is switched

    A session may run any command thats allowed by the active role or any role downstream from it in the role hierarchy.
    When secondary roles are active (by running the command USE SECONDARY ROLES ALL), then the session may also run any
    command that any secondary role or a role downstream from it is allowed to run.

    However, when a CREATE command is run, only the active role is considered. This is because the role that
    creates a new resource owns that resource by default. There are some exceptions with GRANTS.

    For those reasons, we generally don't have to worry about the current role as long as we have activated secondary roles.
    The exception is when creating new resources
    """

    before_change_cmd = []
    change_cmd = None
    after_change_cmd = []

    execution_role, transfer_owner = execution_strategy_for_change(
        change,
        available_roles,
        default_role,
    )
    before_change_cmd.append(f"USE ROLE {execution_role}")

    if isinstance(change, CreateResource):

        change_cmd = lifecycle.create_resource(change.urn, change.after, change.resource_cls.props)
        if transfer_owner:
            after_change_cmd.append(
                lifecycle.transfer_resource(
                    change.urn,
                    owner=change.after["owner"],
                    owner_resource_type=infer_role_type_from_name(change.after["owner"]),
                    copy_current_grants=True,
                )
            )
            # SPECIAL CASE: when creating a database with a custom owner that we will transfer ownership to,
            # we also need to transfer ownership of the public schema to that role. This replicates the behavior
            # if we were to create the database with a custom owner directly
            if change.urn.resource_type == ResourceType.DATABASE:
                after_change_cmd.append(
                    lifecycle.transfer_resource(
                        public_schema_urn(change.urn),
                        owner=change.after["owner"],
                        owner_resource_type=infer_role_type_from_name(change.after["owner"]),
                        copy_current_grants=True,
                    )
                )

            if change.urn.resource_type == ResourceType.SCANNER_PACKAGE:
                after_change_cmd.append(lifecycle.update_resource(change.urn, {}, change.resource_cls.props))
    elif isinstance(change, UpdateResource):
        props = Resource.props_for_resource_type(change.urn.resource_type, change.after)
        change_cmd = lifecycle.update_resource(change.urn, change.delta, props)
    elif isinstance(change, DropResource):
        if transfer_owner:
            before_change_cmd.append(
                lifecycle.transfer_resource(
                    change.urn,
                    owner=str(execution_role),
                    owner_resource_type=infer_role_type_from_name(str(execution_role)),
                    copy_current_grants=True,
                )
            )
        change_cmd = lifecycle.drop_resource(
            change.urn,
            change.before,
            if_exists=True,
        )
    elif isinstance(change, TransferOwnership):
        change_cmd = lifecycle.transfer_resource(
            change.urn,
            owner=change.to_owner,
            owner_resource_type=infer_role_type_from_name(change.to_owner),
            copy_current_grants=True,
        )

    return before_change_cmd + [change_cmd] + after_change_cmd


def compile_plan_to_sql(session_ctx: SessionContext, plan: Plan):
    sql_commands = []

    sql_commands.append("USE SECONDARY ROLES ALL")
    available_roles = session_ctx["available_roles"].copy()
    default_role = session_ctx["role"]
    for change in plan:
        # Generate SQL commands
        commands = sql_commands_for_change(
            change,
            available_roles,
            default_role,
        )
        sql_commands.extend(commands)

        if isinstance(change, CreateResource):
            if change.urn.resource_type == ResourceType.ROLE:
                available_roles.append(ResourceName(change.after["name"]))
            elif change.urn.resource_type == ResourceType.ROLE_GRANT:
                if change.after["to_role"] in available_roles:
                    available_roles.append(ResourceName(change.after["role"]))

    return sql_commands


def topological_sort(resource_set: set[T], references: set[tuple[T, T]]) -> dict[T, int]:
    # Kahn's algorithm

    # Compute in-degree (# of inbound edges) for each node
    in_degrees: dict[T, int] = {}
    outgoing_edges: dict[T, set[T]] = {}

    for node in resource_set:
        in_degrees[node] = 0
        outgoing_edges[node] = set()

    for node, ref in references:
        in_degrees[ref] += 1
        outgoing_edges[node].add(ref)

    # Put all nodes with 0 in-degree in a queue
    queue: Queue = Queue()
    for node, in_degree in in_degrees.items():
        if in_degree == 0:
            queue.put(node)

    # Create an empty node list
    nodes = []

    while not queue.empty():
        node = queue.get()
        nodes.append(node)

        # For each of node's outgoing edges
        empty_neighbors = set()
        for edge in outgoing_edges[node]:
            in_degrees[edge] -= 1
            if in_degrees[edge] == 0:
                queue.put(edge)
                empty_neighbors.add(edge)

        # Remove edges to empty neighbors
        outgoing_edges[node].difference_update(empty_neighbors)
    nodes.reverse()
    if len(nodes) != len(resource_set):
        raise Exception("Graph is not a DAG")
    return {value: index for index, value in enumerate(nodes)}


def diff(remote_state: State, manifest: Manifest):

    def _container_descriptor(resource_urn: URN) -> Optional[ContainerDescriptor]:
        """
        Given the URN of a resource, return a descriptor of the container that owns it.
        """
        if isinstance(RESOURCE_SCOPES[resource_urn.resource_type], AccountScope):
            return None

        container_urn = _container_urn(resource_urn)
        if container_urn in remote_state:
            if "owner" in remote_state[container_urn]:
                container_owner = remote_state[container_urn]["owner"]
            else:
                raise Exception(f"Remote state for {container_urn} is missing owner -> {remote_state[container_urn]}")
        else:
            manifest_item = manifest[container_urn]
            if isinstance(manifest_item, ManifestResource):
                container_owner = manifest_item.data["owner"]
            else:
                raise MissingResourceException(
                    f"Blueprint has pointer to resource that doesn't exist or isn't visible in session: {container_urn}"
                )

        return (container_urn, container_owner)

    def _diff_resource_data(lhs: dict, rhs: dict) -> dict:

        if not isinstance(lhs, dict) or not isinstance(rhs, dict):
            raise TypeError("diff_resources requires two dictionaries")

        delta = {}
        for field_name in lhs.keys():
            lhs_value = lhs[field_name]
            rhs_value = rhs[field_name]
            if lhs_value != rhs_value:
                delta[field_name] = rhs_value
        return delta

    state_urns = set(remote_state.keys())
    manifest_urns = set(manifest.urns)

    # Resources in remote state but not in the manifest should be removed
    for urn in state_urns - manifest_urns:
        yield DropResource(urn, remote_state[urn])

    # Resources in the manifest but not in remote state should be added
    for urn in manifest_urns - state_urns:
        manifest_item = manifest[urn]
        if isinstance(manifest_item, ResourcePointer):
            raise MissingResourceException(
                f"Blueprint has pointer to resource that doesn't exist or isn't visible in session: {urn}"
            )
        elif isinstance(manifest_item, ManifestResource):
            # We don't create implicit resources
            if manifest[urn].implicit:
                continue
            yield CreateResource(
                urn,
                manifest_item.resource_cls,
                _container_descriptor(urn),
                manifest_item.data,
            )
        else:
            raise Exception(f"Unknown type in manifest: {manifest_item}")

    # Resources in both should be compared
    for urn in state_urns & manifest_urns:
        manifest_item = manifest[urn]

        # We don't diff resource pointers
        if isinstance(manifest_item, ResourcePointer):
            continue

        delta = _diff_resource_data(remote_state[urn], manifest_item.data)
        owner_attr = delta.pop("owner", None)

        # TODO: do we care about implicit resources?
        replace_resource = False
        create_resource = False
        ignore_fields = set()
        for attr in delta.keys():
            attr_metadata = manifest_item.resource_cls.spec.get_metadata(attr)
            change_requires_replacement = attr_metadata.triggers_replacement
            change_triggers_create = attr_metadata.triggers_create
            change_is_fetchable = attr_metadata.fetchable
            change_is_known_after_apply = attr_metadata.known_after_apply
            change_should_be_ignored = attr in manifest_item.lifecycle.ignore_changes or attr_metadata.ignore_changes
            if change_requires_replacement:
                replace_resource = True
                break
            elif change_triggers_create:
                create_resource = True
                break
            elif not change_is_fetchable:
                ignore_fields.add(attr)
            elif change_is_known_after_apply:
                ignore_fields.add(attr)
            elif change_should_be_ignored:
                ignore_fields.add(attr)

        if replace_resource:
            raise NotImplementedError("replace_resource")
            # yield DropResource(urn, remote_state[urn])
            # yield CreateResource(urn, manifest_item.resource_cls, manifest_item.data)
            # continue

        if create_resource:
            yield CreateResource(
                urn,
                manifest_item.resource_cls,
                _container_descriptor(urn),
                manifest_item.data,
            )
            continue

        delta = {k: v for k, v in delta.items() if k not in ignore_fields}
        if delta:
            yield UpdateResource(
                urn,
                manifest_item.resource_cls,
                remote_state[urn],
                manifest_item.data,
                delta,
            )

        # Force transfers to occur after all other attribute changes
        if owner_attr:
            owner_metadata = manifest_item.resource_cls.spec.get_metadata("owner")
            owner_is_fetchable = owner_metadata.fetchable
            owner_changes_should_be_ignored = (
                "owner" in manifest_item.lifecycle.ignore_changes or owner_metadata.ignore_changes
            )

            if not owner_is_fetchable or owner_changes_should_be_ignored:
                continue

            yield TransferOwnership(
                urn,
                manifest_item.resource_cls,
                from_owner=remote_state[urn]["owner"],
                to_owner=manifest_item.data["owner"],
            )


def _sort_destructive_changes(
    destructive_changes: list[ResourceChange], sort_order: dict[URN, int]
) -> list[ResourceChange]:
    # Not quite right but close enough for now.
    def sort_key(change: ResourceChange) -> tuple:
        return (
            # Put network policies first
            change.urn.resource_type != ResourceType.NETWORK_POLICY,
            # Put roles and role grants last
            change.urn.resource_type == ResourceType.ROLE_GRANT,
            change.urn.resource_type == ResourceType.ROLE,
            change.urn.database is not None,
            change.urn.schema is not None,
            -1 * sort_order[change.urn],
        )

    return sorted(destructive_changes, key=sort_key)


def _container_urn(resource_urn: URN) -> URN:
    scope = RESOURCE_SCOPES[resource_urn.resource_type]
    container_urn: URN

    if isinstance(scope, AccountScope):
        container_urn = resource_urn.account()
    elif isinstance(scope, DatabaseScope):
        container_urn = resource_urn.database()
    elif isinstance(scope, SchemaScope):
        container_urn = resource_urn.schema()
    else:
        raise NotImplementedError(f"Unsupported resource scope: {scope}")
    return container_urn
