import json
import logging
from dataclasses import dataclass
from queue import Queue
from typing import Generator, Optional, Union

import snowflake.connector

from . import data_provider, lifecycle
from .client import (
    ALREADY_EXISTS_ERR,
    DOES_NOT_EXIST_ERR,
    INVALID_GRANT_ERR,
    execute,
    reset_cache,
)
from .diff import Action, diff
from .enums import ParseableEnum, ResourceType, resource_type_is_grant
from .identifiers import URN, parse_identifier, resource_label_for_type
from .privs import (
    CREATE_PRIV_FOR_RESOURCE_TYPE,
    PRIVS_FOR_RESOURCE_TYPE,
    AccountPriv,
    GrantedPrivilege,
    execution_role_for_priv,
    is_ownership_priv,
)
from .resource_name import ResourceName
from .resources import Account, Database
from .resources.resource import RESOURCE_SCOPES, Resource, ResourceContainer, ResourcePointer
from .resources.tag import Tag, TaggableResource
from .scope import AccountScope, DatabaseScope, OrganizationScope, SchemaScope

logger = logging.getLogger("titan")

SYNC_MODE_BLOCKLIST = [
    ResourceType.FUTURE_GRANT,
    ResourceType.GRANT,
    ResourceType.GRANT_ON_ALL,
    ResourceType.ROLE,
    ResourceType.USER,
    ResourceType.TABLE,
]


class DuplicateResourceException(Exception):
    pass


class MissingResourceException(Exception):
    pass


class MissingPrivilegeException(Exception):
    pass


class MarkedForReplacementException(Exception):
    pass


class NonConformingPlanException(Exception):
    pass


class ResourceInsertionException(Exception):
    pass


class OrphanResourceException(Exception):
    pass


class InvalidOwnerException(Exception):
    pass


class RunMode(ParseableEnum):
    CREATE_OR_UPDATE = "CREATE-OR-UPDATE"
    SYNC = "SYNC"
    SYNC_ALL = "SYNC-ALL"


@dataclass
class ResourceChange:
    action: Action
    urn: URN
    before: dict
    after: dict
    delta: dict

    def to_dict(self):
        return {
            "action": self.action.value,
            "urn": str(self.urn),
            "before": self.before,
            "after": self.after,
            "delta": self.delta,
        }


State = dict[URN, dict]
Plan = list[ResourceChange]


class Manifest:
    def __init__(self, account_locator: str = ""):
        self._account_locator = account_locator
        self._data: dict[URN, Resource] = {}
        self._refs = []

    def __getitem__(self, key: URN):
        if isinstance(key, URN):
            return self._data[key]
        else:
            raise Exception("Manifest keys must be URNs")

    def __contains__(self, key):
        if isinstance(key, URN):
            return key in self._data
        else:
            raise Exception("Manifest keys must be URNs")

    def add(self, resource: Resource):

        urn = URN.from_resource(
            account_locator=self._account_locator,
            resource=resource,
        )

        # resource_data = resource.to_dict()

        if urn in self._data:
            # if resource_data != self._data[urn]:
            if not isinstance(resource, ResourcePointer):
                logger.warning(f"Duplicate resource {urn} with conflicting data, discarding {resource}")
            return
        self._data[urn] = resource
        for ref in resource.refs:
            ref_urn = URN.from_resource(account_locator=self._account_locator, resource=ref)
            self._refs.append((urn, ref_urn))

    def get(self, key: URN, default=None):
        if isinstance(key, URN):
            return self._data.get(key, default)
        else:
            raise Exception("Manifest keys must be URNs")

    def to_dict(self):
        return {k: v.to_dict() for k, v in self._data.items()}

    @property
    def urns(self) -> list[URN]:
        return list(self._data.keys())

    @property
    def refs(self):
        return self._refs


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

        add_count = len([change for change in plan if change.action == Action.ADD])
        change_count = len([change for change in plan if change.action == Action.CHANGE])
        transfer_count = len([change for change in plan if change.action == Action.TRANSFER])
        remove_count = len([change for change in plan if change.action == Action.REMOVE])

        output += "\n» titan core\n"
        output += f"» Plan: {add_count} to add, {change_count} to change, {transfer_count} to transfer, {remove_count} to destroy.\n\n"

        for change in plan:
            action_marker = ""
            if change.action == Action.ADD:
                action_marker = "+"
            elif change.action in (Action.CHANGE, Action.TRANSFER):
                action_marker = "~"
            elif change.action == Action.REMOVE:
                action_marker = "-"
            output += f"{action_marker} {change.urn}"
            if change.action != Action.REMOVE:
                output += " {"
            output += "\n"
            key_lengths = [len(key) for key in change.delta.keys()]
            max_key_length = max(key_lengths) if len(key_lengths) > 0 else 0
            for key, value in change.delta.items():
                if key.startswith("_"):
                    continue
                new_value = _render_value(value)
                before_value = ""
                if key in change.before:
                    before_value = _render_value(change.before[key]) + " -> "
                output += f"  {action_marker} {key:<{max_key_length}} = {before_value}{new_value}\n"
            if change.action != Action.REMOVE:
                output += "}\n"
            output += "\n"

        return output
    else:
        raise Exception(f"Unsupported format {format}")


def print_plan(plan: Plan):
    print(dump_plan(plan, format="text"))


# def plan_sql(plan: Plan) -> list[str]:
#     sql_commands = []
#     for change in plan:
#         props = Resource.props_for_resource_type(change.urn.resource_type, change.after)
#         if change.action == Action.ADD:
#             sql_commands.append(lifecycle.create_resource(change.urn, change.after, props))
#         elif change.action == Action.CHANGE:
#             sql_commands.append(lifecycle.update_resource(change.urn, change.delta))
#         elif change.action == Action.REMOVE:
#             sql_commands.append(lifecycle.drop_resource(change.urn, change.before))
#     return sql_commands


def print_diffs(diffs):
    for action, target, deltas in diffs:
        print(f"[{action}]", target)
        for delta in deltas:
            print("\t", delta)


def _split_by_scope(
    resources: list[Resource],
) -> tuple[list[Resource], list[Resource], list[Resource], list[Resource]]:
    org_scoped = []
    acct_scoped = []
    db_scoped = []
    schema_scoped = []

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
        while root.container is not None:
            root = root.container
        route(root)
    return org_scoped, acct_scoped, db_scoped, schema_scoped


def _walk(resource: Resource) -> Generator[Resource, None, None]:
    yield resource
    if isinstance(resource, ResourceContainer):
        for item in resource.items():
            yield from _walk(item)


def _raise_if_plan_would_drop_session_user(session_ctx: dict, plan: Plan):
    for change in plan:
        if change.urn.resource_type == ResourceType.USER and change.action == Action.REMOVE:
            if ResourceName(session_ctx["user"]) == ResourceName(change.urn.fqn.name):
                raise Exception("Plan would drop the current session user, which is not allowed")


def _merge_pointers(resources: list[Resource]) -> list[Resource]:
    namespace = {}
    resources = sorted(resources, key=lambda resource: isinstance(resource, ResourcePointer))

    def _merge(primary: Resource, secondary: ResourcePointer):
        if secondary.container is not None:
            if primary.container is None:
                raise Exception
            secondary.container.remove(secondary)

        for item in secondary.items():
            secondary.remove(item)
            primary.add(item)

    for resource in resources:
        if hasattr(resource, "name"):
            resource_id = (resource.resource_type, resource.name)
        else:
            resource_id = str(resource.urn)
        if isinstance(resource, ResourcePointer):
            if resource_id in namespace:
                primary = namespace[resource_id]
                _merge(primary, resource)
            else:
                namespace[resource_id] = resource
        else:
            if resource_id in namespace and namespace[resource_id] != resource:

                if resource.implicit or namespace[resource_id].implicit:
                    primary, secondary = resource, namespace[resource_id]
                    if namespace[resource_id].implicit:
                        primary, secondary = secondary, primary
                    _merge(primary, secondary)
                    continue
                raise DuplicateResourceException(f"Duplicate resource found: {resource} and {namespace[resource_id]}")
            namespace[resource_id] = resource

    return list(namespace.values())


class Blueprint:

    def __init__(
        self,
        name: Optional[str] = None,
        resources: Optional[list[Resource]] = None,
        run_mode: RunMode = RunMode.CREATE_OR_UPDATE,
        dry_run: bool = False,
        allowlist: Optional[list[ResourceType]] = None,
        **kwargs,
    ) -> None:

        account = kwargs.pop("account", None)
        database = kwargs.pop("database", None)
        schema = kwargs.pop("schema", None)
        allow_role_switching = kwargs.pop("allow_role_switching", None)
        ignore_ownership = kwargs.pop("ignore_ownership", None)
        valid_resource_types = kwargs.pop("valid_resource_types", None)

        if account is not None:
            raise Exception("account is deprecated, use add instead")
        if database is not None:
            raise Exception("database is deprecated, use add instead")
        if schema is not None:
            raise Exception("schema is deprecated, use add instead")
        if allow_role_switching is not None:
            raise Exception("Role switching must be allowed")
        if ignore_ownership is not None:
            # TODO: exception
            logger.warning("ignore_ownership is deprecated")
        if valid_resource_types is not None:
            logger.warning("valid_resource_types is deprecated")
            allowlist = valid_resource_types

        self._finalized = False
        self._staged: list[Resource] = []
        self._root: Account = None
        self._run_mode: RunMode = RunMode(run_mode)
        self._dry_run: bool = dry_run
        self._allowlist: list[ResourceType] = [ResourceType(v) for v in allowlist or []]

        if self._run_mode == RunMode.SYNC_ALL:
            logger.warning("Sync All mode is dangerous, please use with caution")
            if len(self._allowlist) == 0:
                raise Exception("Sync mode must specify an allowlist")
        elif self._run_mode == RunMode.SYNC:
            if len(self._allowlist) == 0:
                raise Exception("Sync mode must specify an allowlist")
            for resource_type in self._allowlist:
                if resource_type in SYNC_MODE_BLOCKLIST:
                    raise Exception(f"Resource type {resource_type} is not allowed in sync mode")

        self.name = name

        self.add(resources or [])

    def _raise_for_nonconforming_plan(self, plan: Plan):
        exceptions = []

        # Run Mode exceptions
        if self._run_mode == RunMode.CREATE_OR_UPDATE:
            for change in plan:
                if change.action == Action.REMOVE:
                    exceptions.append(
                        f"Create-or-update mode does not allow resources to be removed (ref: {change.urn})"
                    )
                if change.action == Action.CHANGE:
                    if "name" in change.delta:
                        exceptions.append(
                            f"Create-or-update mode does not allow renaming resources (ref: {change.urn})"
                        )

        if self._run_mode == RunMode.SYNC:
            for change in plan:
                if change.urn.resource_type in SYNC_MODE_BLOCKLIST:
                    exceptions.append(
                        f"Sync mode does not allow changes to {change.urn.resource_type} (ref: {change.urn})"
                    )

        # Valid Resource Types exceptions
        if self._allowlist:
            for change in plan:
                if change.urn.resource_type not in self._allowlist:
                    exceptions.append(f"Resource type {change.urn.resource_type} not allowed in blueprint")

        if exceptions:
            if len(exceptions) > 5:
                exception_block = "\n".join(exceptions[0:5]) + f"\n... and {len(exceptions) - 5} more"
            else:
                exception_block = "\n".join(exceptions)
            raise NonConformingPlanException("Non-conforming actions found in plan:\n" + exception_block)

    def _plan(self, remote_state: State, manifest: Manifest) -> Plan:
        config = manifest.to_dict()

        changes: Plan = []
        marked_for_replacement = set()
        for action, urn, delta in diff(remote_state, config):
            before = remote_state.get(urn, {})
            after = config.get(urn, {})

            if action == Action.CHANGE:
                if urn in marked_for_replacement:
                    continue

                resource = manifest[urn]

                # TODO: if the attr is marked as must_replace, then instead we yield a rename, add, remove
                attr = list(delta.keys())[0]
                attr_metadata = resource.spec.get_metadata(attr)

                change_requires_replacement = attr_metadata.triggers_replacement
                change_forces_add = attr_metadata.forces_add
                change_is_fetchable = attr_metadata.fetchable
                change_should_be_ignored = attr in resource.lifecycle.ignore_changes or attr_metadata.ignore_changes

                if change_requires_replacement:
                    raise MarkedForReplacementException(f"Resource {urn} is marked for replacement due to {attr}")
                    marked_for_replacement.add(urn)
                elif change_forces_add:
                    changes.append(ResourceChange(action=Action.ADD, urn=urn, before={}, after=after, delta=delta))
                    continue
                elif not change_is_fetchable:
                    # drift on fields that aren't fetchable should be ignored
                    # TODO: throw a warning, or have a blueprint runmode that fails on this
                    continue
                elif change_should_be_ignored:
                    continue
                else:
                    changes.append(ResourceChange(action, urn, before, after, delta))
            elif action == Action.ADD:
                changes.append(ResourceChange(action=action, urn=urn, before={}, after=after, delta=delta))
            elif action == Action.REMOVE:
                changes.append(ResourceChange(action=action, urn=urn, before=before, after={}, delta={}))
            elif action == Action.TRANSFER:
                resource = manifest[urn]

                attr = list(delta.keys())[0]
                attr_metadata = resource.spec.get_metadata(attr)
                change_is_fetchable = attr_metadata.fetchable
                change_should_be_ignored = attr in resource.lifecycle.ignore_changes or attr_metadata.ignore_changes
                if not change_is_fetchable:
                    continue
                if change_should_be_ignored:
                    continue
                changes.append(ResourceChange(action=action, urn=urn, before=before, after=after, delta=delta))

        for urn in marked_for_replacement:
            raise MarkedForReplacementException(f"Resource {urn} is marked for replacement")
            # changes.append(ResourceChange(action=Action.REMOVE, urn=urn, before=before, after={}, delta={}))
            # changes.append(ResourceChange(action=Action.ADD, urn=urn, before={}, after=after, delta=after))

        # Generate a list of all URNs
        resource_set = set(manifest.urns + list(remote_state.keys()))
        for ref in manifest.refs:
            resource_set.add(ref[0])
            resource_set.add(ref[1])
        # Calculate a topological sort order for the URNs
        sort_order = topological_sort(resource_set, set(manifest.refs))
        return sorted(changes, key=lambda change: sort_order[change.urn])

    def fetch_remote_state(self, session, manifest: Manifest) -> State:
        state: State = {}
        session_ctx = data_provider.fetch_session(session)

        def _normalize(urn: URN, data: dict) -> dict:
            resource_cls = Resource.resolve_resource_cls(urn.resource_type, data)
            if urn.resource_type == ResourceType.FUTURE_GRANT:
                normalized = data
            elif isinstance(data, list):
                raise Exception(f"Fetching list of {urn.resource_type} is not supported yet")
            else:
                # There is an edge case here where the resource spec doesnt have defaults specified.
                # Instead of throwing an error, dataclass will provide a dataclass._MISSINGFIELD object
                # That is bad.
                # The answer is not that defaults should be added. The root cause is that data_provider
                # method return raw dicts that aren't type checked against their corresponding
                # Resource spec.
                # I have considered tightly coupling the data provider to the resource spec, but I don't think
                # the complexity is worth it.
                # Another solution would be to build in automatic tests to check that the data_provider
                # returns data that matches the spec.
                normalized = resource_cls.defaults() | data
            return normalized

        if self._run_mode in (RunMode.SYNC, RunMode.SYNC_ALL):
            """
            In sync mode, the remote state is not just the resources that were added to the blueprint,
            but all resources that exist in Snowflake. This is limited by a few things:
            - allowlist limits the scope of what resources types are allowed in a blueprint
            - if database or schema is set, the blueprint only looks at that database or schema
            """
            if len(self._allowlist) == 0:
                raise Exception("Sync mode must specify an allowlist")

            for resource_type in self._allowlist:
                for fqn in data_provider.list_resource(session, resource_label_for_type(resource_type)):
                    urn = URN(resource_type=resource_type, fqn=fqn, account_locator=session_ctx["account_locator"])
                    data = data_provider.fetch_resource(session, urn)
                    if data is None:
                        raise Exception(f"Resource {urn} not found")
                    normalized_data = _normalize(urn, data)
                    state[urn] = normalized_data

        for urn in manifest.urns:
            data = data_provider.fetch_resource(session, urn)
            if data is not None:
                normalized_data = _normalize(urn, data)
                state[urn] = normalized_data

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
                logger.error(manifest.to_dict())
                raise MissingResourceException(
                    f"Resource {reference} required by {parent} not found or failed to fetch"
                )

        return state

    def _build_resource_graph(self, session_ctx: dict):
        """
        Convert the staged resources into a tree of resources
        """
        org_scoped, acct_scoped, db_scoped, schema_scoped = _split_by_scope(self._staged)
        self._staged = None

        # Create root node of the resource graph
        if len(org_scoped) > 0:
            raise Exception("Blueprint cannot contain an Account resource")
        else:
            self._root = ResourcePointer(name=session_ctx["account"], resource_type=ResourceType.ACCOUNT)

        # Merge account scoped pointers into their proper resource
        acct_scoped = _merge_pointers(acct_scoped)

        # Add all databases and other account scoped resources to the root
        for resource in acct_scoped:
            self._root.add(resource)

        # List all databases connected to root
        databases: list[Union[Database, ResourcePointer]] = self._root.items(resource_type=ResourceType.DATABASE)

        # If the user didn't stage a database, create one from session context
        if len(databases) == 0 and (len(db_scoped) + len(schema_scoped) > 0):
            if session_ctx.get("database") is None:
                raise OrphanResourceException(
                    "Blueprint is missing a database but includes resources that require a database or schema"
                )
            logger.warning(f"No database found in config, using database {session_ctx['database']} from session")
            self._root.add(ResourcePointer(name=session_ctx["database"], resource_type=ResourceType.DATABASE))
            databases = self._root.items(resource_type=ResourceType.DATABASE)

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
            for schema in database.items(resource_type=ResourceType.SCHEMA):
                available_scopes[f"{database.name}.{schema.name}"] = schema

        for resource in schema_scoped:

            if resource.container is None:
                if len(databases) == 1:
                    logger.warning(f"Resource {resource} has no schema, using {databases[0].name}.PUBLIC")
                    databases[0].find(name="PUBLIC", resource_type=ResourceType.SCHEMA).add(resource)
                else:
                    raise Exception(f"No schema for resource {repr(resource)} found")
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
                        raise Exception(f"No database for resource {resource} schema={resource.container}")
                elif isinstance(schema_pointer.container, ResourcePointer):
                    expected_scope = f"{schema_pointer.container.name}.{schema_pointer.name}"
                    if expected_scope in available_scopes:
                        schema = available_scopes[expected_scope]
                        schema.add(resource)
                    else:
                        self._root.add(schema_pointer.container)

            for ref in resource.refs:
                resource_and_ref_share_scope = isinstance(ref.scope, resource.scope.__class__)
                if ref.container is None and resource_and_ref_share_scope:
                    # If a resource requires another, and that secondary resource couldn't be resolved into
                    # an existing scope, then assume it lives in the same container as the original resource
                    resource.container.add(ref)

    def _create_tag_references(self):
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
            resource._tags = new_tags
            tag_ref = resource.create_tag_reference()
            if tag_ref:
                self._root.add(tag_ref)

    def _create_ownership_refs(self, session_ctx):
        for resource in _walk(self._root):
            if isinstance(resource, ResourcePointer):
                continue
            if hasattr(resource._data, "owner"):

                # Misconfigured resource, owner should always be a Role
                if isinstance(resource._data.owner, str):
                    raise RuntimeError(f"Owner of {resource} is a string, {resource._data.owner}")

                # Skip Snowflake-owned system resources (like INFORMATION_SCHEMA) that are owned by blank
                if resource._data.owner.name == "":
                    continue

                # Create the ownership ref
                resource.requires(resource._data.owner)

                # If the resource owner is a custom role, either it must already exist and be in available_roles
                # or we must plan to grant it to the current session
                if resource._data.owner.name not in session_ctx["available_roles"]:
                    for role_grant in self._root.items(ResourceType.ROLE_GRANT):
                        if role_grant.role.name != resource._data.owner.name:
                            continue
                        if role_grant._data.to_role is None:
                            continue
                        if role_grant.to.name not in session_ctx["available_roles"]:
                            continue
                        resource.requires(role_grant)
                        break
                    else:
                        raise InvalidOwnerException(
                            f"Blueprint resource {resource} owner {resource._data.owner} must be granted to the current session"
                        )

    def _create_grandparent_refs(self):
        for resource in _walk(self._root):
            if isinstance(resource.scope, SchemaScope):
                resource.requires(resource.container.container)

    def _finalize(self, session_ctx):
        if self._finalized:
            raise RuntimeError("Blueprint already finalized")
        self._finalized = True
        self._build_resource_graph(session_ctx)
        self._create_tag_references()
        self._create_ownership_refs(session_ctx)
        self._create_grandparent_refs()
        for resource in _walk(self._root):
            resource._finalized = True

    def generate_manifest(self, session_ctx: dict = {}) -> Manifest:
        manifest = Manifest(account_locator=session_ctx["account_locator"])
        self._finalize(session_ctx)
        for resource in _walk(self._root):
            if isinstance(resource, Resource):
                if resource.implicit:
                    continue
                manifest.add(resource)
            else:
                raise RuntimeError(f"Unexpected object found in blueprint: {resource}")

        return manifest

    def plan(self, session) -> Plan:
        reset_cache()
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
        self._raise_for_nonconforming_plan(finished_plan)
        return finished_plan

    def apply(self, session, plan: Plan = None):
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
                if not self._dry_run:
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

    # def destroy(self, session, manifest: Manifest = None):
    #     session_ctx = data_provider.fetch_session(session)
    #     manifest = manifest or self.generate_manifest(session_ctx)
    #     for urn, data in manifest.items():

    #         if isinstance(data, dict) and data.get("_pointer"):
    #             continue
    #         if urn.resource_type == ResourceType.GRANT:
    #             for grant in data:
    #                 execute(session, lifecycle.drop_resource(urn, grant))
    #         else:
    #             try:
    #                 execute(session, lifecycle.drop_resource(urn, data))
    #             except snowflake.connector.errors.ProgrammingError:
    #                 continue

    def _add(self, resource: Resource):
        if self._finalized:
            raise Exception("Cannot add resources to a finalized blueprint")
        if not isinstance(resource, Resource):
            raise Exception(f"Expected a Resource, got {type(resource)} -> {resource}")
        if resource._finalized:
            raise Exception("Cannot add a finalized resource to a blueprint")
        self._staged.append(resource)

    def add(self, *resources):
        if isinstance(resources[0], list):
            resources = resources[0]
        for resource in resources:
            self._add(resource)


def owner_for_change(change: ResourceChange):
    if change.action == Action.ADD and "owner" in change.after:
        return change.after["owner"]
    elif change.action == Action.CHANGE:
        # TRANSFER actions occur strictly after CHANGE actions, so we use the before owner
        # as the role for the change
        return change.before["owner"]
    elif change.action == Action.REMOVE and "owner" in change.before:
        return change.before["owner"]
    elif change.action == Action.TRANSFER and "owner" in change.before:
        return change.before["owner"]
    else:
        return None


def execution_strategy_for_change(
    change: ResourceChange,
    usable_roles: list[str],
    default_role: str,
    role_privileges: dict[str, list[dict]],
) -> tuple[str, bool]:

    if resource_type_is_grant(change.urn.resource_type):

        if change.action == Action.ADD and change.urn.resource_type == ResourceType.GRANT:
            execution_role = execution_role_for_priv(change.after["priv"])
            if execution_role and execution_role in usable_roles:
                return execution_role, False

        if "SECURITYADMIN" in usable_roles:
            return "SECURITYADMIN", False
        alternate_role = find_role_to_execute_change(change, usable_roles, default_role, role_privileges)
        if alternate_role:
            return alternate_role, False
        raise MissingPrivilegeException(f"{change} requires a role with MANAGE GRANTS privilege")

    elif change.urn.resource_type == ResourceType.TAG_REFERENCE:
        # There are two ways you can create a tag reference:
        # 1. You have the global APPLY TAGS priv on the account (given to ACCOUNTADMIN by default)
        # 2. You have APPLY privilege on the TAG object AND you have ownership of the tagged object
        if "ACCOUNTADMIN" in usable_roles:
            return "ACCOUNTADMIN", False
        alternate_role = find_role_to_execute_change(change, usable_roles, default_role, role_privileges)
        if alternate_role:
            return alternate_role, False
        raise MissingPrivilegeException(f"{change} requires a role with APPLY TAGS privilege")

    change_owner = owner_for_change(change)

    if change_owner:
        # If we can use the owner of the change, use that
        if change_owner in usable_roles and role_can_execute_change(change_owner, change, role_privileges):
            return change_owner, False

        # See if there is another role we can use to execute the change
        alternate_role = find_role_to_execute_change(change, usable_roles, default_role, role_privileges)
        if alternate_role:
            return alternate_role, True
        # This change cannot be executed
        raise MissingPrivilegeException(
            f"Owner {change_owner} does not have the required privileges to execute {change.action} on {change.urn}"
        )

    raise NotImplementedError(change)


# NOTE: this is in a hot loop and maybe should be cached
def role_can_execute_change(
    role: str,
    change: ResourceChange,
    role_privileges: dict[str, list[GrantedPrivilege]],
):

    # Assume ACCOUNTADMIN can do anything
    if role == "ACCOUNTADMIN":
        return True

    if role not in role_privileges:
        return False
    if len(role_privileges[role]) == 0:
        return False

    for granted_priv in role_privileges[role]:
        if granted_priv_allows_change(granted_priv, change):
            return True

    return False


def find_role_to_execute_change(
    change: ResourceChange,
    usable_roles: list[str],
    default_role: str,
    role_privileges: dict[str, list[GrantedPrivilege]],
):
    # Check default role first
    sorted_roles = sorted(usable_roles, key=lambda role: (role != default_role))
    for role in sorted_roles:
        if role_can_execute_change(role, change, role_privileges):
            return role
    return None


def granted_priv_allows_change(granted_priv: GrantedPrivilege, change: ResourceChange):

    # if change.action not in (Action.ADD,):
    #     raise NotImplementedError

    scope = RESOURCE_SCOPES[change.urn.resource_type]

    if isinstance(scope, AccountScope):
        container_name = str(change.urn.account_locator)
    elif isinstance(scope, DatabaseScope):
        container_name = str(change.urn.database().fqn)
    elif isinstance(scope, SchemaScope):
        container_name = str(change.urn.schema().fqn)
    else:
        raise Exception("Exception in granted_priv_allows_change, this should never be reached")

    resource_name = str(change.urn.fqn)

    if change.action == Action.ADD:

        # if resource is a grant, check for MANAGE GRANTS
        if resource_type_is_grant(change.urn.resource_type):
            if granted_priv.privilege == AccountPriv.MANAGE_GRANTS:
                return True

        # If resource is a tag reference, check for APPLY TAGS
        elif change.urn.resource_type == ResourceType.TAG_REFERENCE:
            if granted_priv.privilege == AccountPriv.APPLY_TAG:
                return True

        # If we own the resource container, we can always perform ADD
        if is_ownership_priv(granted_priv.privilege) and granted_priv.on == container_name:
            return True

        # If we don't own the container, we need the CREATE privilege for the resource on the container
        create_priv = None
        if change.urn.resource_type in CREATE_PRIV_FOR_RESOURCE_TYPE:
            create_priv = CREATE_PRIV_FOR_RESOURCE_TYPE[change.urn.resource_type]

        if granted_priv.privilege == create_priv and granted_priv.on == container_name:
            return True

        return False

    elif change.action == Action.CHANGE:

        # If we own the resource, we can always make changes
        if is_ownership_priv(granted_priv.privilege) and granted_priv.on == resource_name:
            return True

        # Some resources have a MODIFY privilege that typically allows changes
        if str(granted_priv.privilege) == "MODIFY" and granted_priv.on == resource_name:
            return True

        return False
    elif change.action == Action.REMOVE:
        if is_ownership_priv(granted_priv.privilege) and granted_priv.on == resource_name:
            return True
        return False
    elif change.action == Action.TRANSFER:
        # We must own the resource in order to transfer ownership
        if is_ownership_priv(granted_priv.privilege) and granted_priv.on == resource_name:
            return True
        return False


def sql_commands_for_change(
    change: ResourceChange,
    usable_roles: list[str],
    default_role: str,
    role_privileges: dict[str, list[GrantedPrivilege]],
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

    execution_role, transfer_owner = execution_strategy_for_change(change, usable_roles, default_role, role_privileges)
    before_change_cmd.append(f"USE ROLE {execution_role}")

    if change.action == Action.ADD:
        props = Resource.props_for_resource_type(change.urn.resource_type, change.after)
        change_cmd = lifecycle.create_resource(change.urn, change.after, props)
        if transfer_owner:
            after_change_cmd.append(
                lifecycle.transfer_resource(
                    change.urn,
                    owner=change.after["owner"],
                    copy_current_grants=True,
                )
            )
    elif change.action == Action.CHANGE:
        change_cmd = lifecycle.update_resource(change.urn, change.delta)
    elif change.action == Action.REMOVE:
        change_cmd = lifecycle.drop_resource(
            change.urn,
            change.before,
            if_exists=True,
        )
    elif change.action == Action.TRANSFER:
        change_cmd = lifecycle.transfer_resource(
            change.urn,
            owner=change.after["owner"],
            copy_current_grants=True,
        )

    return before_change_cmd + [change_cmd] + after_change_cmd


def compile_plan_to_sql(session_ctx, plan: Plan):
    sql_commands = []

    sql_commands.append("USE SECONDARY ROLES ALL")
    usable_roles = session_ctx["available_roles"]
    default_role = session_ctx["role"]
    role_privileges = session_ctx["role_privileges"]
    for change in plan:
        # Generate SQL commands
        commands = sql_commands_for_change(
            change,
            usable_roles,
            default_role,
            role_privileges,
        )
        sql_commands.extend(commands)

        # Update state
        if change.action == Action.ADD:
            if change.urn.resource_type == ResourceType.ROLE_GRANT:
                if change.after["to_role"] in usable_roles:
                    usable_roles.append(change.after["role"])
            elif change.urn.resource_type == ResourceType.GRANT:
                grantee_role = change.after["to"]
                if grantee_role not in role_privileges:
                    role_privileges[grantee_role] = []
                for priv in change.after["_privs"]:
                    if change.after["on_type"] is None:
                        print(change)
                        raise
                    role_privileges[grantee_role].append(
                        GrantedPrivilege.from_grant(
                            privilege=priv,
                            granted_on=change.after["on_type"],
                            name=change.after["on"],
                        )
                    )
            elif "owner" in change.after:
                # When creating any other resource type, creating implies ownership
                resource_priv_types = PRIVS_FOR_RESOURCE_TYPE[change.urn.resource_type]
                if resource_priv_types and "OWNERSHIP" in resource_priv_types:
                    ownership_priv = PRIVS_FOR_RESOURCE_TYPE[change.urn.resource_type]("OWNERSHIP")
                    owner_role = str(change.after["owner"])
                    if owner_role not in role_privileges:
                        role_privileges[owner_role] = []
                    role_privileges[owner_role].append(
                        GrantedPrivilege.from_grant(
                            privilege=ownership_priv,
                            granted_on=change.urn.resource_type,
                            name=str(change.urn.fqn),
                        )
                    )
                # Special case for databases: if you create a database you own the public schema
                if change.urn.resource_type == ResourceType.DATABASE:
                    role_privileges[owner_role].append(
                        GrantedPrivilege.from_grant(
                            privilege=PRIVS_FOR_RESOURCE_TYPE[ResourceType.SCHEMA]("OWNERSHIP"),
                            granted_on=ResourceType.SCHEMA,
                            name=str(change.urn.fqn) + ".PUBLIC",
                        )
                    )
    return sql_commands


def topological_sort(resource_set: set, references: set):
    # Kahn's algorithm

    # Compute in-degree (# of inbound edges) for each node
    in_degrees = {}
    outgoing_edges = {}

    for node in resource_set:
        in_degrees[node] = 0
        outgoing_edges[node] = set()

    for node, ref in references:
        in_degrees[ref] += 1
        outgoing_edges[node].add(ref)

    # Put all nodes with 0 in-degree in a queue
    queue = Queue()
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
