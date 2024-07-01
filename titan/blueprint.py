import json
import logging
from dataclasses import dataclass
from queue import Queue
from typing import Optional

import snowflake.connector

from . import data_provider, lifecycle
from .builtins import SYSTEM_ROLES
from .client import (
    ALREADY_EXISTS_ERR,
    DOES_NOT_EXIST_ERR,
    INVALID_GRANT_ERR,
    execute,
    reset_cache,
)
from .diff import Action, diff
from .enums import ParseableEnum, ResourceType
from .identifiers import URN, resource_label_for_type
from .resource_name import ResourceName
from .resources import Account, Database, Schema
from .resources.tag import TaggableResource
from .resources.resource import Resource, ResourceContainer, ResourcePointer
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


class MarkedForReplacementException(Exception):
    pass


class ResourceInsertionException(Exception):
    pass


class OrphanResourceException(Exception):
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


Manifest = dict[URN, dict]
State = dict[URN, dict]
Plan = list[ResourceChange]


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
        remove_count = len([change for change in plan if change.action == Action.REMOVE])

        output += "\n» titan core\n"
        output += f"» Plan: {add_count} to add, {change_count} to change, {remove_count} to destroy.\n\n"

        for change in plan:
            action_marker = ""
            if change.action == Action.ADD:
                action_marker = "+"
            elif change.action == Action.CHANGE:
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


def plan_sql(plan: Plan) -> list[str]:
    sql_commands = []
    for change in plan:
        props = Resource.props_for_resource_type(change.urn.resource_type, change.after)
        if change.action == Action.ADD:
            sql_commands.append(lifecycle.create_resource(change.urn, change.after, props))
        elif change.action == Action.CHANGE:
            sql_commands.append(lifecycle.update_resource(change.urn, change.delta))
        elif change.action == Action.REMOVE:
            sql_commands.append(lifecycle.drop_resource(change.urn, change.before))
    return sql_commands


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


def _walk(resource: Resource):
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
            logger.warning("ignore_ownership is deprecated")
        if valid_resource_types is not None:
            logger.warning("valid_resource_types is deprecated")
            allowlist = valid_resource_types

        self._finalized = False
        self._staged: list[Resource] = []
        self._root: Account = None
        self._account_locator: str = None
        self._run_mode: RunMode = RunMode(run_mode)
        self._dry_run: bool = dry_run
        self._ignore_ownership: bool = ignore_ownership
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

        if ResourceType.USER in self._allowlist and self._run_mode != RunMode.CREATE_OR_UPDATE:
            raise Exception("User resource type is not allowed in this version of Titan")

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
                    if "owner" in change.delta:
                        change_debug = f"{change.before['owner']} => {change.delta['owner']}"
                        exceptions.append(
                            f"Create-or-update mode does not allow ownership changes (resource: {change.urn}, owner: {change_debug})"
                        )
                    elif "name" in change.delta:
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
            raise Exception("Non-conforming actions found in plan:\n" + exception_block)

    def _plan(self, remote_state: State, manifest: Manifest) -> Plan:
        manifest = manifest.copy()
        refs = manifest.pop("_refs")
        urns = manifest.pop("_urns")

        changes: Plan = []
        marked_for_replacement = set()
        for action, urn, delta in diff(remote_state, manifest):
            before = remote_state.get(urn, {})
            after = manifest.get(urn, {})

            if action == Action.CHANGE:
                if urn in marked_for_replacement:
                    continue

                resource_cls = Resource.resolve_resource_cls(urn.resource_type, before)

                # TODO: if the attr is marked as must_replace, then instead we yield a rename, add, remove
                attr = list(delta.keys())[0]
                attr_metadata = resource_cls.spec.get_metadata(attr)
                if attr_metadata.get("triggers_replacement", False):
                    raise MarkedForReplacementException(f"Resource {urn} is marked for replacement", resource_cls, attr)
                    marked_for_replacement.add(urn)
                elif attr_metadata.get("forces_add", False):
                    changes.append(ResourceChange(action=Action.ADD, urn=urn, before={}, after=after, delta=delta))
                    continue
                elif attr_metadata.get("fetchable", True) is False:
                    # drift on fields that aren't fetchable should be ignored
                    # TODO: throw a warning, or have a blueprint runmode that fails on this
                    continue
                elif attr == "owner" and self._ignore_ownership:
                    continue
                else:
                    changes.append(ResourceChange(action, urn, before, after, delta))
            elif action == Action.ADD:
                changes.append(ResourceChange(action=action, urn=urn, before={}, after=after, delta=delta))
            elif action == Action.REMOVE:
                changes.append(ResourceChange(action=action, urn=urn, before=before, after={}, delta={}))

        for urn in marked_for_replacement:
            raise MarkedForReplacementException(f"Resource {urn} is marked for replacement")
            # changes.append(ResourceChange(action=Action.REMOVE, urn=urn, before=before, after={}, delta={}))
            # changes.append(ResourceChange(action=Action.ADD, urn=urn, before={}, after=after, delta=after))

        # # Handle account role ownership by creating an ownership grant
        # for change in changes:
        #     if change.action == Action.ADD and change.after.get("owner") in SYSTEM_ROLES:
        #         changes.append(ResourceChange(action=Action.ADD, urn=change.urn, before={}, after=after, delta=after))

        # Generate a list of all URNs
        resource_set = set(urns + list(remote_state.keys()))
        for ref in refs:
            resource_set.add(ref[0])
            resource_set.add(ref[1])
        # Calculate a topological sort order for the URNs
        sort_order = topological_sort(resource_set, set(refs))
        return sorted(changes, key=lambda change: sort_order[change.urn])

    def fetch_remote_state(self, session, manifest: Manifest) -> State:
        state: State = {}

        manifest_urns: set[URN] = set(manifest["_urns"].copy())

        def _normalize(urn: URN, data: dict) -> dict:
            resource_cls = Resource.resolve_resource_cls(urn.resource_type, data)
            if urn.resource_type == ResourceType.FUTURE_GRANT:
                normalized = data
            elif isinstance(data, list):
                raise Exception(f"Fetching list of {urn.resource_type} is not supported yet")
            else:
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
                    urn = URN(resource_type=resource_type, fqn=fqn, account_locator=self._account_locator)
                    data = data_provider.fetch_resource(session, urn)
                    if data is not None:
                        normalized_data = _normalize(urn, data)
                        state[urn] = normalized_data

        for urn in manifest.keys():
            if str(urn).startswith("_"):
                continue

            manifest_urns.remove(urn)
            data = data_provider.fetch_resource(session, urn)
            if data is not None:
                normalized_data = _normalize(urn, data)
                state[urn] = normalized_data

        # check for existence of resource refs
        for parent, reference in manifest["_refs"]:
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
                logger.error(manifest)
                raise MissingResourceException(
                    f"Resource {reference} required by {parent} not found or failed to fetch"
                )

        return state

    def _finalize(self, session_context: dict):
        """
        Convert the staged resources into a tree of resources
        """
        if self._finalized:
            return
        self._finalized = True

        org_scoped, acct_scoped, db_scoped, schema_scoped = _split_by_scope(self._staged)
        self._staged = None

        if len(org_scoped) > 0:
            raise Exception("Blueprint cannot contain an Account resource")
        else:
            # Create a stub account from the session context
            self._root = ResourcePointer(name=session_context["account"], resource_type=ResourceType.ACCOUNT)
            self._account_locator = session_context["account_locator"]

        acct_scoped = _merge_pointers(acct_scoped)
        # Add all databases and other account scoped resources to the root
        for resource in acct_scoped:
            self._root.add(resource)

        databases: list[Database] = self._root.items(resource_type=ResourceType.DATABASE)

        # If we haven't specified a database, use the one from the session context
        if len(databases) == 0 and (len(db_scoped) + len(schema_scoped) > 0):
            if session_context.get("database") is None:
                raise OrphanResourceException(
                    "Blueprint is missing a database but includes resources that require a database or schema"
                )

            self._root.add(ResourcePointer(name=session_context["database"], resource_type=ResourceType.DATABASE))
            databases = self._root.items(resource_type=ResourceType.DATABASE)

        # db_scoped = _merge_pointers(db_scoped)
        # Add all schemas and database roles to their respective databases
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
                    databases[0].public_schema.add(resource)
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
                if ref.container is None and isinstance(ref.scope, resource.scope.__class__):
                    raise Exception
                    resource.container.add(ref)

    def _create_tag_references(self):
        for resource in _walk(self._root):
            if isinstance(resource, TaggableResource):
                tag_ref = resource.create_tag_reference()
                if tag_ref:
                    self._root.add(tag_ref)

    def generate_manifest(self, session_context: dict = {}) -> Manifest:
        manifest: Manifest = {}
        refs = []
        urns = []

        self._finalize(session_context)

        self._create_tag_references()

        for resource in _walk(self._root):
            if isinstance(resource, Resource):
                if resource.implicit:  # and self._run_mode not in (RunMode.SYNC_ALL, RunMode.SYNC):
                    continue

            urn = URN(
                resource_type=resource.resource_type,
                fqn=resource.fqn,
                account_locator=self._account_locator,
            )

            data = resource.to_dict()

            if isinstance(resource, ResourcePointer):
                data["_pointer"] = True

            if urn in manifest:
                if data != manifest[urn]:
                    logger.warning(f"Duplicate resource {urn} with conflicting data, discarding {data}")
                    continue
            manifest[urn] = data

            urns.append(urn)

            for ref in resource.refs:
                ref_urn = URN.from_resource(account_locator=self._account_locator, resource=ref)
                refs.append((urn, ref_urn))
        manifest["_refs"] = refs
        manifest["_urns"] = urns
        return manifest

    def plan(self, session) -> Plan:
        reset_cache()
        session_ctx = data_provider.fetch_session(session)
        manifest = self.generate_manifest(session_ctx)
        remote_state = self.fetch_remote_state(session, manifest)
        try:
            completed_plan = self._plan(remote_state, manifest)
        except Exception as e:
            logger.error("~" * 80 + "REMOTE STATE")
            logger.error(remote_state)
            logger.error("~" * 80 + "MANIFEST")
            logger.error(manifest)

            raise e
        self._raise_for_nonconforming_plan(completed_plan)
        return completed_plan

    def apply(self, session, plan: Plan = None):
        if plan is None:
            plan = self.plan(session)

        # TODO: cursor setup, including query tag
        # TODO: clean up urn vs urn_str madness

        """
            At this point, we have a list of actions as a part of the plan. Each action is one of:
                1. [ADD] action (CREATE command)
                2. [CHANGE] action (one or many ALTER or SET PARAMETER commands)
                3. [REMOVE] action (DROP command, REVOKE command, or a rename operation)

            Each action requires:
                • a set of privileges necessary to run commands
                • the appropriate role to execute commands

            Once we've determined those things, we can compare the list of required roles and privileges
            against what we have access to in the session and the role tree.
        """

        session_ctx = data_provider.fetch_session(session)

        _raise_if_plan_would_drop_session_user(session_ctx, plan)

        # required_privs = _collect_required_privs(session_ctx, plan)
        # available_privs = _collect_available_privs(session_ctx, session, plan, usable_roles)
        # _raise_if_missing_privs(required_privs, available_privs)

        action_queue = self._compile_plan_to_sql(session_ctx, plan)
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
                else:
                    raise err
        return actions_taken

    def _compile_plan_to_sql(self, session_ctx, plan: Plan):
        action_queue = []
        default_role = session_ctx["role"]

        def _queue_change(change: ResourceChange):
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

            before_action = []
            action = None
            after_action = []
            if change.action == Action.ADD:
                props = Resource.props_for_resource_type(change.urn.resource_type, change.after)
                # TODO: raise exception if role isn't usable. Maybe can just let this fail naturally

                if "owner" in change.after:
                    if change.after["owner"] in SYSTEM_ROLES:
                        before_action.append(f"USE ROLE {change.after['owner']}")
                    else:
                        before_action.append(f"USE ROLE {default_role}")
                        after_action.append(
                            f"GRANT OWNERSHIP ON {change.urn.resource_type} {change.urn.fqn} TO {change.after['owner']}"
                        )
                elif change.urn.resource_type in (ResourceType.FUTURE_GRANT, ResourceType.ROLE_GRANT):
                    # TODO: switch to role with MANAGE GRANTS if we dont have access to SECURITYADMIN
                    before_action.append("USE ROLE SECURITYADMIN")
                else:
                    before_action.append(f"USE ROLE {default_role}")

                action = lifecycle.create_resource(change.urn, change.after, props)
            elif change.action == Action.CHANGE:
                action = lifecycle.update_resource(change.urn, change.delta)
            elif change.action == Action.REMOVE:
                if "owner" in change.before:
                    before_action.append(f"USE ROLE {change.before['owner']}")
                action = lifecycle.drop_resource(change.urn, change.before, if_exists=True)

            action_queue.extend(before_action)
            action_queue.append(action)
            action_queue.extend(after_action)

        action_queue.append("USE SECONDARY ROLES ALL")
        for change in plan:
            _queue_change(change)
        return action_queue

    def destroy(self, session, manifest: Manifest = None):
        session_ctx = data_provider.fetch_session(session)
        manifest = manifest or self.generate_manifest(session_ctx)
        for urn, data in manifest.items():
            if str(urn).startswith("_"):
                continue

            if isinstance(data, dict) and data.get("_pointer"):
                continue
            if urn.resource_type == ResourceType.GRANT:
                for grant in data:
                    execute(session, lifecycle.drop_resource(urn, grant))
            else:
                try:
                    execute(session, lifecycle.drop_resource(urn, data))
                except snowflake.connector.errors.ProgrammingError:
                    continue

    def _add(self, resource: Resource):
        if self._finalized:
            raise Exception("Cannot add resources to a finalized blueprint")
        if not isinstance(resource, Resource):
            raise Exception(f"Expected a Resource, got {type(resource)} -> {resource}")
        self._staged.append(resource)

    def add(self, *resources):
        if isinstance(resources[0], list):
            resources = resources[0]
        for resource in resources:
            self._add(resource)


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
