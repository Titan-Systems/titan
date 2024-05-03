from dataclasses import dataclass
from typing import List, Optional, Union
from queue import Queue

import snowflake.connector

from . import data_provider, lifecycle
from .client import ALREADY_EXISTS_ERR, INVALID_GRANT_ERR, execute, reset_cache
from .diff import diff, DiffAction
from .enums import ResourceType, ParseableEnum
from .logical_grant import And, LogicalGrant, Or
from .identifiers import URN, FQN
from .parse import parse_URN
from .privs import (
    CREATE_PRIV_FOR_RESOURCE_TYPE,
    GlobalPriv,
    DatabasePriv,
    RolePriv,
    SchemaPriv,
    priv_for_principal,
    is_ownership_priv,
)
from .resources import Account, Database, Schema
from .resources.resource import Resource, ResourceContainer, ResourcePointer, convert_to_resource
from .scope import AccountScope, DatabaseScope, OrganizationScope, SchemaScope

Manifest = dict
State = dict


class MissingPrivilegeException(Exception):
    pass


class MissingResourceException(Exception):
    pass


class RunMode(ParseableEnum):
    CREATE_OR_UPDATE = "CREATE-OR-UPDATE"
    FULLY_MANAGED = "FULLY-MANAGED"


@dataclass
class ResourceChange:
    action: DiffAction
    urn: URN
    before: dict
    after: dict
    delta: dict


Plan = list[ResourceChange]


def print_plan(plan: Plan):
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
    for change in plan:
        action_marker = ""
        if change.action == DiffAction.ADD:
            action_marker = "+"
        elif change.action == DiffAction.CHANGE:
            action_marker = "~"
        elif change.action == DiffAction.REMOVE:
            action_marker = "-"
        # »
        print(f"{action_marker} {change.urn}", "{")
        key_length = max(len(key) for key in change.delta.keys())
        for key, value in change.delta.items():
            print(f"  + {key:<{key_length}} = {value}")
        print("}")


def plan_sql(plan: Plan):
    """
    Generate SQL commands based on the plan provided.

    Args:
    plan (Plan): The plan containing changes to be applied to the database.

    Returns:
    List[str]: A list of SQL commands to be executed.
    """
    sql_commands = []
    for change in plan:
        props = Resource.props_for_resource_type(change.urn.resource_type, change.after)
        if change.action == DiffAction.ADD:
            sql_commands.append(lifecycle.create_resource(change.urn, change.after, props))
        elif change.action == DiffAction.CHANGE:
            sql_commands.append(lifecycle.update_resource(change.urn, change.delta, props))
        elif change.action == DiffAction.REMOVE:
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

    def route(resource: Resource):
        """The sorting hat"""
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

    for resource in resources:
        route(resource)
        if isinstance(resource, ResourceContainer):
            for item in resource.items():
                route(item)
    return org_scoped, acct_scoped, db_scoped, schema_scoped


def _walk(resource: Resource):
    yield resource
    if isinstance(resource, ResourceContainer):
        for item in resource.items():
            yield from _walk(item)


def _collect_required_privs(session_ctx: dict, plan: Plan) -> list:
    """
    For each action in the plan, generate a
    """
    required_priv_list = []

    account_urn = URN.from_session_ctx(session_ctx)

    for change in plan:
        # urn = parse_URN(urn_str)
        resource_cls = Resource.resolve_resource_cls(change.urn.resource_type, change.after)
        # privs = []
        privs = And()
        if change.action == DiffAction.ADD:
            # Special cases

            # GRANT ROLE
            # For example, to create a RoleGrant you need OWNERSHIP on the role.
            if change.urn.resource_type == ResourceType.ROLE_GRANT:
                role_urn = URN(
                    resource_type=ResourceType.ROLE,
                    fqn=FQN(change.urn.fqn.name),
                    account_locator=session_ctx["account_locator"],
                )
                privs = privs & (
                    LogicalGrant(role_urn, RolePriv.OWNERSHIP) | LogicalGrant(account_urn, GlobalPriv.MANAGE_GRANTS)
                )

            if isinstance(resource_cls.scope, DatabaseScope):
                privs = privs & (
                    LogicalGrant(change.urn.database(), DatabasePriv.USAGE)
                    | LogicalGrant(change.urn.database(), DatabasePriv.OWNERSHIP)
                )
            elif isinstance(resource_cls.scope, SchemaScope):
                privs = (
                    privs
                    & (
                        LogicalGrant(change.urn.database(), DatabasePriv.USAGE)
                        | LogicalGrant(change.urn.database(), DatabasePriv.OWNERSHIP)
                    )
                    & (
                        LogicalGrant(change.urn.schema(), SchemaPriv.USAGE)
                        | LogicalGrant(change.urn.schema(), SchemaPriv.OWNERSHIP)
                    )
                )

            create_priv = CREATE_PRIV_FOR_RESOURCE_TYPE.get(change.urn.resource_type)
            if create_priv:
                if isinstance(create_priv, GlobalPriv):
                    principal = account_urn
                elif isinstance(create_priv, DatabasePriv):
                    principal = change.urn.database()
                elif isinstance(create_priv, SchemaPriv):
                    principal = change.urn.schema()
                else:
                    raise Exception(f"Unsupported privilege type {type(create_priv)}")
                privs = privs & LogicalGrant(principal, create_priv)

        required_priv_list.append(privs)

    return required_priv_list


def _collect_available_privs(session_ctx: dict, session, plan: Plan, usable_roles: list[str]) -> dict:
    """
    The `priv_map` dictionary structure:
    {
        "SOME_ROLE": {
            "urn::ABC123:database/SOMEDB": {
                privilege1,
                privilege2,
                ...
            },
            ...
        },
        ...
    }
    """
    priv_map = {}

    def _add(role, principal, priv):
        if role not in priv_map:
            priv_map[role] = {}
        if principal not in priv_map[role]:
            priv_map[role][principal] = set()
        priv_map[role][principal].add(priv)

    def _contains(role, principal, priv):
        if role not in priv_map:
            return False
        if principal not in priv_map[role]:
            return False
        return priv in priv_map[role][principal]

    account_urn = URN.from_session_ctx(session_ctx)

    for role in usable_roles:
        priv_map[role] = {}

        if role.startswith("SNOWFLAKE.LOCAL"):
            continue

        # Existing privilege grants
        role_grants = data_provider.fetch_role_grants(session, role)
        if role_grants:
            for principal, grant_list in role_grants.items():
                for grant in grant_list:
                    try:
                        priv = priv_for_principal(parse_URN(principal), grant["priv"])
                        _add(role, principal, priv)
                    except ValueError:
                        # Priv not recognized by Titan
                        pass

        # Implied privilege grants in the context of our plan
        for change in plan:
            # urn = parse_URN(urn_str)

            # If we plan to add a new resource and we have the privs to create it, we can assume
            # that we have the OWNERSHIP priv on that resource
            if change.action == DiffAction.ADD:
                resource_cls = Resource.resolve_resource_cls(change.urn.resource_type, change.after)
                create_priv = CREATE_PRIV_FOR_RESOURCE_TYPE.get(change.urn.resource_type)
                if create_priv is None:
                    continue

                if isinstance(resource_cls.scope, AccountScope):
                    parent_urn = account_urn
                elif isinstance(resource_cls.scope, DatabaseScope):
                    parent_urn = change.urn.database()
                elif isinstance(resource_cls.scope, SchemaScope):
                    parent_urn = change.urn.schema()
                else:
                    raise Exception(f"Unsupported resource type {type(resource_cls)}")
                if _contains(role, str(parent_urn), create_priv):
                    ownership_priv = priv_for_principal(change.urn, "OWNERSHIP")
                    _add(role, str(parent_urn), ownership_priv)
                    if change.urn.resource_type == ResourceType.DATABASE and change.urn.fqn.name != "SNOWFLAKE":
                        public_schema = URN(
                            account_locator=account_urn.account_locator,
                            resource_type=ResourceType.SCHEMA,
                            fqn=FQN(name="PUBLIC", database=change.urn.fqn.name),
                        )
                        information_schema = URN(
                            account_locator=account_urn.account_locator,
                            resource_type=ResourceType.SCHEMA,
                            fqn=FQN(name="PUBLIC", database=change.urn.fqn.name),
                        )
                        _add(role, str(public_schema), priv_for_principal(public_schema, "OWNERSHIP"))
                        _add(role, str(information_schema), priv_for_principal(information_schema, "OWNERSHIP"))

    return priv_map


# TODO
def _raise_if_missing_privs(required: list, available: dict):
    return
    missing = []
    for expr in required:
        pass

    if missing:
        raise MissingPrivilegeException(f"Missing privileges")  #  for {principal}: {required_privs}

    # for principal, privs in required.items():
    #     required_privs = privs.copy()
    #     for priv_map in available.values():
    #         if principal in priv_map:
    #             # If OWNERSHIP in priv_map[principal], we can assume we pass requirements
    #             for priv in priv_map[principal]:
    #                 if is_ownership_priv(priv):
    #                     required_privs = set()
    #                     break
    #             required_privs -= priv_map[principal]
    #     if required_privs:
    #         raise MissingPrivilegeException(f"Missing privileges for {principal}: {required_privs}")


def _fetch_remote_state(session, manifest: Manifest) -> State:
    state: State = {}
    urns = set(manifest["_urns"].copy())

    # FIXME
    session.cursor().execute("USE ROLE ACCOUNTADMIN")

    for urn_str, _data in manifest.items():
        if urn_str.startswith("_"):
            continue
        urns.remove(urn_str)
        urn = parse_URN(urn_str)
        data = data_provider.fetch_resource(session, urn)
        if urn_str in manifest and data is not None:
            resource_cls = Resource.resolve_resource_cls(urn.resource_type, data)
            if urn.resource_type == ResourceType.FUTURE_GRANT:
                normalized = data
            elif isinstance(data, list):
                normalized = [resource_cls.defaults() | d for d in data]
            else:
                normalized = resource_cls.defaults() | data
            state[urn_str] = normalized

    for urn_str in urns:
        urn = parse_URN(urn_str)
        resource_cls = Resource.resolve_resource_cls(urn.resource_type)
        data = data_provider.fetch_resource(session, urn)
        if data is not None:
            if urn.resource_type == ResourceType.FUTURE_GRANT:
                normalized = data
            elif isinstance(data, list):
                normalized = [resource_cls.defaults() | d for d in data]
            else:
                normalized = resource_cls.defaults() | data
            state[urn_str] = normalized

    # check for existence of resource references
    for parent, reference in manifest["_refs"]:
        urn = parse_URN(reference)
        resource_cls = Resource.resolve_resource_cls(urn.resource_type)
        data = data_provider.fetch_resource(session, urn)
        if data is None:
            raise MissingResourceException(f"Resource {urn} required by {parent} not found")

    return state


class Blueprint:
    def __init__(
        self,
        name: str = None,
        account: Union[None, str, Account] = None,
        database: Union[None, str, Database] = None,
        schema: Union[None, str, Schema] = None,
        resources: List[Resource] = [],
        run_mode: RunMode = RunMode.CREATE_OR_UPDATE,
        dry_run: bool = False,
        allow_role_switching: bool = True,
        ignore_ownership: bool = True,
        valid_resource_types: List[ResourceType] = [],
    ) -> None:
        # TODO: input validation

        self._finalized = False
        self._staged: List[Resource] = []
        self._root: Account = None
        self._account_locator: str = None
        self._run_mode: RunMode = run_mode
        self._dry_run: bool = dry_run
        self._allow_role_switching: bool = allow_role_switching
        self._ignore_ownership: bool = ignore_ownership
        self._valid_resource_types: List[ResourceType] = valid_resource_types

        self.name = name
        self.account: Optional[Account] = convert_to_resource(Account, account) if account else None
        self.database: Optional[Database] = convert_to_resource(Database, database) if database else None
        self.schema: Optional[Schema] = convert_to_resource(Schema, schema) if schema else None

        if self.account and self.database:
            self.account.add(self.database)

        if self.database and self.schema:
            self.database.add(self.schema)

        self.add(resources or [])
        self.add([res for res in [self.account, self.database, self.schema] if res is not None])

    def _raise_for_nonconforming_plan(self, plan: Plan):
        exceptions = []

        # Run Mode exceptions
        if self._run_mode == RunMode.FULLY_MANAGED:
            return
        elif self._run_mode == RunMode.CREATE_OR_UPDATE:
            for change in plan:
                if change.action == DiffAction.REMOVE:
                    exceptions.append(
                        f"Create-or-update mode does not allow resources to be removed (ref: {change.urn})"
                    )
                if change.action == DiffAction.CHANGE:
                    if "owner" in change.delta:
                        change_debug = f"{change.before['owner']} => {change.delta['owner']}"
                        exceptions.append(
                            f"Create-or-update mode does not allow ownership changes (resource: {change.urn}, owner: {change_debug})"
                        )
                    elif "name" in change.delta:
                        exceptions.append(
                            f"Create-or-update mode does not allow renaming resources (ref: {change.urn})"
                        )
        else:
            raise Exception(f"Unsupported run mode {self._run_mode}")

        # Valid Resource Types exceptions
        if self._valid_resource_types:
            for change in plan:
                if change.urn.resource_type not in self._valid_resource_types:
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

        # Generate a list of all URNs
        resource_set = set(urns + list(remote_state.keys()))

        for ref in refs:
            resource_set.add(ref[0])
            resource_set.add(ref[1])

        # Calculate a topological sort order for the URNs
        sort_order = topological_sort(resource_set, refs)

        changes: Plan = []
        marked_for_replacement = set()
        for action, urn_str, delta in diff(remote_state, manifest):
            urn = parse_URN(urn_str)
            before = remote_state.get(urn_str, {})
            after = manifest.get(urn_str, {})

            # if urn.resource_type == ResourceType.FUTURE_GRANT and action in (DiffAction.ADD, DiffAction.CHANGE):
            #     for on_type, privs in data.items():
            #         privs_to_add = privs
            #         if action == DiffAction.CHANGE:
            #             privs_to_add = [priv for priv in privs if priv not in remote_state[urn_str].get(on_type, [])]
            #         for priv in privs_to_add:
            #             changes.append(ResourceChange(action=DiffAction.ADD, urn=urn_str, before={}, after={}, delta={on_type: [priv]}))
            if action == DiffAction.CHANGE:
                if urn in marked_for_replacement:
                    continue

                resource_cls = Resource.resolve_resource_cls(urn.resource_type, before)

                # TODO: if the attr is marked as must_replace, then instead we yield a rename, add, remove
                attr = list(delta.keys())[0]
                attr_metadata = resource_cls.spec.get_metadata(attr)
                if attr_metadata.get("triggers_replacement", False):
                    marked_for_replacement.add(urn)
                elif attr_metadata.get("fetchable", True) is False:
                    # drift on fields that aren't fetchable should be ignored
                    # TODO: throw a warning, or have a blueprint runmode that fails on this
                    continue
                elif attr == "owner" and self._ignore_ownership:
                    continue
                else:
                    changes.append(ResourceChange(action, urn, before, after, delta))
            elif action == DiffAction.ADD:
                changes.append(ResourceChange(action=action, urn=urn, before={}, after=after, delta=delta))
            elif action == DiffAction.REMOVE:
                changes.append(ResourceChange(action=action, urn=urn, before=before, after={}, delta={}))

        for urn in marked_for_replacement:
            changes.append(ResourceChange(action=DiffAction.REMOVE, urn=urn, before=before, after={}, delta={}))
            changes.append(ResourceChange(action=DiffAction.ADD, urn=urn, before={}, after=after, delta=after))

        return sorted(changes, key=lambda change: sort_order[str(change.urn)])

    def _finalize(self, session_context: dict):
        """
        Convert the staged resources into a tree of resources
        """
        if self._finalized:
            return
        self._finalized = True

        org_scoped, acct_scoped, db_scoped, schema_scoped = _split_by_scope(self._staged)

        if len(org_scoped) > 1:
            raise Exception("Only one account allowed")
        elif len(org_scoped) == 1:
            # If we have a staged account, use it
            self._root = org_scoped[0]
        else:
            # Otherwise, create a stub account from the session context
            self._root = ResourcePointer(name=session_context["account"], resource_type=ResourceType.ACCOUNT)
            self._account_locator = session_context["account_locator"]
            self.account = self._root

        # Add all databases and other account scoped resources to the root
        for resource in acct_scoped:
            self._root.add(resource)

        # If we haven't specified a database, use the one from the session context
        if self.database is None and session_context.get("database") is not None:
            existing_databases = [db.name for db in self._root.items(resource_type=ResourceType.DATABASE)]
            if session_context["database"] not in existing_databases:
                self._root.add(ResourcePointer(name=session_context["database"], resource_type=ResourceType.DATABASE))

        databases: list[Database] = self._root.items(resource_type=ResourceType.DATABASE)

        # Add all schemas and database roles to their respective databases
        for resource in db_scoped:
            if resource.container is None:
                if len(databases) == 1:
                    databases[0].add(resource)
                else:
                    raise Exception(f"Database [{resource.container}] for resource {resource} not found")

        for resource in schema_scoped:
            if resource.container is None:
                if len(databases) == 1:
                    public_schema: Schema = databases[0].find(resource_type=ResourceType.SCHEMA, name="PUBLIC")
                    if public_schema:
                        public_schema.add(resource)
                else:
                    raise Exception(f"No schema for resource {repr(resource)} found")
            elif isinstance(resource.container, ResourcePointer):
                # TODO: clean this up
                found = False
                for db in databases:
                    for schema in db.items(resource_type=ResourceType.SCHEMA):
                        if schema.name == resource.container.name:
                            schema.add(resource)
                            found = True
                            break
                    if found:
                        break
                if not found:
                    raise Exception(f"Schema [{resource.container}] for resource {resource} not found")

    def generate_manifest(self, session_context: dict = {}) -> Manifest:
        manifest: Manifest = {}
        refs = []
        urns = []

        self._finalize(session_context)

        for resource in _walk(self._root):
            if isinstance(resource, Resource) and resource.implicit:
                continue

            urn = URN(
                resource_type=resource.resource_type,
                fqn=resource.fqn,
                account_locator=self._account_locator,
            )

            data = resource.to_dict()

            if isinstance(resource, ResourcePointer):
                data["_pointer"] = True

            manifest_key = str(urn)

            #### Special Cases
            if resource.resource_type == ResourceType.FUTURE_GRANT:
                # Role up FUTURE GRANTS on the same role/target to a single entry
                # TODO: support grant option, use a single character prefix on the priv
                if manifest_key not in manifest:
                    manifest[manifest_key] = {}
                on_type = data["on_type"].lower()
                if on_type not in manifest[manifest_key]:
                    manifest[manifest_key][on_type] = []
                if data["priv"] in manifest[manifest_key][on_type]:
                    # raise Exception(f"Duplicate resource {urn} with conflicting data")
                    continue
                manifest[manifest_key][on_type].append(data["priv"])

            #### Normal Case
            else:
                if manifest_key in manifest:
                    if data != manifest[manifest_key]:
                        # raise Exception(f"Duplicate resource {urn} with conflicting data")
                        continue
                manifest[manifest_key] = data

            urns.append(manifest_key)

            for ref in resource.refs:
                ref_urn = URN.from_resource(account_locator=self._account_locator, resource=ref)
                refs.append((str(urn), str(ref_urn)))
        manifest["_refs"] = refs
        manifest["_urns"] = urns
        return manifest

    def plan(self, session) -> Plan:
        reset_cache()
        session_ctx = data_provider.fetch_session(session)
        manifest = self.generate_manifest(session_ctx)
        remote_state = _fetch_remote_state(session, manifest)
        try:
            completed_plan = self._plan(remote_state, manifest)
        except Exception as e:
            print("~" * 80, "REMOTE STATE")
            print(remote_state)
            print("~" * 80, "MANIFEST")
            print(manifest)

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
        usable_roles = session_ctx["available_roles"] if self._allow_role_switching else [session_ctx["role"]]
        required_privs = _collect_required_privs(session_ctx, plan)
        available_privs = _collect_available_privs(session_ctx, session, plan, usable_roles)

        _raise_if_missing_privs(required_privs, available_privs)

        action_queue = []
        actions_taken = []

        def _queue_action(change: ResourceChange, props):
            switch_to_role = None
            if "owner" in change.before:
                switch_to_role = change.before["owner"]
            elif change.urn.resource_type in (ResourceType.FUTURE_GRANT, ResourceType.ROLE_GRANT):
                switch_to_role = "SECURITYADMIN"
            if switch_to_role and switch_to_role in usable_roles:
                action_queue.append(f"USE ROLE {switch_to_role}")
            else:
                # raise Exception(f"Role {data.get('owner', '[OWNER MISSING]')} required for {urn} but isn't available")
                print(
                    f"Role {change.before.get('owner', '[OWNER MISSING]')} required for {change.urn} but isn't available"
                )
            if change.action == DiffAction.ADD:
                action_queue.append(lifecycle.create_resource(change.urn, change.after, props))
            elif change.action == DiffAction.CHANGE:
                action_queue.append(lifecycle.update_resource(change.urn, change.delta, props))
            elif change.action == DiffAction.REMOVE:
                action_queue.append(lifecycle.drop_resource(change.urn, change.before))

        for change in plan:
            props = Resource.props_for_resource_type(change.urn.resource_type, change.after)
            _queue_action(change, props)

        while action_queue:
            sql = action_queue.pop(0)
            actions_taken.append(sql)
            try:
                if not self._dry_run:
                    execute(session, sql)
            except snowflake.connector.errors.ProgrammingError as err:
                if err.errno == ALREADY_EXISTS_ERR:
                    print(f"Resource already exists: {change.urn}, skipping...")
                elif err.errno == INVALID_GRANT_ERR:
                    print(f"Invalid grant: {change.urn}, skipping...")
                else:
                    raise err
        return actions_taken

    def destroy(self, session, manifest=None):
        session_ctx = data_provider.fetch_session(session)
        manifest = manifest or self.generate_manifest(session_ctx)
        for urn_str, data in manifest.items():
            if urn_str.startswith("_"):
                continue

            if isinstance(data, dict) and data.get("_pointer"):
                continue
            urn = parse_URN(urn_str)
            if urn.resource_type == ResourceType.GRANT:
                for grant in data:
                    execute(session, lifecycle.drop_resource(urn, grant))
            else:
                try:
                    execute(session, lifecycle.drop_resource(urn, data))
                except snowflake.connector.errors.ProgrammingError as err:
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


def topological_sort(resource_set: set, references: list):
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
    return {value: index for index, value in enumerate(nodes)}
