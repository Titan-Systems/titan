from dataclasses import dataclass
import sys
from typing import Any, Dict, List, Optional, Union

from inflection import pluralize

from titan.parse import parse_URN
from .builder import tidy_sql
from .enums import ResourceType
from .identifiers import URN
from .props import Props
from .enums import TaskState, ResourceType
from .diff import DiffAction, DictDiff

__this__ = sys.modules[__name__]

@dataclass(unsafe_hash=True)
class ResourceChange:
    """
    Represents a required change for a Snowflake resource.
    action: The type of change (add, remove, change)
    urn: The resource URN
    old_value: The old value (or None if the change is an add)
    new_value: The new value (or None if the change is a remove)
    """
    action: DiffAction
    urn: URN
    old_value: Dict[str,Any] = None
    new_value: Dict[str,Any] = None

    @classmethod
    def from_diff(cls, diff_obj: DictDiff):
        """
        Constructs a resource change from a DictDiff object.
        """
        return cls(
            action=diff_obj.action,
            urn=parse_URN(diff_obj.key),
            old_value=diff_obj.old_value,
            new_value=diff_obj.new_value,
        )




def create_resource(resource_change:ResourceChange, props: Props, if_not_exists: bool = False) -> List[str]:
    return getattr(__this__, f"create_{resource_change.urn.resource_label}", create__default)(resource_change, props, if_not_exists)


def create__default(resource_change:ResourceChange, props: Props, if_not_exists: bool = False) -> List[str]:
    if len(resource_change.new_value) == 0:
        return []
    return [tidy_sql(
        "CREATE",
        resource_change.urn.resource_type,
        "IF NOT EXISTS" if if_not_exists else "",
        resource_change.urn.fqn,
        props.render(resource_change.new_value),
    )]

def create_task(resource_change:ResourceChange, props: Props, if_not_exists: bool = False) -> List[str]:
    # we need to track the state of the task, but it's not a creatable property
    if 'state' in resource_change.new_value:
        resource_change.new_value.pop('state')
    return create__default(resource_change, props, if_not_exists)

def create_schema(resource_change:ResourceChange, props: Props, if_not_exists: bool = False) -> List[str]:
    if 'name' in resource_change.new_value and resource_change.new_value['name'] == 'PUBLIC':
        # these updates could be superfluous, since the original plan was to create
        return update_schema(resource_change, props)
    return create__default(resource_change, props, if_not_exists)


def create_function(resource_change:ResourceChange, props: Props, if_not_exists: bool = False) -> List[str]:
    db = f"{resource_change.urn.fqn.database}." if resource_change.urn.fqn.database else ""
    schema = f"{resource_change.urn.fqn.schema}." if resource_change.urn.fqn.schema else ""
    name = f"{db}{schema}{resource_change.new_value['name']}"
    return [tidy_sql(
        "CREATE",
        "IF NOT EXISTS" if if_not_exists else "",
        resource_change.urn.resource_type,
        name,
        props.render(resource_change.new_value),
    )]


def create_procedure(resource_change:ResourceChange, props: Props, if_not_exists: bool = False) -> List[str]:
    db = f"{resource_change.urn.fqn.database}." if resource_change.urn.fqn.database else ""
    schema = f"{resource_change.urn.fqn.schema}." if resource_change.urn.fqn.schema else ""
    name = f"{db}{schema}{resource_change.urn.fqn.name}"
    return [tidy_sql(
        "CREATE",
        "IF NOT EXISTS" if if_not_exists else "",
        resource_change.urn.resource_type,
        name,
        props.render(resource_change.new_value),
    )]


def create_future_grant(urn: URN, data: dict, props: Props, if_not_exists: bool):
    on_type = data["on_type"]
    if "INTEGRATION" in on_type:
        on_type = "INTEGRATION"
    return [tidy_sql(
        "GRANT",
        data["priv"],
        "ON FUTURE",
        pluralize(on_type).upper(),
        "IN",
        data["in_type"],
        data["in_name"],
        "TO ROLE",
        resource_change.urn.fqn.name,
        # props.render(data), #TODO grant option
    )]


def create_grant(resource_change:ResourceChange, props: Props, if_not_exists: bool) -> List[str]:
    on_type = resource_change.new_value["on_type"]
    if "INTEGRATION" in on_type:
        on_type = "INTEGRATION"
    return [tidy_sql(
        "GRANT",
        resource_change.new_value["priv"],
        "ON",
        on_type,
        resource_change.new_value["on"],
        props.render(resource_change.new_value),
    )]


def create_grant_on_all(resource_change:ResourceChange, props: Props, if_not_exists: bool) -> List[str]:
    return [tidy_sql(
        "GRANT",
        resource_change.new_value["priv"],
        "ON ALL",
        pluralize(resource_change.new_value["on_type"]),
        "IN",
        resource_change.new_value["in_type"],
        resource_change.new_value["in_name"],
        "TO ROLE",
        resource_change.new_value["to"],
    )]


def create_role_grant(resource_change:ResourceChange, props: Props, if_not_exists: bool) -> List[str]:
    return [tidy_sql(
        "GRANT",
        props.render(resource_change.new_value),
    )]


def create_view(resource_change:ResourceChange, props: Props, if_not_exists: bool = False) -> List[str]:
    data = resource_change.new_value.copy()
    secure = data.pop("secure", None)
    volatile = data.pop("volatile", None)
    recursive = data.pop("recursive", None)
    return [tidy_sql(
        "CREATE",
        "SECURE" if secure else "",
        "VOLATILE" if volatile else "",
        "RECURSIVE" if recursive else "",
        resource_change.urn.resource_type,
        "IF NOT EXISTS" if if_not_exists else "",
        resource_change.urn.fqn,
        props.render(data),
    )]


def update_resource(resource_change:ResourceChange, props: Props) -> List[str]:
    return getattr(__this__, f"update_{resource_change.urn.resource_label}", update__default)(resource_change, props)


def update_event_table(resource_change:ResourceChange, props: Props) -> List[str]:
    resource_change.urn.resource_type = "TABLE" # when event tables are being altered, they are just tables (Unsupported feature 'EVENT')
    return update__default(resource_change, props)

def update_stream(resource_change:ResourceChange, props: Props) -> List[str]:
    # not the best place to handle this, instead properties should be tagged such that a difference isn't generated
    if 'show_initial_rows' in resource_change.new_value:
        resource_change.new_value.pop('show_initial_rows')
    return update__default(resource_change, props)

def update_table(resource_change:ResourceChange, props: Props) -> List[str]:
    # not the best place to handle this, instead properties should be tagged such that a difference isn't generated
    if 'as_' in resource_change.new_value:
        resource_change.new_value.pop('as_')
    if 'columns' in resource_change.new_value and resource_change.new_value['columns'] is None:
        resource_change.new_value.pop('columns')
    return update__default(resource_change, props)

def update_replication_group(resource_change:ResourceChange, props: Props) -> List[str]:
    # replication groups require certain values to be added and removed rather than set
    # currently we can't do removals because we don't have that info here
    if 'allowed_accounts' in resource_change.new_value:
        old_value = [a.upper().strip() for a in resource_change.old_value['allowed_accounts']]
        new_value = [a.upper().strip() for a in resource_change.new_value['allowed_accounts']]
        accounts_to_remove = [account for account in old_value if account not in new_value]
        accounts_to_add = [account for account in new_value if account not in old_value]
        return_queries = []
        if len(accounts_to_add) > 0:
            return_queries.append(tidy_sql("ALTER",resource_change.urn.resource_type,resource_change.urn.fqn,"ADD",f"{', '.join(accounts_to_add)}","TO ALLOWED_ACCOUNTS"))
        if len(accounts_to_remove) > 0:
            return_queries.append(tidy_sql("ALTER",resource_change.urn.resource_type,resource_change.urn.fqn,"REMOVE",f"{', '.join(accounts_to_remove)}","FROM ALLOWED_ACCOUNTS"))
        return return_queries
    return update__default(resource_change, props)

def update__default(resource_change:ResourceChange, props: Props) -> List[str]:
    if len(resource_change.new_value) == 0:
        return None # allows for stripping out proposed changes, this will be skipped over
    attr, new_value = resource_change.new_value.popitem()
    attr = attr.lower()
    if str(attr).endswith('_'):
            # not sure why this isn't handled by the props alias
            attr = str(attr)[:-1]
    
    if new_value is None:
        return [tidy_sql("ALTER", resource_change.urn.resource_type, resource_change.urn.fqn, "UNSET", attr)]
    elif attr == "name":
        return [tidy_sql("ALTER", resource_change.urn.resource_type, resource_change.urn.fqn, "RENAME TO", new_value)]
    elif attr == "owner":
        return [tidy_sql("GRANT OWNERSHIP ON", resource_change.urn.resource_type, resource_change.urn.fqn, "TO ROLE", new_value)]
    elif attr in ['when','as'] and resource_change.urn.resource_type == ResourceType.TASK:
        # knowing whether to SET or MODIFY could be an attribute of the property.
        # In order to change a task definition, you have to suspend the task
        # otherwise, you get: Unable to update graph with root task <name> since that root task is not suspended
        return [
            tidy_sql("ALTER",resource_change.urn.resource_type,resource_change.urn.fqn,"SUSPEND"),
            tidy_sql("ALTER",resource_change.urn.resource_type,resource_change.urn.fqn,"MODIFY",attr,new_value),
            tidy_sql("ALTER",resource_change.urn.resource_type,resource_change.urn.fqn,"RESUME")
        ]
    elif attr in ['schedule'] and resource_change.urn.resource_type == ResourceType.TASK:
        return [
            tidy_sql("ALTER",resource_change.urn.resource_type,resource_change.urn.fqn,"SUSPEND"),
            tidy_sql("ALTER",resource_change.urn.resource_type,resource_change.urn.fqn,"SET",attr,"=",f"$${new_value}$$"), # more duplication here, needs a refactor
            tidy_sql("ALTER",resource_change.urn.resource_type,resource_change.urn.fqn,"RESUME")
        ]
    elif attr == 'state' and resource_change.urn.resource_type == ResourceType.TASK:
        if new_value not in [str(TaskState.STARTED),str(TaskState.SUSPENDED)]:
            raise ValueError(f"Invalid state '{new_value}' for task")
        return [tidy_sql("ALTER", 
                        resource_change.urn.resource_type,
                        resource_change.urn.fqn,
                        "SUSPEND" if new_value == TaskState.SUSPENDED else "RESUME")]
    else:
        if isinstance(new_value,list) and resource_change.urn.resource_type == ResourceType.REPLICATION_GROUP:
            # not sure if list parameters should universally be treated this way, restricting for now
            quoted_values = [f"$${v}$$" for v in new_value]
            new_value = f"({', '.join(quoted_values)})"
        else:
            # value serialization should really be driven by property type
            new_value = f"$${new_value}$$" if isinstance(new_value, str) else new_value

        return [tidy_sql(
            "ALTER",
            resource_change.urn.resource_type,
            resource_change.urn.fqn,
            "SET",
            attr,
            "=",
            new_value,
        )]


def update_event_table(urn: URN, data: dict, props: Props) -> str:
    new_urn = URN(ResourceType.TABLE, urn.fqn, urn.account_locator)
    return update__default(new_urn, data, props)


def update_procedure(urn: URN, data: dict, props: Props) -> str:
    if "execute_as" in data:
        return tidy_sql(
            "ALTER",
            resource_change.urn.resource_type,
            resource_change.urn.fqn,
            "EXECUTE AS",
            resource_change.new_value["execute_as"],
        )]
    else:
        return update__default(resource_change, props)


def update_role_grant(resource_change:ResourceChange, props: Props) -> List[str]:
    raise NotImplementedError


def update_schema(resource_change:ResourceChange, props: Props) -> List[str]:
    attr, new_value = resource_change.new_value.popitem()
    attr = attr.lower()
    if new_value is None:
        return [tidy_sql("ALTER SCHEMA", resource_change.urn.fqn, "UNSET", attr)]
    elif attr == "name":
        return [tidy_sql("ALTER SCHEMA", resource_change.urn.fqn, "RENAME TO", new_value)]
    elif attr == "owner":
        raise NotImplementedError(f"Cannot change owner of schema {resource_change.urn.fqn}, this is not supported yet")
    elif attr == "transient":
        raise Exception("Cannot change transient property of schema")
    elif attr == "managed_access":
        return [tidy_sql("ALTER SCHEMA", resource_change.urn.fqn, "ENABLE" if new_value else "DISABLE", "MANAGED ACCESS")]
    else:
        new_value = f"'{new_value}'" if isinstance(new_value, str) else new_value
        return [tidy_sql("ALTER SCHEMA", resource_change.urn.fqn, "SET", attr, "=", new_value)]


def drop_resource(resource_change:ResourceChange, if_exists: bool = False) -> List[str]:
    return getattr(__this__, f"drop_{resource_change.urn.resource_label}", drop__default)(resource_change, if_exists=if_exists)


def drop__default(resource_change:ResourceChange, if_exists: bool) -> List[str]:
    return [tidy_sql(
        "DROP",
        resource_change.urn.resource_type,
        "IF EXISTS" if if_exists else "",
        resource_change.urn.fqn,
    )]


def drop_function(resource_change:ResourceChange, if_exists: bool) -> List[str]:
    return [tidy_sql(
        "DROP",
        resource_change.urn.resource_type,
        "IF EXISTS" if if_exists else "",
        resource_change.urn.fqn,
    )]


def drop_future_grant(resource_change:ResourceChange, **kwargs) -> List[str]:
    return [tidy_sql(
        "REVOKE",
        resource_change.old_value["priv"],
        "ON FUTURE",
        pluralize(resource_change.old_value["on_type"]).upper(),
        "IN",
        resource_change.old_value["in_type"],
        resource_change.old_value["in_name"],
        "FROM",
        resource_change.old_value["to"],
        # props.render(data), #TODO grant option
    )]


def drop_grant(resource_change:ResourceChange, **kwargs) -> List[str]:
    # FIXME
    if resource_change.old_value["priv"] == "OWNERSHIP":
        return []
    return [tidy_sql(
        "REVOKE",
        resource_change.old_value["priv"],
        "ON",
        resource_change.old_value["on_type"],
        resource_change.old_value["on"],
        "FROM",
        resource_change.old_value["to"],
        # "CASCADE" if cascade else "RESTRICT",
    )]


def drop_grant_on_all(resource_change:ResourceChange, **kwargs) -> List[str]:
    return [tidy_sql(
        "REVOKE",
        resource_change.old_value["priv"],
        "ON ALL",
        resource_change.old_value["on_type"],
        "IN",
        resource_change.old_value["in_type"],
        resource_change.old_value["in_name"],
    )]


def drop_role_grant(resource_change:ResourceChange, **kwargs) -> List[str]:
    return [tidy_sql(
        "REVOKE ROLE",
        resource_change.old_value["role"],
        "FROM",
        "ROLE" if resource_change.old_value.get("to_role") else "USER",
        resource_change.old_value["to_role"] if resource_change.old_value.get("to_role") else resource_change.old_value["to_user"],
    )]
