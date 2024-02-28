import sys

from inflection import pluralize

from .builder import tidy_sql
from .identifiers import URN
from .props import Props
from .enums import TaskState, ResourceType

__this__ = sys.modules[__name__]


def create_resource(urn: URN, data: dict, props: Props, if_not_exists: bool = False) -> str:
    return getattr(__this__, f"create_{urn.resource_label}", create__default)(urn, data, props, if_not_exists)


def create__default(urn: URN, data: dict, props: Props, if_not_exists: bool = False) -> str:
    if len(data) == 0:
        return None # allows for stripping out proposed changes, this will be skipped over
    return tidy_sql(
        "CREATE",
        urn.resource_type,
        "IF NOT EXISTS" if if_not_exists else "",
        urn.fqn,
        props.render(data),
    )

def create_task(urn: URN, data: dict, props: Props, if_not_exists: bool = False) -> str:
    if 'state' in data:
        data.pop('state')
    return create__default(urn, data, props, if_not_exists)

def create_schema(urn: URN, data: dict, props: Props, if_not_exists: bool = False) -> str:
    if 'name' in data and data['name'] == 'PUBLIC':
        # these updates could be superfluous, since the original plan was to create
        return update_schema(urn, data, props)
    return create__default(urn, data, props, if_not_exists)


def create_function(urn: URN, data: dict, props: Props, if_not_exists: bool = False) -> str:
    db = f"{urn.fqn.database}." if urn.fqn.database else ""
    schema = f"{urn.fqn.schema}." if urn.fqn.schema else ""
    name = f"{db}{schema}{data['name']}"
    return tidy_sql(
        "CREATE",
        "IF NOT EXISTS" if if_not_exists else "",
        urn.resource_type,
        name,
        props.render(data),
    )


def create_procedure(urn: URN, data: dict, props: Props, if_not_exists: bool = False) -> str:
    db = f"{urn.fqn.database}." if urn.fqn.database else ""
    schema = f"{urn.fqn.schema}." if urn.fqn.schema else ""
    name = f"{db}{schema}{urn.fqn.name}"
    return tidy_sql(
        "CREATE",
        "IF NOT EXISTS" if if_not_exists else "",
        urn.resource_type,
        name,
        props.render(data),
    )


def create_future_grant(urn: URN, data: dict, props: Props, if_not_exists: bool):
    in_type, in_name = urn.fqn.params["in"].split("/")
    on_type, privs = list(data.items())[0]
    if "INTEGRATION" in on_type:
        on_type = "INTEGRATION"
    return tidy_sql(
        "GRANT",
        privs[0],
        "ON FUTURE",
        pluralize(on_type).upper(),
        "IN",
        in_type,
        in_name,
        "TO ROLE",
        urn.fqn.name,
        # props.render(data), #TODO grant option
    )


def create_grant(urn: URN, data: dict, props: Props, if_not_exists: bool):
    on_type = data["on_type"]
    if "INTEGRATION" in on_type:
        on_type = "INTEGRATION"
    return tidy_sql(
        "GRANT",
        data["priv"],
        "ON",
        on_type,
        data["on"],
        props.render(data),
    )


def create_grant_on_all(urn: URN, data: dict, props: Props, if_not_exists: bool):
    return tidy_sql(
        "GRANT",
        data["priv"],
        "ON ALL",
        pluralize(data["on_type"]),
        "IN",
        data["in_type"],
        data["in_name"],
        "TO ROLE",
        data["to"],
    )


def create_role_grant(urn: URN, data: dict, props: Props, if_not_exists: bool):
    return tidy_sql(
        "GRANT",
        props.render(data),
    )


def create_view(urn: URN, data: dict, props: Props, if_not_exists: bool = False) -> str:
    data = data.copy()
    secure = data.pop("secure", None)
    volatile = data.pop("volatile", None)
    recursive = data.pop("recursive", None)
    return tidy_sql(
        "CREATE",
        "SECURE" if secure else "",
        "VOLATILE" if volatile else "",
        "RECURSIVE" if recursive else "",
        urn.resource_type,
        "IF NOT EXISTS" if if_not_exists else "",
        urn.fqn,
        props.render(data),
    )


def update_resource(urn: URN, data: dict, props: Props) -> str:
    return getattr(__this__, f"update_{urn.resource_label}", update__default)(urn, data, props)


def update_event_table(urn: URN, data: dict, props: Props) -> str:
    urn.resource_type = "TABLE" # when event tables are being altered, they are just tables (Unsupported feature 'EVENT')
    return update__default(urn, data, props)

def update_stream(urn: URN, data: dict, props: Props) -> str:
    # not the best place to handle this, instead properties should be tagged such that a difference isn't generated
    if 'show_initial_rows' in data:
        data.pop('show_initial_rows')
    return update__default(urn, data, props)

def update_table(urn: URN, data: dict, props: Props) -> str:
    # not the best place to handle this, instead properties should be tagged such that a difference isn't generated
    if 'as_' in data:
        data.pop('as_')
    if 'columns' in data and data['columns']['new_value'] is None:
        data.pop('columns')
    return update__default(urn, data, props)

def update_replication_group(urn: URN, data: dict, props: Props) -> str:
    # replication groups require certain values to be added and removed rather than set
    # currently we can't do removals because we don't have that info here
    if 'allowed_accounts' in data:
        allowed_accounts_change = data.pop('allowed_accounts')
        old_value = [a.upper().strip() for a in allowed_accounts_change['old_value']]
        new_value = [a.upper().strip() for a in allowed_accounts_change['new_value']]
        accounts_to_remove = [account for account in old_value if account not in new_value]
        accounts_to_add = [account for account in new_value if account not in old_value]
        return_queries = []
        if len(accounts_to_add) > 0:
            return_queries.append(tidy_sql("ALTER",urn.resource_type,urn.fqn,"ADD",f"{', '.join(accounts_to_add)}","TO ALLOWED_ACCOUNTS"))
        if len(accounts_to_remove) > 0:
            return_queries.append(tidy_sql("ALTER",urn.resource_type,urn.fqn,"REMOVE",f"{', '.join(accounts_to_remove)}","FROM ALLOWED_ACCOUNTS"))
        return return_queries
    return update__default(urn, data, props)

def update__default(urn: URN, data: dict, props: Props) -> str:
    if len(data) == 0:
        return None # allows for stripping out proposed changes, this will be skipped over
    attr, change_dict = data.popitem()
    new_value = change_dict['new_value']
    attr = attr.lower()
    if str(attr).endswith('_'):
            # not sure why this isn't handled by the props alias
            attr = str(attr)[:-1]
    
    if new_value is None:
        return tidy_sql("ALTER", urn.resource_type, urn.fqn, "UNSET", attr)
    elif attr == "name":
        return tidy_sql("ALTER", urn.resource_type, urn.fqn, "RENAME TO", new_value)
    elif attr == "owner":
        return tidy_sql("GRANT OWNERSHIP ON", urn.resource_type, urn.fqn, "TO ROLE", new_value)
    elif attr in ['when','as'] and urn.resource_type == ResourceType.TASK:
        # knowing whether to SET or MODIFY could be an attribute of the property.
        # In order to change a task definition, you have to suspend the task
        # otherwise, you get: Unable to update graph with root task <name> since that root task is not suspended
        return [
            tidy_sql("ALTER",urn.resource_type,urn.fqn,"SUSPEND"),
            tidy_sql("ALTER",urn.resource_type,urn.fqn,"MODIFY",attr,new_value),
            tidy_sql("ALTER",urn.resource_type,urn.fqn,"RESUME")
        ]
    elif attr in ['schedule'] and urn.resource_type == ResourceType.TASK:
        return [
            tidy_sql("ALTER",urn.resource_type,urn.fqn,"SUSPEND"),
            tidy_sql("ALTER",urn.resource_type,urn.fqn,"SET",attr,"=",f"$${new_value}$$"), # more duplication here, needs a refactor
            tidy_sql("ALTER",urn.resource_type,urn.fqn,"RESUME")
        ]
    elif attr == 'state' and urn.resource_type == ResourceType.TASK:
        if new_value not in [str(TaskState.STARTED),str(TaskState.SUSPENDED)]:
            raise ValueError(f"Invalid state '{new_value}' for task")
        return tidy_sql("ALTER", 
                        urn.resource_type,
                        urn.fqn,
                        "SUSPEND" if new_value == TaskState.SUSPENDED else "RESUME")
    else:
        if isinstance(new_value,list) and urn.resource_type == ResourceType.REPLICATION_GROUP:
            # not sure if list parameters should universally be treated this way, restricting for now
            quoted_values = [f"$${v}$$" for v in new_value]
            new_value = f"({', '.join(quoted_values)})"
        else:
            # value serialization should really be driven by property type
            new_value = f"$${new_value}$$" if isinstance(new_value, str) else new_value

        return tidy_sql(
            "ALTER",
            urn.resource_type,
            urn.fqn,
            "SET",
            attr,
            "=",
            new_value,
        )


def update_procedure(urn: URN, data: dict, props: Props) -> str:
    if "execute_as" in data:
        return tidy_sql(
            "ALTER",
            urn.resource_type,
            urn.fqn,
            "EXECUTE AS",
            data["execute_as"]['new_value'],
        )
    else:
        return update__default(urn, data, props)


def update_role_grant(urn: URN, data: dict, props: Props) -> str:
    raise NotImplementedError


def update_schema(urn: URN, data: dict, props: Props) -> str:
    attr, change_dict = data.popitem()
    new_value = change_dict['new_value']
    attr = attr.lower()
    if new_value is None:
        return tidy_sql("ALTER SCHEMA", urn.fqn, "UNSET", attr)
    elif attr == "name":
        return tidy_sql("ALTER SCHEMA", urn.fqn, "RENAME TO", new_value)
    elif attr == "owner":
        raise NotImplementedError(f"Cannot change owner of schema {urn.fqn}, this is not supported yet")
    elif attr == "transient":
        raise Exception("Cannot change transient property of schema")
    elif attr == "managed_access":
        return tidy_sql("ALTER SCHEMA", urn.fqn, "ENABLE" if new_value else "DISABLE", "MANAGED ACCESS")
    else:
        new_value = f"'{new_value}'" if isinstance(new_value, str) else new_value
        return tidy_sql("ALTER SCHEMA", urn.fqn, "SET", attr, "=", new_value)


def drop_resource(urn: URN, data: dict, if_exists: bool = False) -> str:
    return getattr(__this__, f"drop_{urn.resource_label}", drop__default)(urn, data, if_exists=if_exists)


def drop__default(urn: URN, data: dict, if_exists: bool) -> str:
    return tidy_sql(
        "DROP",
        urn.resource_type,
        "IF EXISTS" if if_exists else "",
        urn.fqn,
    )


def drop_function(urn: URN, data: dict, if_exists: bool) -> str:
    return tidy_sql(
        "DROP",
        urn.resource_type,
        "IF EXISTS" if if_exists else "",
        urn.fqn,
    )


def drop_future_grant(urn: URN, data: dict, **kwargs):
    return tidy_sql(
        "REVOKE",
        data["priv"],
        "ON FUTURE",
        pluralize(data["on_type"]).upper(),
        "IN",
        data["in_type"],
        data["in_name"],
        "FROM",
        data["to"],
        # props.render(data), #TODO grant option
    )


def drop_grant(urn: URN, data: dict, **kwargs):
    # FIXME
    if data["priv"] == "OWNERSHIP":
        return "select 1"
    return tidy_sql(
        "REVOKE",
        data["priv"],
        "ON",
        data["on_type"],
        data["on"],
        "FROM",
        data["to"],
        # "CASCADE" if cascade else "RESTRICT",
    )


def drop_grant_on_all(urn: URN, data: dict, **kwargs):
    return tidy_sql(
        "REVOKE",
        data["priv"],
        "ON ALL",
        data["on_type"],
        "IN",
        data["in_type"],
        data["in_name"],
    )


def drop_role_grant(urn: URN, data: dict, **kwargs):
    return tidy_sql(
        "REVOKE ROLE",
        data["role"],
        "FROM",
        "ROLE" if data.get("to_role") else "USER",
        data["to_role"] if data.get("to_role") else data["to_user"],
    )
