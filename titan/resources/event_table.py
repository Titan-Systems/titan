from dataclasses import dataclass

from .resource import Resource, ResourceSpec
from .role import Role
from ..enums import ResourceType
from ..scope import SchemaScope
from ..props import (
    BoolProp,
    FlagProp,
    IntProp,
    Props,
    StringProp,
    StringListProp,
    TagsProp,
)


@dataclass(unsafe_hash=True)
class _EventTable(ResourceSpec):
    name: str
    cluster_by: str = None
    data_retention_time_in_days: int = None
    max_data_extension_time_in_days: int = None
    change_tracking: bool = False
    default_ddl_collation: str = None
    copy_grants: bool = False
    comment: str = None
    # row_access_policy: str = None
    tags: dict[str, str] = None


class EventTable(Resource):
    """An event table captures events, including logged messages from functions and procedures.

    CREATE [ OR REPLACE ] EVENT TABLE [ IF NOT EXISTS ] <name>
      [ CLUSTER BY ( <expr> [ , <expr> , ... ] ) ]
      [ DATA_RETENTION_TIME_IN_DAYS = <integer> ]
      [ MAX_DATA_EXTENSION_TIME_IN_DAYS = <integer> ]
      [ CHANGE_TRACKING = { TRUE | FALSE } ]
      [ DEFAULT_DDL_COLLATION = '<collation_specification>' ]
      [ COPY GRANTS ]
      [ [ WITH ] COMMENT = '<string_literal>' ]
      [ [ WITH ] ROW ACCESS POLICY <policy_name> ON ( <col_name> [ , <col_name> ... ] ) ]
      [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
    """

    resource_type = ResourceType.EVENT_TABLE
    props = Props(
        cluster_by=StringListProp("cluster_by"),
        data_retention_time_in_days=IntProp("data_retention_time_in_days"),
        max_data_extension_time_in_days=IntProp("max_data_extension_time_in_days"),
        change_tracking=BoolProp("change_tracking"),
        default_ddl_collation=StringProp("default_ddl_collation"),
        copy_grants=FlagProp("copy grants"),
        comment=StringProp("comment", consume="with"),
        # row_access_policy=StringProp("row_access_policy", consume="with"),
        tags=TagsProp(),
    )
    scope = SchemaScope()
    spec = _EventTable

    def __init__(
        self,
        name: str,
        cluster_by: list[str] = None,
        data_retention_time_in_days: int = None,
        max_data_extension_time_in_days: int = None,
        change_tracking: bool = False,
        default_ddl_collation: str = None,
        copy_grants: bool = False,
        comment: str = None,
        # row_access_policy: str = None,
        tags: dict[str, str] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._data: _EventTable = _EventTable(
            name=name,
            cluster_by=cluster_by,
            data_retention_time_in_days=data_retention_time_in_days,
            max_data_extension_time_in_days=max_data_extension_time_in_days,
            change_tracking=change_tracking,
            default_ddl_collation=default_ddl_collation,
            copy_grants=copy_grants,
            comment=comment,
            # row_access_policy=row_access_policy,
            tags=tags,
        )
