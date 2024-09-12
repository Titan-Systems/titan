from dataclasses import dataclass, field

from ..enums import ResourceType
from ..props import (
    BoolProp,
    FlagProp,
    IdentifierListProp,
    IntProp,
    Props,
    StringProp,
    TagsProp,
)
from ..resource_name import ResourceName
from ..role_ref import RoleRef
from ..scope import SchemaScope
from .resource import NamedResource, Resource, ResourceSpec
from .tag import TaggableResource


@dataclass(unsafe_hash=True)
class _EventTable(ResourceSpec):
    name: ResourceName
    cluster_by: list[str] = None
    data_retention_time_in_days: int = None
    max_data_extension_time_in_days: int = None
    change_tracking: bool = False
    default_ddl_collation: str = None
    copy_grants: bool = field(default=None, metadata={"fetchable": False})
    comment: str = None
    # row_access_policy: str = None
    owner: RoleRef = "SYSADMIN"


class EventTable(NamedResource, TaggableResource, Resource):
    """
    Description:
        An event table captures events, including logged messages from functions and procedures.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-event-table

    Fields:
        name (string, required): The name of the event table.
        cluster_by (list): The expressions to cluster data by.
        data_retention_time_in_days (int): The number of days to retain data.
        max_data_extension_time_in_days (int): The maximum number of days to extend data retention.
        change_tracking (bool): Specifies whether change tracking is enabled. Defaults to False.
        default_ddl_collation (string): The default collation for DDL operations.
        copy_grants (bool): Specifies whether to copy grants. Defaults to False.
        comment (string): A comment for the event table.
        tags (dict): Tags associated with the event table.

    Python:

        ```python
        event_table = EventTable(
            name="some_event_table",
            cluster_by=["timestamp", "user_id"],
            data_retention_time_in_days=365,
            max_data_extension_time_in_days=30,
            change_tracking=True,
            default_ddl_collation="utf8",
            copy_grants=True,
            comment="This is a sample event table.",
            tags={"department": "analytics"}
        )
        ```

    Yaml:

        ```yaml
        event_tables:
          - name: some_event_table
            cluster_by:
              - timestamp
              - user_id
            data_retention_time_in_days: 365
            max_data_extension_time_in_days: 30
            change_tracking: true
            default_ddl_collation: utf8
            copy_grants: true
            comment: This is a sample event table.
            tags:
              department: analytics
        ```
    """

    resource_type = ResourceType.EVENT_TABLE
    props = Props(
        cluster_by=IdentifierListProp("cluster by", eq=False, parens=True),
        data_retention_time_in_days=IntProp("data_retention_time_in_days"),
        max_data_extension_time_in_days=IntProp("max_data_extension_time_in_days"),
        change_tracking=BoolProp("change_tracking"),
        default_ddl_collation=StringProp("default_ddl_collation"),
        copy_grants=FlagProp("copy grants"),
        comment=StringProp("comment", consume="with"),
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
        copy_grants: bool = None,
        comment: str = None,
        owner: str = "SYSADMIN",
        tags: dict[str, str] = None,
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._data: _EventTable = _EventTable(
            name=self._name,
            cluster_by=cluster_by,
            data_retention_time_in_days=data_retention_time_in_days,
            max_data_extension_time_in_days=max_data_extension_time_in_days,
            change_tracking=change_tracking,
            default_ddl_collation=default_ddl_collation,
            copy_grants=copy_grants,
            comment=comment,
            owner=owner,
        )
        self.set_tags(tags)
