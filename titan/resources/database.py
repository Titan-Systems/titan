from dataclasses import dataclass

from .resource import Resource, ResourceContainer, ResourceSpec
from .role import Role
from .schema import Schema
from ..enums import ResourceType
from ..props import Props, IntProp, StringProp, TagsProp, FlagProp
from ..resource_name import ResourceName
from ..scope import AccountScope


@dataclass(unsafe_hash=True)
class _Database(ResourceSpec):
    name: ResourceName
    transient: bool = False
    owner: Role = Role("SYSADMIN")
    data_retention_time_in_days: int = 1
    max_data_extension_time_in_days: int = 14
    default_ddl_collation: str = None
    tags: dict[str, str] = None
    comment: str = None


class Database(Resource, ResourceContainer):
    """
    CREATE [ OR REPLACE ] [ TRANSIENT ] DATABASE [ IF NOT EXISTS ] <name>
        [ DATA_RETENTION_TIME_IN_DAYS = <integer> ]
        [ MAX_DATA_EXTENSION_TIME_IN_DAYS = <integer> ]
        [ DEFAULT_DDL_COLLATION = '<collation_specification>' ]
        [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
        [ COMMENT = '<string_literal>' ]
    """

    resource_type = ResourceType.DATABASE

    props = Props(
        transient=FlagProp("transient"),
        data_retention_time_in_days=IntProp("data_retention_time_in_days"),
        max_data_extension_time_in_days=IntProp("max_data_extension_time_in_days"),
        default_ddl_collation=StringProp("default_ddl_collation"),
        tags=TagsProp(),
        comment=StringProp("comment"),
    )
    scope = AccountScope()
    spec = _Database

    def __init__(
        self,
        name: str,
        transient: bool = False,
        owner: str = "SYSADMIN",
        data_retention_time_in_days: int = 1,
        max_data_extension_time_in_days: int = 14,
        default_ddl_collation: str = None,
        tags: dict[str, str] = None,
        comment: str = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._data = _Database(
            name=name.upper(),
            transient=transient,
            owner=owner,
            data_retention_time_in_days=data_retention_time_in_days,
            max_data_extension_time_in_days=max_data_extension_time_in_days,
            default_ddl_collation=default_ddl_collation,
            tags=tags,
            comment=comment,
        )
        if self._data.name != "SNOWFLAKE":
            self.add(
                Schema(name="PUBLIC", implicit=True),
                Schema(name="INFORMATION_SCHEMA", implicit=True),
            )
        print(self._items)

    def schemas(self):
        return self.items(resource_type=ResourceType.SCHEMA)
