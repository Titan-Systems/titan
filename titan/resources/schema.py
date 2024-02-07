from dataclasses import dataclass

from .resource import Resource, ResourceContainer, ResourcePointer, ResourceSpec
from ..enums import ResourceType
from ..props import Props, IntProp, StringProp, TagsProp, FlagProp
from ..scope import DatabaseScope


@dataclass
class _Schema(ResourceSpec):
    name: str
    transient: bool = False
    managed_access: bool = False
    data_retention_time_in_days: int = 1
    max_data_extension_time_in_days: int = 14
    default_ddl_collation: str = None
    tags: dict[str, str] = None
    owner: str = "SYSADMIN"
    comment: str = None

    def __post_init__(self):
        super().__post_init__()
        if self.transient and self.data_retention_time_in_days is not None:
            raise ValueError("Transient schema can't have data retention time")
        elif not self.transient and self.data_retention_time_in_days is None:
            self.data_retention_time_in_days = 1


class Schema(Resource, ResourceContainer):
    """
    CREATE [ OR REPLACE ] [ TRANSIENT ] SCHEMA [ IF NOT EXISTS ] <name>
      [ CLONE <source_schema>
            [ { AT | BEFORE } ( { TIMESTAMP => <timestamp> | OFFSET => <time_difference> | STATEMENT => <id> } ) ] ]
      [ WITH MANAGED ACCESS ]
      [ DATA_RETENTION_TIME_IN_DAYS = <integer> ]
      [ MAX_DATA_EXTENSION_TIME_IN_DAYS = <integer> ]
      [ DEFAULT_DDL_COLLATION = '<collation_specification>' ]
      [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
      [ COMMENT = '<string_literal>' ]
    """

    resource_type = ResourceType.SCHEMA
    props = Props(
        transient=FlagProp("transient"),
        managed_access=FlagProp("with managed access"),
        data_retention_time_in_days=IntProp("data_retention_time_in_days"),
        max_data_extension_time_in_days=IntProp("max_data_extension_time_in_days"),
        default_ddl_collation=StringProp("default_ddl_collation"),
        tags=TagsProp(),
        comment=StringProp("comment"),
    )
    scope = DatabaseScope()
    spec = _Schema

    def __init__(
        self,
        name: str,
        transient: bool = False,
        managed_access: bool = False,
        data_retention_time_in_days: int = None,
        max_data_extension_time_in_days: int = 14,
        default_ddl_collation: str = None,
        tags: dict[str, str] = None,
        owner: str = "SYSADMIN",
        comment: str = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._data: _Schema = _Schema(
            name=name,
            transient=transient,
            managed_access=managed_access,
            data_retention_time_in_days=data_retention_time_in_days,
            max_data_extension_time_in_days=max_data_extension_time_in_days,
            default_ddl_collation=default_ddl_collation,
            tags=tags,
            owner=owner,
            comment=comment,
        )
        if self._data.tags:
            for tag_name in self._data.tags.keys():
                self.requires(ResourcePointer(name=tag_name, resource_type=ResourceType.TAG))
