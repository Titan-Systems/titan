from dataclasses import dataclass, field

from ..enums import ParseableEnum, ResourceType
from ..props import (
    BoolProp,
    EnumProp,
    IntProp,
    Props,
    SchemaProp,
    StringProp,
    TagsProp,
)
from ..resource_name import ResourceName
from ..scope import SchemaScope
from .column import Column
from .external_volume import ExternalVolume
from .resource import NamedResource, Resource, ResourceSpec
from .role import Role
from .tag import TaggableResource


class StorageSerializationPolicy(ParseableEnum):
    COMPATIBLE = "COMPATIBLE"
    OPTIMIZED = "OPTIMIZED"


@dataclass(unsafe_hash=True)
class _SnowflakeIcebergTable(ResourceSpec):
    name: ResourceName
    columns: list[Column]
    owner: Role = "SYSADMIN"
    external_volume: ExternalVolume = None
    catalog: str = "SNOWFLAKE"
    base_location: str = None
    catalog_sync: str = None
    storage_serialization_policy: StorageSerializationPolicy = StorageSerializationPolicy.OPTIMIZED
    data_retention_time_in_days: int = 1
    max_data_extension_time_in_days: int = 14
    # Snowflake does not currently provide a way to check if change tracking is enabled for iceberg tables
    change_tracking: bool = field(default=None, metadata={"fetchable": False})
    default_ddl_collation: str = None
    comment: str = None

    def __post_init__(self):
        super().__post_init__()
        if self.columns is None:
            raise ValueError("columns can't be None")
        if len(self.columns) == 0:
            raise ValueError("columns can't be empty")


class SnowflakeIcebergTable(NamedResource, TaggableResource, Resource):

    resource_type = ResourceType.ICEBERG_TABLE
    props = Props(
        columns=SchemaProp(),
        external_volume=StringProp("external_volume"),
        catalog=StringProp("catalog"),
        base_location=StringProp("base_location"),
        catalog_sync=StringProp("catalog_sync"),
        storage_serialization_policy=EnumProp("storage_serialization_policy", StorageSerializationPolicy),
        data_retention_time_in_days=IntProp("data_retention_time_in_days"),
        max_data_extension_time_in_days=IntProp("max_data_extension_time_in_days"),
        change_tracking=BoolProp("change_tracking"),
        default_ddl_collation=StringProp("default_ddl_collation"),
        comment=StringProp("comment"),
        tags=TagsProp(),
    )
    scope = SchemaScope()
    spec = _SnowflakeIcebergTable

    def __init__(
        self,
        name: str,
        columns: list[Column],
        owner: str = "SYSADMIN",
        external_volume: str = None,
        catalog: str = "SNOWFLAKE",
        base_location: str = None,
        catalog_sync: str = None,
        storage_serialization_policy: str = "OPTIMIZED",
        data_retention_time_in_days: int = 1,
        max_data_extension_time_in_days: int = 14,
        change_tracking: bool = None,
        default_ddl_collation: str = None,
        comment: str = None,
        tags: dict[str, str] = None,
        **kwargs,
    ):

        if "lifecycle" not in kwargs:
            lifecycle = {
                "ignore_changes": "columns",
            }
            kwargs["lifecycle"] = lifecycle

        super().__init__(name, **kwargs)

        self._data: _SnowflakeIcebergTable = _SnowflakeIcebergTable(
            name=self._name,
            columns=columns,
            owner=owner,
            external_volume=external_volume,
            catalog=catalog,
            base_location=base_location,
            catalog_sync=catalog_sync,
            storage_serialization_policy=storage_serialization_policy,
            data_retention_time_in_days=data_retention_time_in_days,
            max_data_extension_time_in_days=max_data_extension_time_in_days,
            change_tracking=change_tracking,
            default_ddl_collation=default_ddl_collation,
            comment=comment,
        )
        if self._data.external_volume:
            self._requires(self._data.external_volume)
        self.set_tags(tags)
