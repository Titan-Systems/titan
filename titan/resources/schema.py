from dataclasses import dataclass

from .resource import Resource, ResourceContainer, ResourcePointer, ResourceSpec, ResourceNameTrait
from .role import Role
from ..builtins import SYSTEM_SCHEMAS
from ..enums import ResourceType
from ..props import Props, IntProp, StringProp, TagsProp, FlagProp
from ..resource_name import ResourceName
from ..scope import DatabaseScope


@dataclass(unsafe_hash=True)
class _Schema(ResourceSpec):
    name: ResourceName
    transient: bool = False
    managed_access: bool = False
    data_retention_time_in_days: int = 1
    max_data_extension_time_in_days: int = 14
    default_ddl_collation: str = None
    tags: dict[str, str] = None
    owner: Role = "SYSADMIN"
    comment: str = None

    def __post_init__(self):
        super().__post_init__()
        if self.transient and self.data_retention_time_in_days is not None:
            raise ValueError("Transient schema can't have data retention time")
        elif not self.transient and self.data_retention_time_in_days is None:
            self.data_retention_time_in_days = 1


class Schema(ResourceNameTrait, Resource, ResourceContainer):
    """
    Description:
        Represents a schema in Snowflake, which is a logical grouping of database objects such as tables, views, and stored procedures. Schemas are used to organize and manage such objects within a database.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-schema

    Fields:
        name (string, required): The name of the schema.
        transient (bool): Specifies if the schema is transient. Defaults to False.
        managed_access (bool): Specifies if the schema has managed access. Defaults to False.
        data_retention_time_in_days (int): The number of days to retain data. Defaults to 1.
        max_data_extension_time_in_days (int): The maximum number of days to extend data retention. Defaults to 14.
        default_ddl_collation (string): The default DDL collation setting.
        tags (dict): Tags associated with the schema.
        owner (string or Role): The owner of the schema. Defaults to "SYSADMIN".
        comment (string): A comment about the schema.

    Python:

        ```python
        schema = Schema(
            name="some_schema",
            transient=True,
            managed_access=True,
            data_retention_time_in_days=7,
            max_data_extension_time_in_days=28,
            default_ddl_collation="utf8",
            tags={"project": "analytics"},
            owner="SYSADMIN",
            comment="Schema for analytics project."
        )
        ```

    Yaml:

        ```yaml
        schemas:
          - name: some_schema
            transient: true
            managed_access: true
            data_retention_time_in_days: 7
            max_data_extension_time_in_days: 28
            default_ddl_collation: utf8
            tags:
              project: analytics
            owner: SYSADMIN
            comment: Schema for analytics project.
        ```
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
        super().__init__(name, **kwargs)

        # TODO:
        # This is a temporary fix to allow the creation of schemas in the format
        # `DB.SCHEMA` without having to create the database first.
        # The plan going forward is to put this behavior into a ResourceNameTrait trait
        # and have all resources with names inherit from that.
        # name = self._add_implied_containers(name)

        if self._name == "INFORMATION_SCHEMA":
            comment = "Views describing the contents of schemas in this database"

        if self._name in SYSTEM_SCHEMAS:
            self.implicit = True

        self._data: _Schema = _Schema(
            name=self._name,
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
