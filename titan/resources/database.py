from dataclasses import dataclass

from ..enums import ResourceType
from ..identifiers import FQN, URN
from ..props import FlagProp, IdentifierProp, IntProp, Props, StringProp, TagsProp
from ..resource_name import ResourceName
from ..scope import AccountScope
from .external_volume import ExternalVolume
from .resource import NamedResource, Resource, ResourceContainer, ResourceSpec
from .role import Role
from .schema import Schema
from .tag import TaggableResource


@dataclass(unsafe_hash=True)
class _Database(ResourceSpec):
    name: ResourceName
    transient: bool = False
    owner: Role = "SYSADMIN"
    data_retention_time_in_days: int = 1
    max_data_extension_time_in_days: int = 14
    external_volume: ExternalVolume = None
    catalog: str = None
    default_ddl_collation: str = None
    comment: str = None


class Database(NamedResource, TaggableResource, Resource, ResourceContainer):
    """
    Description:
        Represents a database in Snowflake.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-database

    Fields:
        name (string, required): The name of the database.
        transient (bool): Specifies if the database is transient. Defaults to False.
        owner (string or Role): The owner role of the database. Defaults to "SYSADMIN".
        data_retention_time_in_days (int): The number of days to retain data. Defaults to 1.
        max_data_extension_time_in_days (int): The maximum number of days to extend data retention. Defaults to 14.
        default_ddl_collation (string): The default collation for DDL statements.
        tags (dict): A dictionary of tags associated with the database.
        comment (string): A comment describing the database.

    Python:

        ```python
        database = Database(
            name="some_database",
            transient=True,
            owner="SYSADMIN",
            data_retention_time_in_days=7,
            max_data_extension_time_in_days=28,
            default_ddl_collation="utf8",
            tags={"project": "research", "priority": "high"},
            comment="This is a database."
        )
        ```

        A database can contain schemas. In Python, you can add a schema to a database in several ways:

        By database name:
        ```python
        sch = Schema(
            name = "some_schema",
            database = "my_test_db",
        )
        ```

        By database object:
        ```python
        db = Database(name = "my_test_db")
        sch = Schema(
            name = "some_schema",
            database = db,
        )
        ```

        Or using the `add` method:
        ```python
        db = Database(name = "my_test_db")
        sch = Schema(name = "some_schema")
        db.add(sch)
        ```

    Yaml:

        ```yaml
        databases:
          - name: some_database
            transient: true
            owner: SYSADMIN
            data_retention_time_in_days: 7
            max_data_extension_time_in_days: 28
            default_ddl_collation: utf8
            tags:
              project: research
              priority: high
            comment: This is a database.
        ```

        In yaml, you can add schemas to a database using the `schemas` field:

        ```yaml
        databases:
          - name: some_database
            schemas:
              - name: another_schema
        ```

        Or by name:

        ```yaml
        databases:
          - name: some_database
        schemas:
            - name: another_schema
              database: some_database
        ```
    """

    resource_type = ResourceType.DATABASE

    props = Props(
        transient=FlagProp("transient"),
        data_retention_time_in_days=IntProp("data_retention_time_in_days"),
        max_data_extension_time_in_days=IntProp("max_data_extension_time_in_days"),
        default_ddl_collation=StringProp("default_ddl_collation"),
        external_volume=IdentifierProp("external_volume"),
        catalog=StringProp("catalog"),
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
        external_volume: str = None,
        catalog: str = None,
        default_ddl_collation: str = None,
        tags: dict[str, str] = None,
        comment: str = None,
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._data = _Database(
            name=self._name,
            transient=transient,
            owner=owner,
            data_retention_time_in_days=data_retention_time_in_days,
            max_data_extension_time_in_days=max_data_extension_time_in_days,
            external_volume=external_volume,
            catalog=catalog,
            default_ddl_collation=default_ddl_collation,
            comment=comment,
        )
        if self._data.name != "SNOWFLAKE":
            self._public_schema = Schema(name="PUBLIC", implicit=True, owner=owner)
            self.add(self._public_schema)
        self.set_tags(tags)

    def schemas(self):
        return self.items(resource_type=ResourceType.SCHEMA)

    @property
    def public_schema(self) -> Schema:
        return self._public_schema


def public_schema_urn(database_urn: URN) -> URN:
    return URN(
        resource_type=ResourceType.SCHEMA,
        fqn=FQN(name=ResourceName("PUBLIC"), database=ResourceName(str(database_urn.fqn))),
        account_locator=database_urn.account_locator,
    )
