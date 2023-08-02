from typing import ClassVar, Union, Dict

from inflection import underscore
from pydantic import BaseModel, BeforeValidator, Field, ConfigDict, field_validator
from pydantic._internal._model_construction import ModelMetaclass
from pyparsing import ParseException

from ..enums import Scope
from ..props import Props, IntProp, StringProp, TagsProp, FlagProp
from ..parse import _parse_create_header, _resolve_resource_class
from ..sql import add_ref

resource_db = {}


class _Resource(ModelMetaclass):
    classes = {}
    resource_key: str = None

    def __new__(cls, name, bases, attrs):
        cls_ = super().__new__(cls, name, bases, attrs)
        cls_.resource_key = underscore(name)
        cls_.__doc__ = cls_.__doc__ or ""
        cls.classes[cls_.resource_key] = cls_
        return cls_


class Resource(BaseModel, metaclass=_Resource):
    model_config = ConfigDict(from_attributes=True, extra="forbid", validate_assignment=True)

    resource_type: ClassVar[str] = None
    props: ClassVar[Props]

    name: str
    implicit: bool = Field(exclude=True, default=False)
    stub: bool = Field(exclude=True, default=False)

    # @field_validator("*")
    # @classmethod
    # def coerce_resource(cls, v: Any, field: Field) -> Any:
    #     if field.field_name != "name":
    #         print(field)
    #     # if issubclass(field, Resource and isinstance(v, str)):
    #     #     return field.type_.find(v)
    #     return v

    # TODO: check if this is being super()'d correctly
    def model_post_init(self, ctx):
        resource_db[(self.__class__, self.name)] = self

    # TODO: snowflake resource name compatibility
    @field_validator("name")
    @classmethod
    def validate_name(cls, name: str):
        return name.upper()

    # TODO: Reconsider
    @classmethod
    def find(cls, resource_name):
        return cls(name=resource_name, stub=True)
        # if resource_name is None:
        #     return None
        # key = (cls, resource_name)
        # if key not in resource_db:
        #     resource_db[key] = cls(name=resource_name, stub=True)
        # return resource_db[key]

    @classmethod
    def from_sql(cls, sql):
        resource_cls = cls
        if resource_cls == Resource:
            resource_cls = Resource.classes[_resolve_resource_class(sql)]

        identifier, remainder = _parse_create_header(sql, resource_cls)

        try:
            props = resource_cls.props.parse(remainder) if remainder else {}
            return resource_cls(**identifier, **props)
        except ParseException as err:
            print(f"Error parsing resource props {resource_cls.__name__} {identifier}")
            print(err.explain())
            # return None
            raise err

    # def __format__(self, format_spec):
    #     add_ref(self)
    #     return self.fully_qualified_name


class Createable(BaseModel):
    def create_sql(self):
        return f"CREATE {self.resource_type} {self.name}"  # self.props.sql()

    # def create_or_replace_sql(self):
    #     return f"CREATE OR REPLACE {self.resource_type} {self.name}"


class ResourceDB:
    def __init__(self, cls):
        self.resource_class = cls
        self._db = {}

    def __getitem__(self, key):
        if key is None:
            return None
        if key not in self._db:
            self._db[key] = self.resource_class(name=key, stub=True)
        return self._db[key]

    def __setitem__(self, key, value):
        if key is None:
            raise Exception
        if key in self._db:
            if self._db[key].stub:
                self._db[key] = value
            else:
                raise Exception("An object already exists with that name")
        # if key not in self._db:
        # self._db[key] = self.resource_class(name=key, implicit=True)
        # return self._db[key]
        self._db[key] = value


class Organization(Resource):
    resource_type = "ORGANIZATION"

    name: str


class OrganizationScoped(BaseModel):
    scope: ClassVar[Scope] = Scope.ORGANIZATION
    organziation: Union[str, Organization] = Field(default=None, exclude=True)

    @property
    def parent(self):
        return self.organziation

    @property
    def fully_qualified_name(self):
        return self.name.upper()

    @property
    def fqn(self):
        return self.fully_qualified_name


class Account(Resource, OrganizationScoped):
    resource_type = "ACCOUNT"

    name: str


class AccountScoped(BaseModel):
    scope: ClassVar[Scope] = Scope.ACCOUNT
    account: Union[str, Account] = Field(default=None, exclude=True)

    @property
    def parent(self):
        return self.account

    @property
    def fully_qualified_name(self):
        return self.name.upper()

    @property
    def fqn(self):
        return self.fully_qualified_name

    @field_validator("account")
    @classmethod
    def validate_account(cls, account: Union[str, Account]):
        return account if isinstance(account, Account) else Account.find(account)


class Database(Resource, AccountScoped):
    """
    CREATE [ OR REPLACE ] [ TRANSIENT ] DATABASE [ IF NOT EXISTS ] <name>
        [ CLONE <source_db>
                [ { AT | BEFORE } ( { TIMESTAMP => <timestamp> | OFFSET => <time_difference> | STATEMENT => <id> } ) ] ]
        [ DATA_RETENTION_TIME_IN_DAYS = <integer> ]
        [ MAX_DATA_EXTENSION_TIME_IN_DAYS = <integer> ]
        [ DEFAULT_DDL_COLLATION = '<collation_specification>' ]
        [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
        [ COMMENT = '<string_literal>' ]
    """

    resource_type = "DATABASE"
    props = Props(
        transient=FlagProp("transient"),
        data_retention_time_in_days=IntProp("data_retention_time_in_days"),
        max_data_extension_time_in_days=IntProp("max_data_extension_time_in_days"),
        default_ddl_collation=StringProp("default_ddl_collation"),
        tags=TagsProp(),
        comment=StringProp("comment"),
    )

    name: str
    transient: bool = False
    owner: str = "SYSADMIN"
    data_retention_time_in_days: int = 1
    max_data_extension_time_in_days: int = 14
    default_ddl_collation: str = None
    tags: Dict[str, str] = None
    comment: str = None


class DatabaseScoped(BaseModel):
    scope: ClassVar[Scope] = Scope.DATABASE
    database: Union[str, Database] = Field(default=None, exclude=True)

    @property
    def parent(self):
        return self.database

    @property
    def fully_qualified_name(self):
        parts = [self.name.upper()]
        if self.database:
            parts.insert(0, self.database.name)
        return ".".join(parts)

    @property
    def fqn(self):
        return self.fully_qualified_name

    @field_validator("database")
    @classmethod
    def validate_database(cls, database: Union[str, Database]):
        return database if isinstance(database, Database) else Database.find(database)


class Schema(Resource, DatabaseScoped):
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

    resource_type = "SCHEMA"
    props = Props(
        transient=FlagProp("transient"),
        with_managed_access=FlagProp("with managed access"),
        data_retention_time_in_days=IntProp("data_retention_time_in_days"),
        max_data_extension_time_in_days=IntProp("max_data_extension_time_in_days"),
        default_ddl_collation=StringProp("default_ddl_collation"),
        tags=TagsProp(),
        comment=StringProp("comment"),
    )

    name: str
    transient: bool = False
    owner: str = None
    with_managed_access: bool = False
    data_retention_time_in_days: int = None
    max_data_extension_time_in_days: int = None
    default_ddl_collation: str = None
    tags: Dict[str, str] = None
    comment: str = None


class SchemaScoped(BaseModel):
    scope: ClassVar[Scope] = Scope.SCHEMA
    schema_: Union[str, Schema] = Field(alias="schema", default=None, exclude=True)

    @property
    def parent(self):
        return self.schema_

    @property
    def fully_qualified_name(self):
        parts = [self.name.upper()]
        if self.schema_:
            parts.insert(0, self.schema_.fully_qualified_name)
        return ".".join(parts)

    @property
    def fqn(self):
        return self.fully_qualified_name

    @field_validator("schema_")
    @classmethod
    def validate_schema(cls, schema: Union[str, Schema]):
        return schema if isinstance(schema, Schema) else Schema.find(schema)


def coerce_from_str(cls: Resource) -> BeforeValidator:
    return BeforeValidator(lambda name: cls.find(name) if isinstance(name, str) else name)
