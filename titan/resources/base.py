from typing import Any, ClassVar, Union, Dict, List

from typing_extensions import Annotated

from inflection import underscore
from pydantic import BaseModel, Field, ConfigDict, BeforeValidator, PlainSerializer, field_validator
from pydantic._internal._generics import PydanticGenericMetadata
from pydantic.functional_validators import AfterValidator
from pydantic._internal._model_construction import ModelMetaclass
from pyparsing import ParseException

from ..privs import Privs
from ..enums import DatabasePriv, GlobalPriv, SchemaPriv, Scope
from ..props import Props, IntProp, StringProp, TagsProp, FlagProp
from ..parse import _parse_create_header, _parse_props, _resolve_resource_class

from ..sql import SQL, track_ref

from ..identifiers import FQN
from ..builder import tidy_sql
from .validators import coerce_from_str


# TODO: snowflake resource name compatibility
# TODO: make this configurable
def normalize_resource_name(name: str):
    return name.upper()


ResourceName = Annotated[str, AfterValidator(normalize_resource_name)]

serialize_resource_by_name = PlainSerializer(lambda resource: resource.name if resource else None, return_type=str)


def _add_child(parent, child):
    if child.parent is not None:
        child.parent.children._remove(child)
    if parent is not None:
        parent.children._add(child)


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
    model_config = ConfigDict(
        from_attributes=True,
        extra="forbid",
        validate_assignment=True,
        populate_by_name=True,
        # Don't use this
        use_enum_values=True,
    )

    lifecycle_privs: ClassVar[Privs] = None
    props: ClassVar[Props]
    resource_type: ClassVar[str] = None

    implicit: bool = Field(exclude=True, default=False, repr=False)
    stub: bool = Field(exclude=True, default=False, repr=False)
    _refs: List["Resource"] = []

    def model_post_init(self, ctx):
        for field_name in self.model_fields.keys():
            field_value = getattr(self, field_name)
            if isinstance(field_value, Resource) and not field_value.stub:
                self._refs.append(field_value)
            elif isinstance(field_value, SQL):
                self._refs.extend(field_value.refs)
                setattr(self, field_name, field_value.sql)

    @classmethod
    def fetchable_fields(cls, data):
        data = data.copy()
        for key in list(data.keys()):
            field = cls.model_fields[key]
            fetchable = field.json_schema_extra is None or field.json_schema_extra.get("fetchable", True)
            if not fetchable:
                del data[key]
        return data

    @classmethod
    def from_sql(cls, sql):
        resource_cls = cls
        if resource_cls == Resource:
            resource_cls = Resource.classes[_resolve_resource_class(sql)]

        identifier, remainder_sql = _parse_create_header(sql, resource_cls)

        try:
            props = _parse_props(resource_cls.props, remainder_sql) if remainder_sql else {}
            return resource_cls(**identifier, **props)
        except ParseException as err:
            raise ParseException(f"Error parsing {resource_cls.__name__} props {identifier}") from err

    @property
    def refs(self):
        return self._refs

    def __format__(self, format_spec):
        track_ref(self)
        return self.fully_qualified_name

    def _requires(self, resource):
        self._refs.add(resource)

    def requires(self, *resources):
        if isinstance(resources[0], list):
            resources = resources[0]
        for resource in resources:
            self._requires(resource)
        return self

    @classmethod
    def lifecycle_create(cls, fqn, data, or_replace=False, if_not_exists=False):
        # TODO: modify props to split into header props and footer props
        return tidy_sql(
            "CREATE",
            "OR REPLACE" if or_replace else "",
            cls.resource_type,
            "IF NOT EXISTS" if if_not_exists else "",
            fqn,
            cls.props.render(data),
        )

    @classmethod
    def lifecycle_delete(cls, fqn, data, if_exists=False):
        return tidy_sql("DROP", cls.resource_type, "IF EXISTS" if if_exists else "", fqn)

    def create_sql(self, **kwargs):
        data = self.model_dump(exclude_none=True, exclude_defaults=True)
        return self.lifecycle_create(self.fqn, data, **kwargs)

    def drop_sql(self, **kwargs):
        data = self.model_dump(exclude_none=True, exclude_defaults=True)
        return self.lifecycle_delete(self.fqn, data, **kwargs)


class ResourceChildren:
    def __init__(self, parent):
        self.parent = parent
        self.items = {}

    def _add(self, child):
        if child.resource_key not in self.items:
            self.items[child.resource_key] = []
        # TODO: dedupe?
        self.items[child.resource_key].append(child)

    def _remove(self, child):
        self.items[child.resource_key].remove(child)

    def __contains__(self, child):
        return child.resource_key in self.items and child in self.items[child.resource_key]


class Organization(Resource):
    resource_type = "ORGANIZATION"

    name: ResourceName
    _children: ResourceChildren

    def model_post_init(self, ctx):
        super().model_post_init(ctx)
        self._children = ResourceChildren(self)

    @property
    def children(self):
        return self._children


class OrganizationScoped(BaseModel):
    scope: ClassVar[Scope] = Scope.ORGANIZATION

    organziation: Annotated[Organization, BeforeValidator(coerce_from_str(Organization))] = Field(
        alias="parent", default=None, exclude=True, repr=False
    )

    @property
    def parent(self):
        return self.organziation

    @parent.setter
    def parent(self, new_parent):
        if not isinstance(new_parent, Organization):
            raise ValueError(f"Parent must be an Organization, not {new_parent}")
        new_parent.children.add(self)

    @property
    def fully_qualified_name(self):
        return FQN(name=self.name.upper())

    @property
    def fqn(self):
        return self.fully_qualified_name


class Account(Resource, OrganizationScoped):
    resource_type = "ACCOUNT"

    name: ResourceName

    _children: ResourceChildren

    def model_post_init(self, ctx):
        super().model_post_init(ctx)
        self._children = ResourceChildren(self)

    @property
    def children(self):
        return self._children

    def add(self, *children: "AccountScoped"):
        if isinstance(children[0], list):
            children = children[0]
        for child in children:
            child.account = self

    def remove(self, *children: "AccountScoped"):
        if isinstance(children[0], list):
            children = children[0]
        for child in children:
            child.account = None


class AccountScoped(BaseModel):
    scope: ClassVar[Scope] = Scope.ACCOUNT

    _account: Annotated[
        Account,
        BeforeValidator(coerce_from_str(Account)),
        Field(exclude=True, repr=False, alias="account"),
    ] = None

    @property
    def parent(self):
        return self._account

    @parent.setter
    def parent(self, new_account):
        self.account = new_account

    @property
    def account(self):
        return self._account

    @account.setter
    def account(self, new_account):
        _add_child(new_account, self)
        self._account = new_account

    @property
    def fully_qualified_name(self):
        return FQN(name=self.name.upper())

    @property
    def fqn(self):
        return self.fully_qualified_name


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

    lifecycle_privs = Privs(
        create=GlobalPriv.CREATE_DATABASE,
        read=DatabasePriv.USAGE,
        delete=DatabasePriv.OWNERSHIP,
    )

    props = Props(
        transient=FlagProp("transient"),
        data_retention_time_in_days=IntProp("data_retention_time_in_days"),
        max_data_extension_time_in_days=IntProp("max_data_extension_time_in_days"),
        default_ddl_collation=StringProp("default_ddl_collation"),
        tags=TagsProp(),
        comment=StringProp("comment"),
    )

    name: ResourceName
    transient: bool = False
    owner: str = "SYSADMIN"
    data_retention_time_in_days: int = 1
    max_data_extension_time_in_days: int = 14
    default_ddl_collation: str = None
    tags: Dict[str, str] = None
    comment: str = None

    _children: ResourceChildren

    def model_post_init(self, ctx):
        super().model_post_init(ctx)
        self._children = ResourceChildren(self)
        self.add(
            Schema(name="PUBLIC", implicit=True),
            Schema(name="INFORMATION_SCHEMA", implicit=True),
        )

    @property
    def children(self):
        return self._children

    @classmethod
    def lifecycle_create(cls, fqn, data, or_replace=False, if_not_exists=False):
        return tidy_sql(
            "CREATE",
            "OR REPLACE" if or_replace else "",
            "TRANSIENT" if data.get("transient") else "",
            "DATABASE",
            "IF NOT EXISTS" if if_not_exists else "",
            fqn,
            cls.props.render(data),
        )

    @classmethod
    def lifecycle_update(cls, fqn, change, if_exists=False):
        attr, new_value = change.popitem()
        attr = attr.upper()
        if new_value is None:
            return tidy_sql(
                "ALTER DATABASE",
                "IF EXISTS" if if_exists else "",
                fqn,
                "UNSET",
                attr,
            )
        elif attr == "NAME":
            return tidy_sql(
                "ALTER DATABASE",
                "IF EXISTS" if if_exists else "",
                fqn,
                "RENAME TO",
                new_value,
            )
        else:
            new_value = f"'{new_value}'" if isinstance(new_value, str) else new_value
            return tidy_sql(
                "ALTER DATABASE",
                "IF EXISTS" if if_exists else "",
                fqn,
                "SET",
                attr,
                "=",
                new_value,
            )

    def add(self, *children: "DatabaseScoped"):
        if isinstance(children[0], list):
            children = children[0]
        for child in children:
            child.database = self

    def remove(self, *children: "DatabaseScoped"):
        if isinstance(children[0], list):
            children = children[0]
        for child in children:
            child.database = None


class DatabaseScoped(BaseModel):
    scope: ClassVar[Scope] = Scope.DATABASE

    _database: Annotated[
        Database,
        BeforeValidator(coerce_from_str(Database)),
        Field(exclude=True, repr=False),
    ] = None

    @property
    def parent(self):
        return self.database

    @parent.setter
    def parent(self, new_database):
        self.database = new_database

    @property
    def database(self):
        return self._database

    @database.setter
    def database(self, new_database):
        _add_child(new_database, self)
        self._database = new_database

    @property
    def fully_qualified_name(self):
        return FQN(database=self.database.name if self.database else None, name=self.name.upper())

    @property
    def fqn(self):
        return self.fully_qualified_name


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
    lifecycle_privs = Privs(
        create=DatabasePriv.CREATE_SCHEMA,
    )
    props = Props(
        transient=FlagProp("transient"),
        with_managed_access=FlagProp("with managed access"),
        data_retention_time_in_days=IntProp("data_retention_time_in_days"),
        max_data_extension_time_in_days=IntProp("max_data_extension_time_in_days"),
        default_ddl_collation=StringProp("default_ddl_collation"),
        tags=TagsProp(),
        comment=StringProp("comment"),
    )

    name: ResourceName
    transient: bool = False
    owner: str = "SYSADMIN"
    with_managed_access: bool = None
    data_retention_time_in_days: int = None
    max_data_extension_time_in_days: int = None
    default_ddl_collation: str = None
    tags: Dict[str, str] = None
    comment: str = None

    _children: ResourceChildren

    def model_post_init(self, ctx):
        super().model_post_init(ctx)
        self._children = ResourceChildren(self)

    @property
    def children(self):
        return self._children

    def add(self, *children: "SchemaScoped"):
        if isinstance(children[0], list):
            children = children[0]
        for child in children:
            child.schema = self

    def remove(self, *children: "SchemaScoped"):
        if isinstance(children[0], list):
            children = children[0]
        for child in children:
            child.schema = None


class SchemaScoped(BaseModel):
    scope: ClassVar[Scope] = Scope.SCHEMA
    _schema: Annotated[
        Schema,
        BeforeValidator(coerce_from_str(Schema)),
        Field(exclude=True, repr=False),
    ] = None

    @property
    def parent(self):
        return self._schema

    @parent.setter
    def parent(self, new_schema):
        self.schema = new_schema

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, new_schema):
        _add_child(new_schema, self)
        self._schema = new_schema

    @property
    def fully_qualified_name(self):
        schema = self.schema_.name if self.schema_ else None
        database = None
        if self.schema_ and self.schema_.database:
            database = self.schema_.database.name
        return FQN(database=database, schema=schema, name=self.name.upper())

    @property
    def fqn(self):
        return self.fully_qualified_name
