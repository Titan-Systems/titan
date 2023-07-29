from inflection import underscore
from typing import ClassVar, Union

from pydantic import BaseModel, Field, ConfigDict, SerializeAsAny, field_serializer
from pydantic._internal._model_construction import ModelMetaclass

from pyparsing import ParseException

from .enums import Scope
from .parse import _parse_create_header, _resolve_resource_class
from .props import Props
from .urn import URN
from .sql import add_ref


resource_db = {}


class _Resource(ModelMetaclass):
    classes = {}

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

    # TODO: check if this is being super()'d correctly
    def model_post_init(self, ctx):
        resource_db[(self.__class__, self.name)] = self

    # TODO: snowflake resource name compatibility
    @field_serializer("name")
    def serialize_name(self, name: str, _info):
        return name.upper()

    # TODO: Reconsider
    @classmethod
    def find(cls, resource_name):
        if resource_name is None:
            return None
        key = (cls, resource_name)
        if key not in resource_db:
            resource_db[key] = cls(name=resource_name, stub=True)
        return resource_db[key]

    @classmethod
    def from_sql(cls, sql):
        resource_cls = cls
        if resource_cls == Resource:
            resource_cls = Resource.classes[_resolve_resource_class(sql)]

        identifier, remainder = _parse_create_header(sql, resource_cls)

        try:
            props = resource_cls.props.parse(remainder) if remainder else {}
            if isinstance(identifier, list):
                print(identifier)
                print("debug")
            return resource_cls(**identifier, **props)
        except ParseException as err:
            print(f"Error parsing resource props {resource_cls.__name__} {identifier}")
            print(err.explain())
            # return None
            raise err

    # @property
    # def fully_qualified_name(self):
    #     return self.name.upper()

    @property
    def fqn(self):
        return self.fully_qualified_name

    @property
    def urn(self):
        """
        urn:sf:us-central1.gcp::account/UJ63311
        """
        return URN(resource_key=self.resource_key, name=self.fully_qualified_name)

    def __format__(self, format_spec):
        add_ref(self)
        return self.fully_qualified_name

    def finalize(self):
        raise NotImplementedError


class OrganizationScoped(BaseModel):
    scope: ClassVar[Scope] = Scope.ORGANIZATION
    organziation: SerializeAsAny[Union[str, Resource]] = None

    @property
    def fully_qualified_name(self):
        return self.name.upper()

    @property
    def urn(self):
        """
        urn:sf:us-central1.gcp:AB11223:database/TEST_DB
        """
        if self.account:
            account = self.account.name
            region = self.account.region
        else:
            account = "NULL"
            region = "NULL"
        return URN(region, account, self.resource_key, self.fully_qualified_name)

    def finalize(self):
        raise NotImplementedError


class AccountScoped(BaseModel):
    scope: ClassVar[Scope] = Scope.ACCOUNT
    account: SerializeAsAny[Union[str, Resource]] = None

    @property
    def fully_qualified_name(self):
        return self.name.upper()

    @property
    def urn(self):
        """
        urn:sf:us-central1.gcp:AB11223:database/TEST_DB
        """
        if self.account:
            account = self.account.name
            region = self.account.region
        else:
            account = "NULL"
            region = "NULL"
        return URN(region, account, self.resource_key, self.fully_qualified_name)

    def finalize(self):
        if self.account is None:
            raise Exception(f"AccountScoped resource {self} has no account")


class DatabaseScoped(BaseModel):
    scope: ClassVar[Scope] = Scope.DATABASE
    database: SerializeAsAny[Union[str, Resource]] = None

    @property
    def fully_qualified_name(self):
        database = self.database.name if self.database else "[NULL]"
        return ".".join([database, self.name.upper()])

    def finalize(self):
        if self.database is None:
            raise Exception(f"DatabaseScoped resource {self} has no database")


class SchemaScoped(BaseModel):
    scope: ClassVar[Scope] = Scope.SCHEMA
    schema_: Union[str, Resource] = Field(alias="schema", default=None)

    @property
    def fully_qualified_name(self):
        schema = self.schema_.name if self.schema_ else "[NULL]"
        return ".".join([schema, self.name.upper()])

    def finalize(self):
        if self.schema_ is None:
            raise Exception(f"SchemaScoped resource {self} has no schema")


class Createable(BaseModel):
    def create_sql(self):
        return f"CREATE {self.resource_type} {self.name}"  # self.props.sql()


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
