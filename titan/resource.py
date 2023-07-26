from enum import Enum
from typing import ClassVar

import pyparsing as pp

from pydantic import BaseModel, Field, ConfigDict, SerializeAsAny, field_serializer

from .props import Props
from .urn import URN
from .sql import add_ref

Keyword = pp.CaselessKeyword

CREATE = Keyword("CREATE").suppress()
OR_REPLACE = (Keyword("OR") + Keyword("REPLACE")).suppress()
IF_NOT_EXISTS = (Keyword("IF") + Keyword("NOT") + Keyword("EXISTS")).suppress()
TEMPORARY = (Keyword("TEMP") | Keyword("TEMPORARY")).suppress()
WITH = Keyword("WITH").suppress()

REST_OF_STRING = pp.Word(pp.printables + " \n") | pp.StringEnd() | pp.Empty()

Identifier = pp.Word(pp.alphanums + "_", pp.alphanums + "_$") | pp.dbl_quoted_string
ScopedIdentifier = Identifier


class Namespace(Enum):
    ORGANIZATION = "ORGANIZATION"
    ACCOUNT = "ACCOUNT"
    DATABASE = "DATABASE"
    SCHEMA = "SCHEMA"
    TABLE = "TABLE"


resource_db = {}


class Resource(BaseModel):
    model_config = ConfigDict(from_attributes=True, extra="forbid", validate_assignment=True)

    resource_type: ClassVar[str] = None
    namespace: ClassVar[Namespace]
    props: ClassVar[Props]

    name: str
    implicit: bool = Field(exclude=True, default=False)
    stub: bool = Field(exclude=True, default=False)

    def model_post_init(self, ctx):
        resource_db[(self.__class__, self.name)] = self

    @field_serializer("name")
    def serialize_dt(self, name: str, _info):
        return name.upper()

    @classmethod
    def find(cls, resource_name):
        if resource_name is None:
            return None
        key = (cls, resource_name)
        if key not in resource_db:
            resource_db[key] = cls(name=resource_name, stub=True)
        return resource_db[key]

    @classmethod
    def _resolve_class(cls, resource_type: str, props_sql: str):
        if cls.resource_type == resource_type:
            return cls
        for resource_cls in cls.__subclasses__():
            if resource_cls.resource_type == resource_type:
                return resource_cls._resolve_class(resource_type, props_sql)

    @classmethod
    def from_sql(cls, sql):
        is_generic = cls == Resource

        # New but not working. Fixes bug with 2+ spaces between keywords eg RESOURCE  MONITOR
        #                                                                           ^^
        types = [cls.resource_type]
        if is_generic:
            types = [subcls.resource_type for subcls in cls.__subclasses__() if subcls.resource_type]
        resource_type_token = pp.Or([Keyword(resource_type) for resource_type in types])
        header = pp.And(
            [
                CREATE,
                pp.Optional(OR_REPLACE),
                pp.Optional(TEMPORARY),
                ...,
                resource_type_token.set_results_name("resource_type"),
                pp.Optional(IF_NOT_EXISTS),
                ScopedIdentifier.set_results_name("resource_name"),
                REST_OF_STRING.set_results_name("remainder"),
            ]
        )
        try:
            parsed = header.parse_string(sql)
        except pp.ParseException as e:
            print("❌", "failed to parse header")
            return None
        resource_name = parsed.resource_name
        resource_type = parsed.resource_type.upper()
        [header_props] = parsed._skipped
        props_sql = (header_props + " " + parsed.remainder).strip()

        resource_cls = cls._resolve_class(resource_type, props_sql)
        try:
            props = resource_cls.props.parse(props_sql) if props_sql else {}
        except Exception as e:
            print(e)
            print("❌", "failed to parse props", resource_cls, props_sql)
            return None
            # raise e
        return resource_cls(name=resource_name, **props)

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
        resource_type = self.resource_type.lower().replace(" ", "_")
        return URN(resource_type=resource_type, name=self.fully_qualified_name)

    def __format__(self, format_spec):
        add_ref(self)
        return self.fully_qualified_name

    # def finalize(self):
    #     pass


class AccountScoped(BaseModel):
    namespace: ClassVar[Namespace] = Namespace.ACCOUNT
    account: SerializeAsAny[Resource] = None

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
        resource_type = self.resource_type.lower().replace(" ", "_")
        return URN(region, account, resource_type, self.fully_qualified_name)

    def finalize(self):
        # super().finalize()
        if self.account is None:
            raise Exception(f"AccountScoped resource {self} has no account")


class DatabaseScoped(BaseModel):
    namespace: ClassVar[Namespace] = Namespace.DATABASE
    database: SerializeAsAny[Resource] = None

    @property
    def fully_qualified_name(self):
        database = self.database.name if self.database else "[NULL]"
        return ".".join([database, self.name.upper()])

    def finalize(self):
        # super().finalize()
        if self.database is None:
            raise Exception(f"DatabaseScoped resource {self} has no database")


class SchemaScoped(BaseModel):
    namespace: ClassVar[Namespace] = Namespace.SCHEMA
    schema_: Resource = Field(alias="schema", default=None)
    # _schema: Resource = Field(alias="schema", default=None)

    @property
    def fully_qualified_name(self):
        schema = self.schema_.name if self.schema_ else "[NULL]"
        return ".".join([schema, self.name.upper()])

    def finalize(self):
        if self.schema_ is None:
            raise Exception(f"SchemaScoped resource {self} has no schema")


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
