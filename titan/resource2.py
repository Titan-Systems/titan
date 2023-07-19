from enum import Enum
from typing import ClassVar

import pyparsing as pp

from pydantic import BaseModel, Field, ConfigDict

from .props import Props

Keyword = pp.CaselessKeyword

CREATE = Keyword("CREATE").suppress()
OR_REPLACE = (Keyword("OR") + Keyword("REPLACE")).suppress()
IF_NOT_EXISTS = (Keyword("IF") + Keyword("NOT") + Keyword("EXISTS")).suppress()
TEMPORARY = (Keyword("TEMP") + Keyword("TEMPORARY")).suppress()
WITH = Keyword("WITH").suppress()

REST_OF_STRING = pp.Word(pp.printables + " \n") | pp.StringEnd() | pp.Empty()

Identifier = pp.Word(pp.alphanums + "_", pp.alphanums + "_$") | pp.dbl_quoted_string
ScopedIdentifier = Identifier


class Namespace(Enum):
    ACCOUNT = "ACCOUNT"
    DATABASE = "DATABASE"
    SCHEMA = "SCHEMA"
    TABLE = "TABLE"


resource_db = {}


class Resource(BaseModel):
    model_config = ConfigDict(from_attributes=True, extra="forbid")

    resource_type: ClassVar[str]
    namespace: ClassVar[Namespace]
    props: ClassVar[Props]

    implicit: bool = Field(exclude=True, default=False)
    stub: bool = Field(exclude=True, default=False)

    def model_post_init(self, ctx):
        resource_db[(self.__class__, self.name)] = self

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
        command = sql.split()[0].lower()
        if command != "create":
            print(f"Command {command} not supported")
            return
        props = {}

        is_generic = cls == Resource

        if is_generic:
            types = {}
            type_parsers = []
            for subcls in cls.__subclasses__():
                types[subcls.resource_type] = subcls
                type_parsers.append(
                    pp.Combine(pp.And([Keyword(tok) for tok in subcls.resource_type.split()]))
                )
            # New but not working. Fixes bug with 2+ spaces between keywords eg RESOURCE  MONITOR
            #                                                                           ^^
            # resource_type = pp.Or(type_parsers)

            # Old
            resource_type = pp.Or([Keyword(resource_type) for resource_type in types.keys()])
        else:
            types = None
            resource_type = Keyword(cls.resource_type)

        header = pp.And(
            [
                CREATE,
                pp.Optional(OR_REPLACE),
                pp.Optional(TEMPORARY),
                ...,
                resource_type.set_results_name("resource_type"),
                pp.Optional(IF_NOT_EXISTS),
                ScopedIdentifier.set_results_name("resource_name"),
                REST_OF_STRING.set_results_name("remainder"),
            ]
        )

        parsed = header.parse_string(sql)
        resource_name = parsed.resource_name
        resource_type = parsed.resource_type.upper()
        [header_props] = parsed._skipped
        props_sql = (header_props + " " + parsed.remainder).strip()

        if is_generic:
            resource_cls = types[resource_type]
        else:
            resource_cls = cls

        if props_sql:
            props = resource_cls.props.parse(props_sql)

        return resource_cls(name=resource_name, **props)


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
