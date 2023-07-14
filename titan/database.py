from __future__ import annotations

import re

from typing import Union, Optional, List, Tuple, Dict, ClassVar

from pydantic import BaseModel, ConfigDict


from .resource import DatabaseLevelResource, ResourceDB
from .schema import Schema
from .props import IntProp, StringProp, TagsProp, FlagProp

from .resource2 import Resource


class Database(ResourcePydantic):
    model_config = ConfigDict(from_attributes=True, extra="forbid")

    resource_name = "DATABASE"
    ownable = True
    # namespace = Namespace.ACCOUNT

    # create_stmt = StatementParser(
    #     _="CREATE",
    #     or_replace="[ OR REPLACE ]",
    #     transient="[ TRANSIENT ]",
    #     database="DATABASE",
    #     name="<name>",
    #     props="<props>",
    # )
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
    data_retention_time_in_days: int = None
    max_data_extension_time_in_days: int = None
    default_ddl_collation: str = None
    tags: Dict[str, str] = {}
    comment: str = None

    _schemas: ResourceDB

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

    def model_post_init(self, ctx):
        self._schemas = ResourceDB(Schema)
        self.add(
            Schema("PUBLIC", implicit=True),
            Schema("INFORMATION_SCHEMA", implicit=True),
        )

    @property
    def schemas(self):
        return self._schemas

    # create_statement = re.compile(
    #     rf"""
    #         CREATE\s+
    #         (?:OR\s+REPLACE\s+)?
    #         (?:TRANSIENT\s+)?
    #         DATABASE\s+
    #         (?:IF\s+NOT\s+EXISTS\s+)?
    #         ({Identifier.pattern})
    #     """,
    #     re.VERBOSE | re.IGNORECASE,
    # )

    def add(self, *other_resources: DatabaseLevelResource):
        for other_resource in other_resources:
            if not isinstance(other_resource, DatabaseLevelResource):
                raise TypeError(f"Cannot add {other_resource} to {self}")
            other_resource.database = self
            if isinstance(other_resource, Schema):
                self.schemas[other_resource.name] = other_resource
            # elif isinstance(other_resource, DatabaseRole):
            #     self.database_roles[other_resource.name] = other_resource
            else:
                raise TypeError(f"Cannot add {other_resource} to {self}")


# Lookup
# db = titan.Database.all["ADMIN"]
# db = titan.Database.all.get("ADMIN")
# for db in titan.Database.all:
#    ...


# db = ...
# db.schemas["PUBLIC"].tables["ORDERS"].columns["ORDER_ID"].grant("READ", "role:ACCOUNTADMIN")
# db.resources["PUBLIC.ORDERS"]
# for schema in db.schemas:
#   ...

# Create
# db = titan.Database.from_sql("CREATE DATABASE ADMIN").create()
# db = titan.Database(name="ADMIN").create()
