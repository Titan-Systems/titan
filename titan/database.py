from __future__ import annotations

import re

from typing import Union, Optional, List, Tuple

from .resource import AccountLevelResource, DatabaseLevelResource, ResourceDB
from .schema import Schema
from .props import IntProp, StringProp, TagsProp, Identifier


class Database(AccountLevelResource):
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

    props = {
        "DATA_RETENTION_TIME_IN_DAYS": IntProp("DATA_RETENTION_TIME_IN_DAYS"),
        "MAX_DATA_EXTENSION_TIME_IN_DAYS": IntProp("MAX_DATA_EXTENSION_TIME_IN_DAYS"),
        "DEFAULT_DDL_COLLATION": StringProp("DEFAULT_DDL_COLLATION"),
        "TAGS": TagsProp(),
        "COMMENT": StringProp("COMMENT"),
    }

    create_statement = re.compile(
        rf"""
            CREATE\s+
            (?:OR\s+REPLACE\s+)?
            (?:TRANSIENT\s+)?
            DATABASE\s+
            (?:IF\s+NOT\s+EXISTS\s+)?
            ({Identifier.pattern})
        """,
        re.VERBOSE | re.IGNORECASE,
    )

    ownable = True

    def __init__(
        self,
        name: str,
        data_retention_time_in_days: Optional[int] = None,
        max_data_extension_time_in_days: Optional[int] = None,
        default_ddl_collation: Optional[str] = None,
        tags: List[Tuple[str, str]] = [],
        comment: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self.data_retention_time_in_days = data_retention_time_in_days
        self.max_data_extension_time_in_days = max_data_extension_time_in_days
        self.default_ddl_collation = default_ddl_collation
        self.tags = tags
        self.comment = comment

        # self.database_roles = ResourceDB(DatabaseRole)
        self.schemas = ResourceDB(Schema)

        self.add(
            Schema("PUBLIC", implicit=True),
            Schema("INFORMATION_SCHEMA", implicit=True),
        )

    @property
    def sql(self):
        return f"""
            CREATE DATABASE {self.fully_qualified_name}
            {self.props["DATA_RETENTION_TIME_IN_DAYS"].render(self.data_retention_time_in_days)}
            {self.props["MAX_DATA_EXTENSION_TIME_IN_DAYS"].render(self.max_data_extension_time_in_days)}
            {self.props["DEFAULT_DDL_COLLATION"].render(self.default_ddl_collation)}
            {self.props["TAGS"].render(self.tags)}
            {self.props["COMMENT"].render(self.comment)}
        """.strip()

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
