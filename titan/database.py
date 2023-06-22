from __future__ import annotations

import re

from typing import Union, Optional, List, Tuple

from .resource import AccountLevelResource, ResourceDB
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
        data_retention_time_in_days: Optional[int] = None,
        max_data_extension_time_in_days: Optional[int] = None,
        default_ddl_collation: Optional[str] = None,
        tags: List[Tuple[str, str]] = [],
        comment: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.data_retention_time_in_days = data_retention_time_in_days
        self.max_data_extension_time_in_days = max_data_extension_time_in_days
        self.default_ddl_collation = default_ddl_collation
        self.tags = tags
        self.comment = comment
        self.schemas = ResourceDB(Schema)
        self.schemas["PUBLIC"] = Schema(name="PUBLIC", database=self, implicit=True)

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
