import re

from typing import Optional, List, Tuple

from .resource import DatabaseLevelResource, ResourceDB

from .props import IntProp, StringProp, TagsProp, FlagProp, Identifier


class Schema(DatabaseLevelResource):
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

    props = {
        "WITH_MANAGED_ACCESS": FlagProp("WITH MANAGED ACCESS"),
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
            SCHEMA\s+
            (?:IF\s+NOT\s+EXISTS\s+)?
            ({Identifier.pattern})
        """,
        re.VERBOSE | re.IGNORECASE,
    )

    ownable = True

    def __init__(
        self,
        with_managed_access: Optional[bool] = None,
        data_retention_time_in_days: Optional[int] = None,
        max_data_extension_time_in_days: Optional[int] = None,
        default_ddl_collation: Optional[str] = None,
        tags: List[Tuple[str, str]] = [],
        comment: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.with_managed_access = with_managed_access
        self.data_retention_time_in_days = data_retention_time_in_days
        self.max_data_extension_time_in_days = max_data_extension_time_in_days
        self.default_ddl_collation = default_ddl_collation
        self.tags = tags
        self.comment = comment
        # self.tables
        # self.schemas = ResourceDB(Schema)
        # self.schemas["PUBLIC"] = Schema(name="PUBLIC", database=self, implicit=True)
