import re

from typing import Optional, List, Tuple

from .resource import DatabaseLevelResource, SchemaLevelResource, ResourceDB
from .props import IntProp, StringProp, TagsProp, FlagProp, Identifier

from .dynamic_table import DynamicTable
from .file_format import FileFormat
from .pipe import Pipe
from .sproc import Sproc
from .stage import Stage
from .table import Table
from .view import View


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
        "with_managed_access": FlagProp("WITH MANAGED ACCESS"),
        "data_retention_time_in_days": IntProp("DATA_RETENTION_TIME_IN_DAYS"),
        "max_data_extension_time_in_days": IntProp("MAX_DATA_EXTENSION_TIME_IN_DAYS"),
        "default_ddl_collation": StringProp("DEFAULT_DDL_COLLATION"),
        "tags": TagsProp(),
        "comment": StringProp("COMMENT"),
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
        name: str,
        with_managed_access: Optional[bool] = None,
        data_retention_time_in_days: Optional[int] = None,
        max_data_extension_time_in_days: Optional[int] = None,
        default_ddl_collation: Optional[str] = None,
        tags: List[Tuple[str, str]] = [],
        comment: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self.with_managed_access = with_managed_access
        self.data_retention_time_in_days = data_retention_time_in_days
        self.max_data_extension_time_in_days = max_data_extension_time_in_days
        self.default_ddl_collation = default_ddl_collation
        self.tags = tags
        self.comment = comment

        self.dynamic_tables = ResourceDB(DynamicTable)
        self.file_formats = ResourceDB(FileFormat)
        self.pipes = ResourceDB(Pipe)
        self.sprocs = ResourceDB(Sproc)
        self.stages = ResourceDB(Stage)
        self.tables = ResourceDB(Table)
        self.views = ResourceDB(View)

    @property
    def sql(self):
        return f"""
            CREATE SCHEMA {self.fully_qualified_name}
            {self.props["WITH_MANAGED_ACCESS"].render(self.with_managed_access)}
            {self.props["DATA_RETENTION_TIME_IN_DAYS"].render(self.data_retention_time_in_days)}
            {self.props["MAX_DATA_EXTENSION_TIME_IN_DAYS"].render(self.max_data_extension_time_in_days)}
            {self.props["DEFAULT_DDL_COLLATION"].render(self.default_ddl_collation)}
            {self.props["TAGS"].render(self.tags)}
            {self.props["COMMENT"].render(self.comment)}
        """.strip()

    def add(self, *other_resources: SchemaLevelResource):
        for other_resource in other_resources:
            if not isinstance(other_resource, SchemaLevelResource):
                raise TypeError(f"Cannot add {other_resource} to {self}")
            other_resource.schema = self
