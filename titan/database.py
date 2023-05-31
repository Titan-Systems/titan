from enum import Enum

from typing import Union, Optional

from .entity import AccountLevelEntity

from .helpers import ParsableEnum

from .schema import Schema


# class WarehouseType(ParsableEnum):
#     STANDARD = "STANDARD"
#     SNOWPARK_OPTIMIZED = "SNOWPARK-OPTIMIZED"


# WAREHOUSE_TYPE_T = Union[None, str, WarehouseType]


# class WarehouseSize(ParsableEnum):
#     XSMALL = "XSMALL"
#     SMALL = "SMALL"
#     MEDIUM = "MEDIUM"
#     LARGE = "LARGE"
#     XLARGE = "XLARGE"
#     XXLARGE = "XXLARGE"
#     XXXLARGE = "XXXLARGE"
#     X4LARGE = "X4LARGE"
#     X5LARGE = "X5LARGE"
#     X6LARGE = "X6LARGE"


# WAREHOUSE_SIZE_T = Union[None, str, WarehouseSize]


class Database(AccountLevelEntity):
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

    def __init__(
        self,
        # name,
        # query_text=None,
        # implicit: bool = False,
        data_retention_time_in_days: int = None,
        max_data_extension_time_in_days: int = None,
        default_ddl_collation: str = None,
        comment: str = None,
        **kwargs,
    ):
        # query_text = query_text or f"CREATE DATABASE {name.upper()}"
        # super().__init__(name=name, query_text=query_text)
        super().__init__(**kwargs)
        self.data_retention_time_in_days = data_retention_time_in_days
        self.max_data_extension_time_in_days = max_data_extension_time_in_days
        self.default_ddl_collation = default_ddl_collation
        self.comment = comment

    def schema(self, schemaname):
        # table = Table(name=tablename, database=self, schema=self.implicit_schema, implicit=True)
        if schemaname != "PUBLIC":
            raise NotImplementedError
        public_schema = Schema(name=schemaname, database=self, implicit=True)

        # TODO: there needs to be a way for share to bring its ridealongs
        if self.graph:
            self.graph.add(public_schema)

        return public_schema
