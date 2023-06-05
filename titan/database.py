from enum import Enum

from typing import Union, Optional, List, Tuple

from sqlglot import exp

from .resource import AccountLevelResource

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

    props = {"DATA_RETENTION_TIME_IN_DAYS": int}

    def __init__(
        self,
        data_retention_time_in_days: Optional[int] = None,
        max_data_extension_time_in_days: Optional[int] = None,
        default_ddl_collation: Optional[str] = None,
        tags: List[Tuple[str, str]] = [],
        comment: Optional[str] = None,
        **kwargs,
    ):
        # query_text = query_text or f"CREATE DATABASE {name.upper()}"
        # super().__init__(name=name, query_text=query_text)
        super().__init__(**kwargs)
        self.data_retention_time_in_days = data_retention_time_in_days
        self.max_data_extension_time_in_days = max_data_extension_time_in_days
        self.default_ddl_collation = default_ddl_collation
        self.comment = comment
        self.schemas = {}
        public_schema = Schema(name="PUBLIC", database=self, implicit=True)
        self.schemas["PUBLIC"] = public_schema

    @classmethod
    def from_expression(cls, expression: exp.Create):
        name = expression.this.this.this
        data_retention_time_in_days = None
        max_data_extension_time_in_days = None
        default_ddl_collation = None
        tags = []
        comment = None

        if "properties" in expression.args and expression.args["properties"] is not None:
            for prop in expression.args["properties"].expressions:
                if isinstance(prop, exp.SchemaCommentProperty):
                    comment = prop.this
                elif isinstance(prop, exp.Property):
                    prop_name = prop.this.this.lower()
                    prop_value = prop.args["value"]
                    if prop_name == "data_retention_time_in_days":
                        data_retention_time_in_days = prop_value
                    elif prop_name == "max_data_extension_time_in_days":
                        max_data_extension_time_in_days = prop_value
                    elif prop_name == "default_ddl_collation":
                        default_ddl_collation = prop_value

        return cls(
            name=name,
            data_retention_time_in_days=data_retention_time_in_days,
            max_data_extension_time_in_days=max_data_extension_time_in_days,
            default_ddl_collation=default_ddl_collation,
            tags=tags,
            comment=comment,
        )

    # def schema(self, schemaname):
    #     # table = Table(name=tablename, database=self, schema=self.implicit_schema, implicit=True)
    #     if schemaname != "PUBLIC":
    #         raise NotImplementedError

    #     # TODO: there needs to be a way for share to bring its ridealongs
    #     if self.graph:
    #         self.graph.add(public_schema)

    #     return public_schema


def parse_database(str_or_database: Union[None, str, Database]) -> Optional[Database]:
    if isinstance(str_or_database, Database):
        return str_or_database
    elif type(str_or_database) == str:
        return Database(name=str_or_database)
    else:
        return None
