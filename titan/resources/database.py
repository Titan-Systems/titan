from typing import Dict

from .base import Resource, AccountScoped
from ..props import Props, IntProp, StringProp, TagsProp, FlagProp

# from .schema import Schema


# class Database(Resource, AccountScoped):
#     """
#     CREATE [ OR REPLACE ] [ TRANSIENT ] DATABASE [ IF NOT EXISTS ] <name>
#         [ CLONE <source_db>
#                 [ { AT | BEFORE } ( { TIMESTAMP => <timestamp> | OFFSET => <time_difference> | STATEMENT => <id> } ) ] ]
#         [ DATA_RETENTION_TIME_IN_DAYS = <integer> ]
#         [ MAX_DATA_EXTENSION_TIME_IN_DAYS = <integer> ]
#         [ DEFAULT_DDL_COLLATION = '<collation_specification>' ]
#         [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
#         [ COMMENT = '<string_literal>' ]
#     """

#     resource_type = "DATABASE"
#     props = Props(
#         transient=FlagProp("transient"),
#         data_retention_time_in_days=IntProp("data_retention_time_in_days"),
#         max_data_extension_time_in_days=IntProp("max_data_extension_time_in_days"),
#         default_ddl_collation=StringProp("default_ddl_collation"),
#         tags=TagsProp(),
#         comment=StringProp("comment"),
#     )

#     name: str
#     transient: bool = False
#     owner: str = "SYSADMIN"
#     data_retention_time_in_days: int = 1
#     max_data_extension_time_in_days: int = 14
#     default_ddl_collation: str = None
#     tags: Dict[str, str] = None
#     comment: str = None

# _schemas: ResourceDB

# def model_post_init(self, ctx):
#     super().model_post_init(ctx)
#     self._schemas = ResourceDB(Schema)
#     self.add(
#         Schema(name="PUBLIC", implicit=True),
#         Schema(name="INFORMATION_SCHEMA", implicit=True),
#     )

# @property
# def schemas(self):
#     return self._schemas


class SharedDatabase(Resource, AccountScoped):
    """
    CREATE DATABASE
        IDENTIFIER('SNOWPARK_FOR_PYTHON__HANDSONLAB__WEATHER_DATA')
    FROM SHARE
        IDENTIFIER('WEATHERSOURCE.SNOWFLAKE_MANAGED$PUBLIC_GCP_US_CENTRAL1."WEATHERSOURCE_SNOWFLAKE_SNOWPARK_TILE_SNOWFLAKE_SECURE_SHARE_1651768630709"');
    """

    resource_type = "DATABASE"
    props = Props(
        # TODO: IdentifierProp("from share", Listing)
        from_share=StringProp("from share"),
    )

    name: str
    from_share: str
