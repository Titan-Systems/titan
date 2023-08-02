from typing import Dict, Union

from . import Resource
from .base import SchemaScoped
from ..enums import ParseableEnum
from ..props import Props, FlagProp, StringProp, BoolProp, AtBeforeProp
from ..parse import _resolve_resource_class


class StreamType(ParseableEnum):
    TABLE = "TABLE"
    EXTERNAL_TABLE = "EXTERNAL TABLE"
    STAGE = "STAGE"
    VIEW = "VIEW"


class TableStream(Resource, SchemaScoped):
    """
    -- table
    CREATE [ OR REPLACE ] STREAM [IF NOT EXISTS]
      <name>
      [ COPY GRANTS ]
      ON TABLE <table_name>
      [ { AT | BEFORE } ( { TIMESTAMP => <timestamp> | OFFSET => <time_difference> | STATEMENT => <id> | STREAM => '<name>' } ) ]
      [ APPEND_ONLY = TRUE | FALSE ]
      [ SHOW_INITIAL_ROWS = TRUE | FALSE ]
      [ COMMENT = '<string_literal>' ]

    """

    resource_type = "STREAM"
    props = Props(
        copy_grants=FlagProp("copy grants"),
        on_table=StringProp("on table"),
        at_before=AtBeforeProp(),
        append_only=BoolProp("append_only"),
        show_initial_rows=BoolProp("show_initial_rows"),
        comment=StringProp("comment"),
    )

    name: str
    owner: str = None
    copy_grants: bool = None
    at_before: Dict[str, str] = None
    on_table: str
    append_only: bool = None
    show_initial_rows: bool = None
    comment: str = None


class ExternalTableStream(Resource, SchemaScoped):
    """
    -- External table
    CREATE [ OR REPLACE ] STREAM [IF NOT EXISTS]
      <name>
      [ COPY GRANTS ]
      ON EXTERNAL TABLE <external_table_name>
      [ { AT | BEFORE } ( { TIMESTAMP => <timestamp> | OFFSET => <time_difference> | STATEMENT => <id> | STREAM => '<name>' } ) ]
      [ INSERT_ONLY = TRUE ]
      [ COMMENT = '<string_literal>' ]
    """

    resource_type = "STREAM"
    props = Props(
        copy_grants=FlagProp("copy grants"),
        on_external_table=StringProp("on external table"),
        at_before=AtBeforeProp(),
        insert_only=BoolProp("insert_only"),
        comment=StringProp("comment"),
    )

    name: str
    owner: str = None
    copy_grants: bool = None
    on_external_table: str
    at_before: Dict[str, str] = None
    insert_only: bool = None
    comment: str = None


class StageStream(Resource, SchemaScoped):
    """
    -- Directory table
    CREATE [ OR REPLACE ] STREAM [IF NOT EXISTS]
      <name>
      [ COPY GRANTS ]
      ON STAGE <stage_name>
      [ COMMENT = '<string_literal>' ]
    """

    resource_type = "STREAM"
    props = Props(
        copy_grants=FlagProp("copy grants"),
        on_stage=StringProp("on stage"),
        comment=StringProp("comment"),
    )

    name: str
    owner: str = None
    copy_grants: bool = None
    on_stage: str
    comment: str = None


class ViewStream(Resource, SchemaScoped):
    """
    -- View
    CREATE [ OR REPLACE ] STREAM [IF NOT EXISTS]
      <name>
      [ COPY GRANTS ]
      ON VIEW <view_name>
      [ { AT | BEFORE } ( { TIMESTAMP => <timestamp> | OFFSET => <time_difference> | STATEMENT => <id> | STREAM => '<name>' } ) ]
      [ APPEND_ONLY = TRUE | FALSE ]
      [ SHOW_INITIAL_ROWS = TRUE | FALSE ]
      [ COMMENT = '<string_literal>' ]
    """

    resource_type = "STREAM"
    props = Props(
        copy_grants=FlagProp("copy grants"),
        on_view=StringProp("on view"),
        at_before=AtBeforeProp(),
        append_only=BoolProp("append_only"),
        show_initial_rows=BoolProp("show_initial_rows"),
        comment=StringProp("comment"),
    )

    name: str
    owner: str = None
    copy_grants: bool = None
    on_view: str
    at_before: Dict[str, str] = None
    append_only: bool = None
    show_initial_rows: bool = None
    comment: str = None


StreamTypeMap = {
    StreamType.TABLE: TableStream,
    StreamType.EXTERNAL_TABLE: ExternalTableStream,
    StreamType.STAGE: StageStream,
    StreamType.VIEW: ViewStream,
}


class Stream(Resource):
    def __init__(self, *args, **kwargs):
        raise NotImplementedError

    def __new__(
        cls, type: Union[str, StreamType], **kwargs
    ) -> Union[TableStream, ExternalTableStream, StageStream, ViewStream]:
        file_type = StreamType.parse(type)
        file_type_cls = StreamTypeMap[file_type]
        return file_type_cls(**kwargs)

    @classmethod
    def from_sql(cls, sql):
        resource_cls = Resource.classes[_resolve_resource_class(sql)]
        return resource_cls.from_sql(sql)
