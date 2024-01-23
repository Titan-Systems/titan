from dataclasses import dataclass
from typing import Union

from .resource import Resource, ResourceSpec
from ..enums import ParseableEnum, ResourceType
from ..scope import SchemaScope
from ..props import Props, FlagProp, StringProp, BoolProp, TimeTravelProp


class StreamType(ParseableEnum):
    TABLE = "TABLE"
    EXTERNAL_TABLE = "EXTERNAL TABLE"
    STAGE = "STAGE"
    VIEW = "VIEW"


@dataclass
class _TableStream(ResourceSpec):
    name: str
    on_table: str
    owner: str = "SYSADMIN"
    copy_grants: bool = None
    at: dict[str, str] = None
    before: dict[str, str] = None
    append_only: bool = None
    show_initial_rows: bool = None
    comment: str = None


class TableStream(Resource):
    """
    CREATE [ OR REPLACE ] STREAM [IF NOT EXISTS]
      <name>
      [ COPY GRANTS ]
      ON TABLE <table_name>
      [ { AT | BEFORE } ( { TIMESTAMP => <timestamp> | OFFSET => <time_difference> | STATEMENT => <id> | STREAM => '<name>' } ) ]
      [ APPEND_ONLY = TRUE | FALSE ]
      [ SHOW_INITIAL_ROWS = TRUE | FALSE ]
      [ COMMENT = '<string_literal>' ]

    """

    resource_type = ResourceType.STREAM
    props = Props(
        copy_grants=FlagProp("copy grants"),
        on_table=StringProp("on table", eq=False),
        at=TimeTravelProp("at"),
        before=TimeTravelProp("before"),
        append_only=BoolProp("append_only"),
        show_initial_rows=BoolProp("show_initial_rows"),
        comment=StringProp("comment"),
    )
    scope = SchemaScope()
    spec = _TableStream

    def __init__(
        self,
        name: str,
        on_table: str,
        owner: str = "SYSADMIN",
        copy_grants: bool = None,
        at: dict[str, str] = None,
        before: dict[str, str] = None,
        append_only: bool = None,
        show_initial_rows: bool = None,
        comment: str = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._data = _TableStream(
            name=name,
            on_table=on_table,
            owner=owner,
            copy_grants=copy_grants,
            at=at,
            before=before,
            append_only=append_only,
            show_initial_rows=show_initial_rows,
            comment=comment,
        )


@dataclass
class _ExternalTableStream(ResourceSpec):
    name: str
    on_external_table: str
    owner: str = "SYSADMIN"
    copy_grants: bool = None
    at: dict[str, str] = None
    before: dict[str, str] = None
    insert_only: bool = None
    comment: str = None


class ExternalTableStream(Resource):
    """
    CREATE [ OR REPLACE ] STREAM [IF NOT EXISTS]
      <name>
      [ COPY GRANTS ]
      ON EXTERNAL TABLE <external_table_name>
      [ { AT | BEFORE } ( { TIMESTAMP => <timestamp> | OFFSET => <time_difference> | STATEMENT => <id> | STREAM => '<name>' } ) ]
      [ INSERT_ONLY = TRUE ]
      [ COMMENT = '<string_literal>' ]
    """

    resource_type = ResourceType.STREAM
    props = Props(
        copy_grants=FlagProp("copy grants"),
        on_external_table=StringProp("on external table", eq=False),
        at=TimeTravelProp("at"),
        before=TimeTravelProp("before"),
        insert_only=BoolProp("insert_only"),
        comment=StringProp("comment"),
    )
    scope = SchemaScope()
    spec = _ExternalTableStream

    def __init__(
        self,
        name: str,
        on_external_table: str,
        owner: str = "SYSADMIN",
        copy_grants: bool = None,
        at: dict[str, str] = None,
        before: dict[str, str] = None,
        insert_only: bool = None,
        comment: str = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._data = _ExternalTableStream(
            name=name,
            on_external_table=on_external_table,
            owner=owner,
            copy_grants=copy_grants,
            at=at,
            before=before,
            insert_only=insert_only,
            comment=comment,
        )


@dataclass
class _StageStream(ResourceSpec):
    name: str
    on_stage: str
    owner: str = "SYSADMIN"
    copy_grants: bool = None
    comment: str = None


class StageStream(Resource):
    """
    -- Directory table
    CREATE [ OR REPLACE ] STREAM [IF NOT EXISTS]
      <name>
      [ COPY GRANTS ]
      ON STAGE <stage_name>
      [ COMMENT = '<string_literal>' ]
    """

    resource_type = ResourceType.STREAM
    props = Props(
        copy_grants=FlagProp("copy grants"),
        on_stage=StringProp("on stage", eq=False),
        comment=StringProp("comment"),
    )
    scope = SchemaScope()
    spec = _StageStream

    def __init__(
        self,
        name: str,
        on_stage: str,
        owner: str = "SYSADMIN",
        copy_grants: bool = None,
        comment: str = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._data = _StageStream(
            name=name,
            on_stage=on_stage,
            owner=owner,
            copy_grants=copy_grants,
            comment=comment,
        )


@dataclass
class _ViewStream(ResourceSpec):
    name: str
    on_view: str
    owner: str = "SYSADMIN"
    copy_grants: bool = None
    at: dict[str, str] = None
    before: dict[str, str] = None
    append_only: bool = None
    show_initial_rows: bool = None
    comment: str = None


class ViewStream(Resource):
    """
    CREATE [ OR REPLACE ] STREAM [IF NOT EXISTS]
      <name>
      [ COPY GRANTS ]
      ON VIEW <view_name>
      [ { AT | BEFORE } ( { TIMESTAMP => <timestamp> | OFFSET => <time_difference> | STATEMENT => <id> | STREAM => '<name>' } ) ]
      [ APPEND_ONLY = TRUE | FALSE ]
      [ SHOW_INITIAL_ROWS = TRUE | FALSE ]
      [ COMMENT = '<string_literal>' ]
    """

    resource_type = ResourceType.STREAM
    props = Props(
        copy_grants=FlagProp("copy grants"),
        on_view=StringProp("on view", eq=False),
        at=TimeTravelProp("at"),
        before=TimeTravelProp("before"),
        append_only=BoolProp("append_only"),
        show_initial_rows=BoolProp("show_initial_rows"),
        comment=StringProp("comment"),
    )
    scope = SchemaScope()
    spec = _ViewStream

    def __init__(
        self,
        name: str,
        on_view: str,
        owner: str = "SYSADMIN",
        copy_grants: bool = None,
        at: dict[str, str] = None,
        before: dict[str, str] = None,
        append_only: bool = None,
        show_initial_rows: bool = None,
        comment: str = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._data = _ViewStream(
            name=name,
            on_view=on_view,
            owner=owner,
            copy_grants=copy_grants,
            at=at,
            before=before,
            append_only=append_only,
            show_initial_rows=show_initial_rows,
            comment=comment,
        )


StreamTypeMap = {
    StreamType.TABLE: TableStream,
    StreamType.EXTERNAL_TABLE: ExternalTableStream,
    StreamType.STAGE: StageStream,
    StreamType.VIEW: ViewStream,
}


class Stream:
    def __new__(
        cls, type: Union[str, StreamType], **kwargs
    ) -> Union[TableStream, ExternalTableStream, StageStream, ViewStream]:
        if isinstance(type, str):
            type = StreamType(type)

        file_type_cls = StreamTypeMap[type]
        return file_type_cls(**kwargs)

    # @classmethod
    # def from_sql(cls, sql):
    #     resource_cls = Resource.classes[_resolve_resource_class(sql)]
    #     return resource_cls.from_sql(sql)
