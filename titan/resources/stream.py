from dataclasses import dataclass, field

from ..enums import ParseableEnum, ResourceType
from ..props import BoolProp, FlagProp, IdentifierProp, Props, StringProp, TimeTravelProp
from ..resource_name import ResourceName
from ..role_ref import RoleRef
from ..scope import SchemaScope
from .resource import NamedResource, Resource, ResourcePointer, ResourceSpec
from .table import Table
from .view import View


class StreamType(ParseableEnum):
    TABLE = "TABLE"
    EXTERNAL_TABLE = "EXTERNAL TABLE"
    STAGE = "STAGE"
    VIEW = "VIEW"


@dataclass(unsafe_hash=True)
class _TableStream(ResourceSpec):
    name: ResourceName
    on_table: Table
    owner: RoleRef = "SYSADMIN"
    copy_grants: bool = field(default=None, metadata={"fetchable": False})
    at: dict[str, str] = field(default=None, metadata={"fetchable": False})
    before: dict[str, str] = field(default=None, metadata={"fetchable": False})
    append_only: bool = False
    show_initial_rows: bool = field(default=None, metadata={"fetchable": False})
    comment: str = None

    def __post_init__(self):
        super().__post_init__()
        if self.at:
            self.at = {k.lower(): v for k, v in self.at.items()}
        if self.before:
            self.before = {k.lower(): v for k, v in self.before.items()}


class TableStream(NamedResource, Resource):
    """
    Description:
        Represents a stream on a table in Snowflake, which allows for change data capture on the table.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-stream

    Fields:
        name (string, required): The name of the stream.
        on_table (string, required): The name of the table the stream is based on.
        owner (string or Role): The role that owns the stream. Defaults to "SYSADMIN".
        copy_grants (bool): Whether to copy grants from the source table to the stream.
        at (dict): A dictionary specifying the point in time for the stream to start, using keys like TIMESTAMP, OFFSET, STATEMENT, or STREAM.
        before (dict): A dictionary specifying the point in time for the stream to start, similar to 'at' but defining a point before the specified time.
        append_only (bool): If set to True, the stream records only append operations.
        show_initial_rows (bool): If set to True, the stream includes the initial rows of the table at the time of stream creation.
        comment (string): An optional description for the stream.

    Python:

        ```python
        stream = TableStream(
            name="some_stream",
            on_table="some_table",
            owner="SYSADMIN",
            copy_grants=True,
            at={"TIMESTAMP": "2022-01-01 00:00:00"},
            before={"STREAM": "some_other_stream"},
            append_only=False,
            show_initial_rows=True,
            comment="This is a sample stream."
        )
        ```

    Yaml:

        ```yaml
        streams:
          - name: some_stream
            on_table: some_table
            owner: SYSADMIN
            copy_grants: true
            at:
              TIMESTAMP: "2022-01-01 00:00:00"
            before:
              STREAM: some_other_stream
            append_only: false
            show_initial_rows: true
            comment: This is a sample stream.
        ```
    """

    resource_type = ResourceType.STREAM
    props = Props(
        copy_grants=FlagProp("copy grants"),
        on_table=IdentifierProp("on table", eq=False),
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
        append_only: bool = False,
        show_initial_rows: bool = None,
        comment: str = None,
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._data: _TableStream = _TableStream(
            name=self._name,
            on_table=on_table,
            owner=owner,
            copy_grants=copy_grants,
            at=at,
            before=before,
            append_only=append_only,
            show_initial_rows=show_initial_rows,
            comment=comment,
        )
        self.requires(self._data.on_table)
        if self._data.at and "stream" in self._data.at:
            self.requires(ResourcePointer(name=self._data.at["stream"], resource_type=ResourceType.STREAM))
        if self._data.before and "stream" in self._data.before:
            self.requires(ResourcePointer(name=self._data.before["stream"], resource_type=ResourceType.STREAM))


# @dataclass(unsafe_hash=True)
# class _ExternalTableStream(ResourceSpec):
#     name: str
#     on_external_table: str
#     owner: str = "SYSADMIN"
#     copy_grants: bool = None
#     at: dict[str, str] = None
#     before: dict[str, str] = None
#     insert_only: bool = None
#     comment: str = None


# class ExternalTableStream(Resource):
#     """
#     CREATE [ OR REPLACE ] STREAM [IF NOT EXISTS]
#       <name>
#       [ COPY GRANTS ]
#       ON EXTERNAL TABLE <external_table_name>
#       [ { AT | BEFORE } ( { TIMESTAMP => <timestamp> | OFFSET => <time_difference> | STATEMENT => <id> | STREAM => '<name>' } ) ]
#       [ INSERT_ONLY = TRUE ]
#       [ COMMENT = '<string_literal>' ]
#     """

#     resource_type = ResourceType.STREAM
#     props = Props(
#         copy_grants=FlagProp("copy grants"),
#         on_external_table=IdentifierProp("on external table", eq=False),
#         at=TimeTravelProp("at"),
#         before=TimeTravelProp("before"),
#         insert_only=BoolProp("insert_only"),
#         comment=StringProp("comment"),
#     )
#     scope = SchemaScope()
#     spec = _ExternalTableStream

#     def __init__(
#         self,
#         name: str,
#         on_external_table: str,
#         owner: str = "SYSADMIN",
#         copy_grants: bool = None,
#         at: dict[str, str] = None,
#         before: dict[str, str] = None,
#         insert_only: bool = None,
#         comment: str = None,
#         **kwargs,
#     ):
#         super().__init__(**kwargs)
#         self._data = _ExternalTableStream(
#             name=name,
#             on_external_table=on_external_table,
#             owner=owner,
#             copy_grants=copy_grants,
#             at=at,
#             before=before,
#             insert_only=insert_only,
#             comment=comment,
#         )


@dataclass(unsafe_hash=True)
class _StageStream(ResourceSpec):
    name: ResourceName
    on_stage: str
    owner: RoleRef = "SYSADMIN"
    copy_grants: bool = field(default=None, metadata={"fetchable": False})
    comment: str = None


class StageStream(NamedResource, Resource):
    """
    Description:
        Represents a stream on a stage in Snowflake, which allows for capturing data changes on the stage.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-stream

    Fields:
        name (string, required): The name of the stream.
        on_stage (string, required): The name of the stage the stream is based on.
        owner (string or Role): The role that owns the stream. Defaults to "SYSADMIN".
        copy_grants (bool): Whether to copy grants from the source stage to the stream.
        comment (string): An optional description for the stream.

    Python:

        ```python
        stream = StageStream(
            name="some_stream",
            on_stage="some_stage",
            owner="SYSADMIN",
            copy_grants=True,
            comment="This is a sample stream."
        )
        ```

    Yaml:

        ```yaml
        streams:
          - name: some_stream
            on_stage: some_stage
            owner: SYSADMIN
            copy_grants: true
            comment: This is a sample stream.
        ```
    """

    resource_type = ResourceType.STREAM
    props = Props(
        copy_grants=FlagProp("copy grants"),
        on_stage=IdentifierProp("on stage", eq=False),
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
        super().__init__(name, **kwargs)
        self._data: _StageStream = _StageStream(
            name=self._name,
            on_stage=on_stage,
            owner=owner,
            copy_grants=copy_grants,
            comment=comment,
        )
        self.requires(ResourcePointer(name=self._data.on_stage, resource_type=ResourceType.STAGE))


@dataclass(unsafe_hash=True)
class _ViewStream(ResourceSpec):
    name: ResourceName
    on_view: View
    owner: RoleRef = "SYSADMIN"
    copy_grants: bool = field(default=None, metadata={"fetchable": False})
    at: dict[str, str] = field(default=None, metadata={"fetchable": False})
    before: dict[str, str] = field(default=None, metadata={"fetchable": False})
    append_only: bool = False
    show_initial_rows: bool = field(default=None, metadata={"fetchable": False})
    comment: str = None


class ViewStream(NamedResource, Resource):
    """
    Description:
        Represents a stream on a view in Snowflake, allowing for real-time data processing and querying.
        This stream can be configured with various options such as time travel, append-only mode, and initial row visibility.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-stream

    Fields:
        name (string, required): The name of the stream.
        on_view (string, required): The name of the view the stream is based on.
        owner (string or Role): The role that owns the stream. Defaults to 'SYSADMIN'.
        copy_grants (bool): Whether to copy grants from the view to the stream.
        at (dict): A dictionary specifying the point in time for the stream to start, using keys like TIMESTAMP, OFFSET, STATEMENT, or STREAM.
        before (dict): A dictionary specifying the point in time for the stream to start, similar to 'at' but defining a point before the specified time.
        append_only (bool): If set to True, the stream records only append operations.
        show_initial_rows (bool): If set to True, the stream includes the initial rows of the view at the time of stream creation.
        comment (string): An optional description for the stream.

    Python:

        ```python
        view_stream = ViewStream(
            name="some_stream",
            on_view="some_view",
            owner="SYSADMIN",
            copy_grants=True,
            at={"TIMESTAMP": "2022-01-01 00:00:00"},
            before={"STREAM": "some_other_stream"},
            append_only=False,
            show_initial_rows=True,
            comment="This is a sample stream on a view."
        )
        ```

    Yaml:

        ```yaml
        streams:
          - name: some_stream
            on_view: some_view
            owner: SYSADMIN
            copy_grants: true
            at:
              TIMESTAMP: "2022-01-01 00:00:00"
            before:
              STREAM: some_other_stream
            append_only: false
            show_initial_rows: true
            comment: This is a sample stream on a view.
        ```
    """

    resource_type = ResourceType.STREAM
    props = Props(
        copy_grants=FlagProp("copy grants"),
        on_view=IdentifierProp("on view", eq=False),
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
        super().__init__(name, **kwargs)
        self._data: _ViewStream = _ViewStream(
            name=self._name,
            on_view=on_view,
            owner=owner,
            copy_grants=copy_grants,
            at=at,
            before=before,
            append_only=append_only,
            show_initial_rows=show_initial_rows,
            comment=comment,
        )
        self.requires(self._data.on_view)


StreamTypeMap = {
    StreamType.TABLE: TableStream,
    # StreamType.EXTERNAL_TABLE: ExternalTableStream,
    StreamType.STAGE: StageStream,
    StreamType.VIEW: ViewStream,
}


def _resolver(data: dict):
    if "on_table" in data:
        return TableStream
    # elif "on_external_table" in data:
    #     return ExternalTableStream
    elif "on_stage" in data:
        return StageStream
    elif "on_view" in data:
        return ViewStream
    # using this as a workaround because there may not be enough properties during a small change to disambiguate
    # really the different stream types should probably have seperate resource types.
    # Either that, or the resolver would need to look at the database to see what type of stream it is
    return TableStream


Resource.__resolvers__[ResourceType.STREAM] = _resolver
