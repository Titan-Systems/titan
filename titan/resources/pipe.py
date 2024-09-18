# TODO:
# the error_integration field is a reference to a notification integration resource.
# This is a critical edge case of polymorphic resources. This requires the implementation
# of ResourcePointers

from dataclasses import dataclass, field

from ..enums import ResourceType
from ..props import BoolProp, Props, QueryProp, StringProp
from ..resource_name import ResourceName
from ..role_ref import RoleRef
from ..scope import SchemaScope
from .resource import NamedResource, Resource, ResourceSpec


@dataclass(unsafe_hash=True)
class _Pipe(ResourceSpec):
    name: ResourceName
    as_: str
    owner: RoleRef = "SYSADMIN"
    auto_ingest: bool = field(default=None, metadata={"fetchable": False})
    error_integration: str = None
    aws_sns_topic: str = None
    integration: str = None
    comment: str = None


class Pipe(NamedResource, Resource):
    """
    Description:
        Represents a data ingestion pipeline in Snowflake, which automates the loading of data into tables.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-pipe

    Fields:
        name (string, required): The name of the pipe.
        as_ (string, required): The SQL statement that defines the data loading operation.
        owner (string or Role): The owner role of the pipe. Defaults to "SYSADMIN".
        auto_ingest (bool): Specifies if the pipe automatically ingests data when files are added to the stage. Defaults to None.
        error_integration (string): The name of the integration used for error notifications. Defaults to None.
        aws_sns_topic (string): The AWS SNS topic where notifications are sent. Defaults to None.
        integration (string): The integration used for data loading. Defaults to None.
        comment (string): A comment for the pipe. Defaults to None.

    Python:

        ```python
        pipe = Pipe(
            name="some_pipe",
            as_="COPY INTO some_table FROM @%some_stage",
            owner="SYSADMIN",
            auto_ingest=True,
            error_integration="some_integration",
            aws_sns_topic="some_topic",
            integration="some_integration",
            comment="This is a sample pipe"
        )
        ```

    Yaml:

        ```yaml
        pipes:
          - name: some_pipe
            as_: "COPY INTO some_table FROM @%some_stage"
            owner: SYSADMIN
            auto_ingest: true
            error_integration: some_integration
            aws_sns_topic: some_topic
            integration: some_integration
            comment: "This is a sample pipe"
        ```

    """

    resource_type = ResourceType.PIPE
    props = Props(
        auto_ingest=BoolProp("auto_ingest"),
        error_integration=StringProp("error_integration"),
        aws_sns_topic=StringProp("aws_sns_topic"),
        integration=StringProp("integration"),
        comment=StringProp("comment"),
        as_=QueryProp("as"),
    )
    scope = SchemaScope()
    spec = _Pipe

    def __init__(
        self,
        name: str,
        as_: str,
        owner: str = "SYSADMIN",
        auto_ingest: bool = None,
        error_integration: str = None,
        aws_sns_topic: str = None,
        integration: str = None,
        comment: str = None,
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        as_ = as_.strip().rstrip(";")
        self._data: _Pipe = _Pipe(
            name=self._name,
            as_=as_,
            owner=owner,
            auto_ingest=auto_ingest,
            error_integration=error_integration,
            aws_sns_topic=aws_sns_topic,
            integration=integration,
            comment=comment,
        )
        # copy_into = _parse_copy_into(as_)
        # self.requires(
        #     Table(name=copy_into["destination"], stub=True),
        #     Stage(name=copy_into["stage"], stub=True),
        # )
