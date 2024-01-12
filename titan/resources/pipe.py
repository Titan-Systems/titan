from dataclasses import dataclass

from .resource import Resource, ResourceSpec
from ..enums import ResourceType
from ..scope import SchemaScope

from ..props import BoolProp, Props, StringProp, QueryProp


@dataclass
class _Pipe(ResourceSpec):
    name: str
    as_: str
    owner: str = "SYSADMIN"
    auto_ingest: bool = None
    error_integration: str = None
    aws_sns_topic: str = None
    integration: str = None
    comment: str = None


class Pipe(Resource):
    """
    CREATE [ OR REPLACE ] PIPE [ IF NOT EXISTS ] <name>
      [ AUTO_INGEST = [ TRUE | FALSE ] ]
      [ ERROR_INTEGRATION = <integration_name> ]
      [ AWS_SNS_TOPIC = '<string>' ]
      [ INTEGRATION = '<string>' ]
      [ COMMENT = '<string_literal>' ]
      AS <copy_statement>
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

    # TODO: parse as_ statement and extract stage reference
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
        super().__init__(**kwargs)
        self._data = _Pipe(
            name=name,
            as_=as_,
            owner=owner,
            auto_ingest=auto_ingest,
            error_integration=error_integration,
            aws_sns_topic=aws_sns_topic,
            integration=integration,
            comment=comment,
        )
