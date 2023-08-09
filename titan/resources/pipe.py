from . import Resource
from .base import SchemaScoped
from ..props import BoolProp, Props, StringProp, QueryProp


class Pipe(Resource, SchemaScoped):
    """
    CREATE [ OR REPLACE ] PIPE [ IF NOT EXISTS ] <name>
      [ AUTO_INGEST = [ TRUE | FALSE ] ]
      [ ERROR_INTEGRATION = <integration_name> ]
      [ AWS_SNS_TOPIC = '<string>' ]
      [ INTEGRATION = '<string>' ]
      [ COMMENT = '<string_literal>' ]
      AS <copy_statement>
    """

    resource_type = "PIPE"
    props = Props(
        auto_ingest=BoolProp("auto_ingest"),
        error_integration=StringProp("error_integration"),
        aws_sns_topic=StringProp("aws_sns_topic"),
        integration=StringProp("integration"),
        comment=StringProp("comment"),
        as_=QueryProp("as"),
    )

    name: str
    owner: str = "SYSADMIN"
    auto_ingest: bool = None
    error_integration: str = None
    aws_sns_topic: str = None
    integration: str = None
    comment: str = None
    as_: str
