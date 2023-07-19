from .props import (
    BoolProp,
    IdentifierProp,
    Props,
    StringProp,
    QueryProp,
)

from .resource import Resource, Namespace


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

    resource_type = "PIPE"
    namespace = Namespace.SCHEMA
    props = Props(
        auto_ingest=BoolProp("auto_ingest"),
        # error_integration=IdentifierProp("error_integration"),
        aws_sns_topic=StringProp("aws_sns_topic"),
        integration=StringProp("integration"),
        comment=StringProp("comment"),
        as_=QueryProp("as"),
    )

    name: str
    owner: str = None
    auto_ingest: bool = None
    error_integration: str = None
    aws_sns_topic: str = None
    integration: str = None
    comment: str = None
    as_: str = None
