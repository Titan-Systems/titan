import re

from typing import Optional

from .resource import SchemaLevelResource
from .props import Identifier, StringProp, BoolProp, IdentifierProp, QueryProp


class Pipe(SchemaLevelResource):
    """
    CREATE [ OR REPLACE ] PIPE [ IF NOT EXISTS ] <name>
      [ AUTO_INGEST = [ TRUE | FALSE ] ]
      [ ERROR_INTEGRATION = <integration_name> ]
      [ AWS_SNS_TOPIC = '<string>' ]
      [ INTEGRATION = '<string>' ]
      [ COMMENT = '<string_literal>' ]
      AS <copy_statement>
    """

    props = {
        "AUTO_INGEST": BoolProp("AUTO_INGEST"),
        "ERROR_INTEGRATION": IdentifierProp("ERROR_INTEGRATION"),
        "AWS_SNS_TOPIC": StringProp("AWS_SNS_TOPIC"),
        "INTEGRATION": StringProp("INTEGRATION"),
        "COMMENT": StringProp("COMMENT"),
        "AS_": QueryProp("AS"),
    }

    create_statement = re.compile(
        rf"""
            CREATE\s+
            (?:OR\s+REPLACE\s+)?
            PIPE\s+
            (?:IF\s+NOT\s+EXISTS\s+)?
            ({Identifier.pattern})
        """,
        re.IGNORECASE | re.VERBOSE,
    )

    ownable = True

    def __init__(
        self,
        auto_ingest: Optional[bool] = None,
        error_integration: Optional[str] = None,
        aws_sns_topic: Optional[str] = None,
        integration: Optional[str] = None,
        comment: Optional[str] = None,
        as_: Optional[str] = None,
        **kwargs,
    ):
        self.auto_ingest = auto_ingest
        self.error_integration = error_integration
        self.aws_sns_topic = aws_sns_topic
        self.integration = integration
        self.comment = comment
        self.as_ = as_
        super().__init__(**kwargs)
