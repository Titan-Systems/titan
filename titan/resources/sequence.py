from . import Resource
from .base import SchemaScoped
from ..props import Props, IntProp, StringProp


class Sequence(Resource, SchemaScoped):
    """
    CREATE [ OR REPLACE ] SEQUENCE [ IF NOT EXISTS ] <name>
      [ WITH ]
      [ START [ WITH ] [ = ] <initial_value> ]
      [ INCREMENT [ BY ] [ = ] <sequence_interval> ]
      [ COMMENT = '<string_literal>' ]
    """

    resource_type = "SEQUENCE"
    props = Props(
        _start_token="with",
        start=IntProp("start"),
        increment=IntProp("increment"),
        comment=StringProp("comment"),
    )

    name: str
    owner: str = None
    start: int = None
    increment: int = None
    comment: str = None
