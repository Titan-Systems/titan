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
        start=IntProp("start", consume=["with", "="], eq=False),
        increment=IntProp("increment", consume=["by", "="], eq=False),
        comment=StringProp("comment"),
    )

    name: str
    owner: str = "SYSADMIN"
    start: int = None
    increment: int = None
    comment: str = None
