from dataclasses import dataclass

from .resource import Resource, ResourceSpec
from .role import Role
from ..enums import ResourceType
from ..scope import SchemaScope

from ..props import Props, IntProp, StringProp


@dataclass(unsafe_hash=True)
class _Sequence(ResourceSpec):
    name: str
    owner: Role = "SYSADMIN"
    start: int = None
    increment: int = None
    comment: str = None


class Sequence(Resource):
    """
    CREATE [ OR REPLACE ] SEQUENCE [ IF NOT EXISTS ] <name>
      [ WITH ]
      [ START [ WITH ] [ = ] <initial_value> ]
      [ INCREMENT [ BY ] [ = ] <sequence_interval> ]
      [ COMMENT = '<string_literal>' ]
    """

    resource_type = ResourceType.SEQUENCE
    props = Props(
        _start_token="with",
        start=IntProp("start", consume=["with", "="], eq=False),
        increment=IntProp("increment", consume=["by", "="], eq=False),
        comment=StringProp("comment"),
    )
    scope = SchemaScope()
    spec = _Sequence

    def __init__(
        self,
        name: str,
        owner: str = "SYSADMIN",
        start: int = None,
        increment: int = None,
        comment: str = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._data = _Sequence(
            name=name,
            owner=owner,
            start=start,
            increment=increment,
            comment=comment,
        )
