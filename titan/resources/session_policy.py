from dataclasses import dataclass

from .resource import Resource, ResourceSpec
from .role import Role
from ..enums import ResourceType
from ..scope import AccountScope

from ..props import (
    Props,
    IntegerProp,
    StringProp,
)


@dataclass(unsafe_hash=True)
class _SessionPolicy(ResourceSpec):
    name: str
    session_idle_timeout_mins: int = None
    session_ui_idle_timeout_mins: int = None
    comment: str = None

    def __post_init__(self):
        super().__post_init__()
        if self.session_idle_timeout_mins is not None and self.session_idle_timeout_mins < 0:
            raise ValueError("SESSION_IDLE_TIMEOUT_MINS must be a positive integer.")
        if self.session_ui_idle_timeout_mins is not None and self.session_ui_idle_timeout_mins < 0:
            raise ValueError("SESSION_UI_IDLE_TIMEOUT_MINS must be a positive integer.")


class SessionPolicy(Resource):
    """
    A Session Policy defines session timeout settings within a Snowflake account.

    CREATE [ OR REPLACE ] SESSION POLICY [IF NOT EXISTS] <name>
      [ SESSION_IDLE_TIMEOUT_MINS = <integer> ]
      [ SESSION_UI_IDLE_TIMEOUT_MINS = <integer> ]
      [ COMMENT = '<string_literal>' ]
    """

    resource_type = ResourceType.SESSION_POLICY
    props = Props(
        session_idle_timeout_mins=IntegerProp("session_idle_timeout_mins"),
        session_ui_idle_timeout_mins=IntegerProp("session_ui_idle_timeout_mins"),
        comment=StringProp("comment"),
    )
    scope = AccountScope()
    spec = _SessionPolicy

    def __init__(
        self,
        name: str,
        session_idle_timeout_mins: int = None,
        session_ui_idle_timeout_mins: int = None,
        comment: str = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._data: _SessionPolicy = _SessionPolicy(
            name=name,
            session_idle_timeout_mins=session_idle_timeout_mins,
            session_ui_idle_timeout_mins=session_ui_idle_timeout_mins,
            comment=comment,
        )
