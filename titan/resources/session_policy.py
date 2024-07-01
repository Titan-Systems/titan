from dataclasses import dataclass

from .resource import Resource, ResourceSpec, NamedResource
from ..enums import ResourceType
from ..scope import SchemaScope
from ..resource_name import ResourceName

from ..props import (
    Props,
    IntProp,
    StringProp,
)


# TODO: needs ownership, fetch
@dataclass(unsafe_hash=True)
class _SessionPolicy(ResourceSpec):
    name: ResourceName
    session_idle_timeout_mins: int = None
    session_ui_idle_timeout_mins: int = None
    comment: str = None

    def __post_init__(self):
        super().__post_init__()
        if self.session_idle_timeout_mins is not None and self.session_idle_timeout_mins < 0:
            raise ValueError("SESSION_IDLE_TIMEOUT_MINS must be a positive integer.")
        if self.session_ui_idle_timeout_mins is not None and self.session_ui_idle_timeout_mins < 0:
            raise ValueError("SESSION_UI_IDLE_TIMEOUT_MINS must be a positive integer.")


class SessionPolicy(NamedResource, Resource):
    """
    Description:
        Manages session policies in Snowflake, which define timeout settings for user sessions to enhance security.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-session-policy

    Fields:
        name (string, required): The name of the session policy.
        session_idle_timeout_mins (int): The maximum amount of time a session can remain idle before it is automatically terminated.
        session_ui_idle_timeout_mins (int): The maximum amount of time a user interface session can remain idle before it is automatically terminated.
        comment (string): A description or comment about the session policy.

    Python:

        ```python
        session_policy = SessionPolicy(
            name="some_session_policy",
            session_idle_timeout_mins=30,
            session_ui_idle_timeout_mins=10,
            comment="Policy for standard users."
        )
        ```

    Yaml:

        ```yaml
        session_policies:
          - name: some_session_policy
            session_idle_timeout_mins: 30
            session_ui_idle_timeout_mins: 10
            comment: Policy for standard users.
        ```
    """

    resource_type = ResourceType.SESSION_POLICY
    props = Props(
        session_idle_timeout_mins=IntProp("session_idle_timeout_mins"),
        session_ui_idle_timeout_mins=IntProp("session_ui_idle_timeout_mins"),
        comment=StringProp("comment"),
    )
    scope = SchemaScope()
    spec = _SessionPolicy

    def __init__(
        self,
        name: str,
        session_idle_timeout_mins: int = None,
        session_ui_idle_timeout_mins: int = None,
        comment: str = None,
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._data: _SessionPolicy = _SessionPolicy(
            name=self._name,
            session_idle_timeout_mins=session_idle_timeout_mins,
            session_ui_idle_timeout_mins=session_ui_idle_timeout_mins,
            comment=comment,
        )
