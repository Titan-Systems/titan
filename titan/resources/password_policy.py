from dataclasses import dataclass

from .resource import Resource, ResourceSpec
from .role import Role
from ..enums import ResourceType
from ..scope import SchemaScope

from ..props import (
    Props,
    StringProp,
    IntProp,
)


@dataclass(unsafe_hash=True)
class _PasswordPolicy(ResourceSpec):
    name: str
    password_min_length: int = 8
    password_max_length: int = 256
    password_min_upper_case_chars: int = 1
    password_min_lower_case_chars: int = 1
    password_min_numeric_chars: int = 1
    password_min_special_chars: int = 0
    password_min_age_days: int = 0
    password_max_age_days: int = 90
    password_max_retries: int = 5
    password_lockout_time_mins: int = 15
    password_history: int = 0
    comment: str = None
    owner: Role = "SYSADMIN"


class PasswordPolicy(Resource):
    """
    A Password Policy defines a set of rules for password complexity.

    CREATE [ OR REPLACE ] PASSWORD POLICY [ IF NOT EXISTS ] <name>
      [ PASSWORD_MIN_LENGTH = <integer> ]
      [ PASSWORD_MAX_LENGTH = <integer> ]
      [ PASSWORD_MIN_UPPER_CASE_CHARS = <integer> ]
      [ PASSWORD_MIN_LOWER_CASE_CHARS = <integer> ]
      [ PASSWORD_MIN_NUMERIC_CHARS = <integer> ]
      [ PASSWORD_MIN_SPECIAL_CHARS = <integer> ]
      [ PASSWORD_MIN_AGE_DAYS = <integer> ]
      [ PASSWORD_MAX_AGE_DAYS = <integer> ]
      [ PASSWORD_MAX_RETRIES = <integer> ]
      [ PASSWORD_LOCKOUT_TIME_MINS = <integer> ]
      [ PASSWORD_HISTORY = <integer> ]
      [ COMMENT = '<string_literal>' ]
    """

    resource_type = ResourceType.PASSWORD_POLICY
    props = Props(
        password_min_length=IntProp("password_min_length"),
        password_max_length=IntProp("password_max_length"),
        password_min_upper_case_chars=IntProp("password_min_upper_case_chars"),
        password_min_lower_case_chars=IntProp("password_min_lower_case_chars"),
        password_min_numeric_chars=IntProp("password_min_numeric_chars"),
        password_min_special_chars=IntProp("password_min_special_chars"),
        password_min_age_days=IntProp("password_min_age_days"),
        password_max_age_days=IntProp("password_max_age_days"),
        password_max_retries=IntProp("password_max_retries"),
        password_lockout_time_mins=IntProp("password_lockout_time_mins"),
        password_history=IntProp("password_history"),
        comment=StringProp("comment"),
    )
    scope = SchemaScope()
    spec = _PasswordPolicy

    def __init__(
        self,
        name: str,
        password_min_length: int = 8,
        password_max_length: int = 256,
        password_min_upper_case_chars: int = 1,
        password_min_lower_case_chars: int = 1,
        password_min_numeric_chars: int = 1,
        password_min_special_chars: int = 0,
        password_min_age_days: int = 0,
        password_max_age_days: int = 90,
        password_max_retries: int = 5,
        password_lockout_time_mins: int = 15,
        password_history: int = 0,
        comment: str = None,
        owner: str = "SYSADMIN",
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._data: _PasswordPolicy = _PasswordPolicy(
            name=name,
            password_min_length=password_min_length,
            password_max_length=password_max_length,
            password_min_upper_case_chars=password_min_upper_case_chars,
            password_min_lower_case_chars=password_min_lower_case_chars,
            password_min_numeric_chars=password_min_numeric_chars,
            password_min_special_chars=password_min_special_chars,
            password_min_age_days=password_min_age_days,
            password_max_age_days=password_max_age_days,
            password_max_retries=password_max_retries,
            password_lockout_time_mins=password_lockout_time_mins,
            password_history=password_history,
            comment=comment,
            owner=owner,
        )
