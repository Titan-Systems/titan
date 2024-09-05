from dataclasses import dataclass

from ..enums import ResourceType
from ..props import (
    IntProp,
    Props,
    StringProp,
)
from ..resource_name import ResourceName
from ..role_ref import RoleRef
from ..scope import SchemaScope
from .resource import NamedResource, Resource, ResourceSpec


@dataclass(unsafe_hash=True)
class _PasswordPolicy(ResourceSpec):
    name: ResourceName
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
    owner: RoleRef = "SYSADMIN"


class PasswordPolicy(NamedResource, Resource):
    """
    Description:
        Defines the rules and constraints for creating and managing passwords within the system, ensuring they meet specific security standards.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-password-policy

    Fields:
        name (string, required): The name of the password policy.
        password_min_length (int): The minimum length of the password. Defaults to 8.
        password_max_length (int): The maximum length of the password. Defaults to 256.
        password_min_upper_case_chars (int): The minimum number of uppercase characters in the password. Defaults to 1.
        password_min_lower_case_chars (int): The minimum number of lowercase characters in the password. Defaults to 1.
        password_min_numeric_chars (int): The minimum number of numeric characters in the password. Defaults to 1.
        password_min_special_chars (int): The minimum number of special characters in the password. Defaults to 0.
        password_min_age_days (int): The minimum age of the password in days. Defaults to 0.
        password_max_age_days (int): The maximum age of the password in days. Defaults to 90.
        password_max_retries (int): The maximum number of login retries before the account is locked. Defaults to 5.
        password_lockout_time_mins (int): The time in minutes an account remains locked after exceeding retry limit. Defaults to 15.
        password_history (int): The number of unique new passwords before an old password can be reused.
        comment (string): A comment about the password policy.
        owner (string or Role): The owner role of the password policy. Defaults to "SYSADMIN".

    Python:

        ```python
        password_policy = PasswordPolicy(
            name="some_password_policy",
            password_min_length=10,
            password_max_length=128,
            password_min_upper_case_chars=2,
            password_min_lower_case_chars=2,
            password_min_numeric_chars=2,
            password_min_special_chars=1,
            password_min_age_days=1,
            password_max_age_days=60,
            password_max_retries=3,
            password_lockout_time_mins=30,
            password_history=5,
            comment="Strict policy for admin accounts.",
            owner="SYSADMIN"
        )
        ```

    Yaml:

        ```yaml
        password_policies:
          - name: some_password_policy
            password_min_length: 10
            password_max_length: 128
            password_min_upper_case_chars: 2
            password_min_lower_case_chars: 2
            password_min_numeric_chars: 2
            password_min_special_chars: 1
            password_min_age_days: 1
            password_max_age_days: 60
            password_max_retries: 3
            password_lockout_time_mins: 30
            password_history: 5
            comment: Strict policy for admin accounts
            owner: SYSADMIN
        ```
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
        super().__init__(name, **kwargs)
        self._data: _PasswordPolicy = _PasswordPolicy(
            name=self._name,
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
