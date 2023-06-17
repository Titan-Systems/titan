from __future__ import annotations

import re

from typing import Optional

from .props import BoolProp, IntProp, StringProp, Identifier
from .resource import AccountLevelResource


class User(AccountLevelResource):
    """
    CREATE [ OR REPLACE ] USER [ IF NOT EXISTS ] <name>
        [ objectProperties ]
        [ objectParams ]
        [ sessionParams ]
        [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]

    objectProperties ::=
        PASSWORD = '<string>'
        LOGIN_NAME = <string>
        DISPLAY_NAME = <string>
        FIRST_NAME = <string>
        MIDDLE_NAME = <string>
        LAST_NAME = <string>
        EMAIL = <string>
        MUST_CHANGE_PASSWORD = TRUE | FALSE
        DISABLED = TRUE | FALSE
        DAYS_TO_EXPIRY = <integer>
        MINS_TO_UNLOCK = <integer>
        DEFAULT_WAREHOUSE = <string>
        DEFAULT_NAMESPACE = <string>
        DEFAULT_ROLE = <string>
        DEFAULT_SECONDARY_ROLES = ( 'ALL' )
        MINS_TO_BYPASS_MFA = <integer>
        RSA_PUBLIC_KEY = <string>
        RSA_PUBLIC_KEY_2 = <string>
        COMMENT = '<string_literal>'

    objectParams ::=
        NETWORK_POLICY = <string>

    sessionParams ::=
        ABORT_DETACHED_QUERY = TRUE | FALSE
        AUTOCOMMIT = TRUE | FALSE
        BINARY_INPUT_FORMAT = <string>
        BINARY_OUTPUT_FORMAT = <string>
        DATE_INPUT_FORMAT = <string>
        DATE_OUTPUT_FORMAT = <string>
        ERROR_ON_NONDETERMINISTIC_MERGE = TRUE | FALSE
        ERROR_ON_NONDETERMINISTIC_UPDATE = TRUE | FALSE
        JSON_INDENT = <num>
        LOCK_TIMEOUT = <num>
        QUERY_TAG = <string>
        ROWS_PER_RESULTSET = <num>
        SIMULATED_DATA_SHARING_CONSUMER = <string>
        STATEMENT_TIMEOUT_IN_SECONDS = <num>
        STRICT_JSON_OUTPUT = TRUE | FALSE
        TIMESTAMP_DAY_IS_ALWAYS_24H = TRUE | FALSE
        TIMESTAMP_INPUT_FORMAT = <string>
        TIMESTAMP_LTZ_OUTPUT_FORMAT = <string>
        TIMESTAMP_NTZ_OUTPUT_FORMAT = <string>
        TIMESTAMP_OUTPUT_FORMAT = <string>
        TIMESTAMP_TYPE_MAPPING = <string>
        TIMESTAMP_TZ_OUTPUT_FORMAT = <string>
        TIMEZONE = <string>
        TIME_INPUT_FORMAT = <string>
        TIME_OUTPUT_FORMAT = <string>
        TRANSACTION_DEFAULT_ISOLATION_LEVEL = <string>
        TWO_DIGIT_CENTURY_START = <num>
        UNSUPPORTED_DDL_ACTION = <string>
        USE_CACHED_RESULT = TRUE | FALSE
        WEEK_OF_YEAR_POLICY = <num>
        WEEK_START = <num>
    """

    props = {
        "PASSWORD": StringProp("PASSWORD"),
        "LOGIN_NAME": StringProp("LOGIN_NAME"),
        "DISPLAY_NAME": StringProp("DISPLAY_NAME"),
        "FIRST_NAME": StringProp("FIRST_NAME"),
        "MIDDLE_NAME": StringProp("MIDDLE_NAME"),
        "LAST_NAME": StringProp("LAST_NAME"),
        "EMAIL": StringProp("EMAIL"),
        "MUST_CHANGE_PASSWORD": BoolProp("MUST_CHANGE_PASSWORD"),
        "DISABLED": BoolProp("DISABLED"),
        "DAYS_TO_EXPIRY": IntProp("DAYS_TO_EXPIRY"),
        "MINS_TO_UNLOCK": IntProp("MINS_TO_UNLOCK"),
        "DEFAULT_WAREHOUSE": StringProp("DEFAULT_WAREHOUSE"),
        "DEFAULT_NAMESPACE": StringProp("DEFAULT_NAMESPACE"),
        "DEFAULT_ROLE": StringProp("DEFAULT_ROLE"),
        "DEFAULT_SECONDARY_ROLES": StringProp("DEFAULT_SECONDARY_ROLES"),
        "MINS_TO_BYPASS_MFA": IntProp("MINS_TO_BYPASS_MFA"),
        "RSA_PUBLIC_KEY": StringProp("RSA_PUBLIC_KEY"),
        "RSA_PUBLIC_KEY_2": StringProp("RSA_PUBLIC_KEY_2"),
        "COMMENT": StringProp("COMMENT"),
        "NETWORK_POLICY": StringProp("NETWORK_POLICY"),
    }

    create_statement = re.compile(
        rf"CREATE\s+(?:OR\s+REPLACE\s+)?USER\s+(?:IF\s+NOT\s+EXISTS\s+)?({Identifier.pattern})",
        re.IGNORECASE,
    )

    ownable = True

    def __init__(
        self,
        password: Optional[str] = None,
        login_name: Optional[str] = None,
        display_name: Optional[str] = None,
        first_name: Optional[str] = None,
        middle_name: Optional[str] = None,
        last_name: Optional[str] = None,
        email: Optional[str] = None,
        must_change_password: Optional[bool] = None,
        disabled: Optional[bool] = None,
        days_to_expiry: Optional[int] = None,
        mins_to_unlock: Optional[int] = None,
        default_warehouse: Optional[str] = None,
        default_namespace: Optional[str] = None,
        default_role: Optional[str] = None,
        default_secondary_roles: Optional[str] = None,
        mins_to_bypass_mfa: Optional[int] = None,
        rsa_public_key: Optional[str] = None,
        rsa_public_key_2: Optional[str] = None,
        comment: Optional[str] = None,
        network_policy: Optional[str] = None,
        **kwargs,
    ):
        self.password = password
        self.login_name = login_name
        self.display_name = display_name
        self.first_name = first_name
        self.middle_name = middle_name
        self.last_name = last_name
        self.email = email
        self.must_change_password = must_change_password
        self.disabled = disabled
        self.days_to_expiry = days_to_expiry
        self.mins_to_unlock = mins_to_unlock
        self.default_warehouse = default_warehouse
        self.default_namespace = default_namespace
        self.default_role = default_role
        self.default_secondary_roles = default_secondary_roles
        self.mins_to_bypass_mfa = mins_to_bypass_mfa
        self.rsa_public_key = rsa_public_key
        self.rsa_public_key_2 = rsa_public_key_2
        self.comment = comment
        self.network_policy = network_policy
        super().__init__(**kwargs)

    def __repr__(self):
        return f"<{type(self).__name__}:{self.name}>"
