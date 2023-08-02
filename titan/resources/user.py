from typing import Dict, List

from . import Resource
from .base import AccountScoped
from ..props import Props, BoolProp, IntProp, StringProp, StringListProp, TagsProp


class User(Resource, AccountScoped):
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

    resource_type = "USER"
    props = Props(
        password=StringProp("password"),
        login_name=StringProp("login_name"),
        display_name=StringProp("display_name"),
        first_name=StringProp("first_name"),
        middle_name=StringProp("middle_name"),
        last_name=StringProp("last_name"),
        email=StringProp("email"),
        must_change_password=BoolProp("must_change_password"),
        disabled=BoolProp("disabled"),
        days_to_expiry=IntProp("days_to_expiry"),
        mins_to_unlock=IntProp("mins_to_unlock"),
        default_warehouse=StringProp("default_warehouse"),
        default_namespace=StringProp("default_namespace"),
        default_role=StringProp("default_role"),
        default_secondary_roles=StringListProp("default_secondary_roles"),
        mins_to_bypass_mfa=IntProp("mins_to_bypass_mfa"),
        rsa_public_key=StringProp("rsa_public_key"),
        rsa_public_key_2=StringProp("rsa_public_key_2"),
        comment=StringProp("comment"),
        network_policy=StringProp("network_policy"),
        tags=TagsProp(),
    )

    name: str
    owner: str = "USERADMIN"
    password: str = None
    login_name: str = None
    display_name: str = None
    first_name: str = None
    middle_name: str = None
    last_name: str = None
    email: str = None
    must_change_password: bool = None
    disabled: bool = None
    days_to_expiry: int = None
    mins_to_unlock: int = None
    default_warehouse: str = None
    default_namespace: str = None
    default_role: str = None
    default_secondary_roles: List[str] = None
    mins_to_bypass_mfa: int = None
    rsa_public_key: str = None
    rsa_public_key_2: str = None
    comment: str = None
    network_policy: str = None
    tags: Dict[str, str] = None
