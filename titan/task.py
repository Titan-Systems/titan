import re
from typing import Optional, List

from titan.schema import Schema

from .parseable_enum import ParseableEnum
from .props import (
    BoolProp,
    EnumProp,
    FlagProp,
    Identifier,
    IdentifierListProp,
    IdentifierProp,
    IntProp,
    PropSet,
    StringProp,
    QueryProp,
)
from .resource import SchemaLevelResource
from .warehouse import WarehouseSize


class Task(SchemaLevelResource):
    """
    CREATE [ OR REPLACE ] TASK [ IF NOT EXISTS ] <name>
      [ { WAREHOUSE = <string> } | { USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = <string> } ]
      [ SCHEDULE = '{ <num> MINUTE | USING CRON <expr> <time_zone> }' ]
      [ CONFIG = <configuration_string> ]
      [ ALLOW_OVERLAPPING_EXECUTION = TRUE | FALSE ]
      [ <session_parameter> = <value> [ , <session_parameter> = <value> ... ] ]
      [ USER_TASK_TIMEOUT_MS = <num> ]
      [ SUSPEND_TASK_AFTER_NUM_FAILURES = <num> ]
      [ ERROR_INTEGRATION = <integration_name> ]
      [ COPY GRANTS ]
      [ COMMENT = '<string_literal>' ]
      [ AFTER <string> [ , <string> , ... ] ]
    [ WHEN <boolean_expr> ]
    AS
      <sql>
    """

    resource_name = "TASK"
    ownable = True

    create_statement = re.compile(
        rf"""
            CREATE\s+
            (?:OR\s+REPLACE\s+)?
            TASK\s+
            (?:IF\s+NOT\s+EXISTS\s+)?
            ({Identifier.pattern})
        """,
        re.VERBOSE | re.IGNORECASE,
    )

    props = {
        "warehouse": IdentifierProp("WAREHOUSE"),
        "user_task_managed_initial_warehouse_size": EnumProp("USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE", WarehouseSize),
        "schedule": StringProp("SCHEDULE"),
        "config": StringProp("CONFIG"),
        "allow_overlapping_execution": BoolProp("ALLOW_OVERLAPPING_EXECUTION"),
        # "session_parameters": PropSet("SESSION PARAMETERS"),
        "user_task_timeout_ms": IntProp("USER_TASK_TIMEOUT_MS"),
        "suspend_task_after_num_failures": IntProp("SUSPEND_TASK_AFTER_NUM_FAILURES"),
        "error_integration": IdentifierProp("ERROR_INTEGRATION"),
        "copy_grants": FlagProp("COPY GRANTS"),
        "comment": StringProp("COMMENT"),
        "after": IdentifierListProp("AFTER", naked=True),
        # "when": ExpressionProp("WHEN"),
        "when": StringProp("WHEN"),
        "as_": QueryProp("AS"),
    }

    def __init__(
        self,
        name: str,
        warehouse: Optional[str] = None,
        user_task_managed_initial_warehouse_size: Optional[str] = None,
        schedule: Optional[str] = None,
        config: Optional[str] = None,
        allow_overlapping_execution: Optional[bool] = None,
        # session_parameters: Optional[Dict[str, str]] = None,
        user_task_timeout_ms: Optional[int] = None,
        suspend_task_after_num_failures: Optional[int] = None,
        error_integration: Optional[str] = None,
        copy_grants: Optional[bool] = None,
        comment: Optional[str] = None,
        after: Optional[List[str]] = None,
        when: Optional[str] = None,
        as_: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self.warehouse = warehouse
        self.user_task_managed_initial_warehouse_size = user_task_managed_initial_warehouse_size
        self.schedule = schedule
        self.config = config
        self.allow_overlapping_execution = allow_overlapping_execution
        # self.session_parameters = session_parameters
        self.user_task_timeout_ms = user_task_timeout_ms
        self.suspend_task_after_num_failures = suspend_task_after_num_failures
        self.error_integration = error_integration
        self.copy_grants = copy_grants
        self.comment = comment
        self.after = after
        self.when = when
        self.as_ = as_
