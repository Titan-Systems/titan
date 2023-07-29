from ..resource import Resource, AccountScoped
from ..props import (
    BoolProp,
    EnumProp,
    ExpressionProp,
    FlagProp,
    IdentifierProp,
    IntProp,
    Props,
    QueryProp,
    StringListProp,
    StringProp,
)

from .warehouse import Warehouse, WarehouseSize


class Task(Resource, AccountScoped):
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

    resource_type = "TASK"
    props = Props(
        warehouse=IdentifierProp("warehouse", resource_class=Warehouse),
        user_task_managed_initial_warehouse_size=EnumProp("user_task_managed_initial_warehouse_size", WarehouseSize),
        schedule=StringProp("schedule"),
        config=StringProp("config"),
        allow_overlapping_execution=BoolProp("allow_overlapping_execution"),
        # session_parameters=PropSet("session parameters"),
        user_task_timeout_ms=IntProp("user_task_timeout_ms"),
        suspend_task_after_num_failures=IntProp("suspend_task_after_num_failures"),
        error_integration=StringProp("error_integration"),
        copy_grants=FlagProp("copy grants"),
        comment=StringProp("comment"),
        after=StringListProp("after"),
        when=ExpressionProp("when"),
        as_=QueryProp("as"),
    )

    name: str
    owner: str = None
    warehouse: Warehouse = None
    user_task_managed_initial_warehouse_size: WarehouseSize = None
    schedule: str = None
    config: str = None
    allow_overlapping_execution: bool = None
    # session_parameters: Optional[Dict[str] = None
    user_task_timeout_ms: int = None
    suspend_task_after_num_failures: int = None
    error_integration: str = None
    copy_grants: bool = None
    comment: str = None
    after: str = None
    when: str = None
    as_: str = None
