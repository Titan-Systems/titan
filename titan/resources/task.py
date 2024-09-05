from dataclasses import dataclass

from ..enums import ResourceType, TaskState, WarehouseSize
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
from ..resource_name import ResourceName
from ..role_ref import RoleRef
from ..scope import SchemaScope
from .resource import NamedResource, Resource, ResourceSpec
from .warehouse import Warehouse


@dataclass(unsafe_hash=True)
class _Task(ResourceSpec):
    name: ResourceName
    owner: RoleRef = "SYSADMIN"
    warehouse: Warehouse = None
    user_task_managed_initial_warehouse_size: WarehouseSize = None
    schedule: str = None
    config: str = None
    allow_overlapping_execution: bool = None
    user_task_timeout_ms: int = None
    suspend_task_after_num_failures: int = None
    error_integration: str = None
    copy_grants: bool = None
    comment: str = None
    after: list[str] = None
    when: str = None
    state: TaskState = TaskState.SUSPENDED
    as_: str = None


class Task(NamedResource, Resource):
    """
    Description:
        Represents a scheduled task in Snowflake that performs a specified SQL statement at a recurring interval.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-task

    Fields:
        warehouse (string or Warehouse): The warehouse used by the task.
        user_task_managed_initial_warehouse_size (string or WarehouseSize): The initial warehouse size when the task is managed by the user. Defaults to None.
        schedule (string): The schedule on which the task runs.
        config (string): Configuration settings for the task.
        allow_overlapping_execution (bool): Whether the task can have overlapping executions.
        user_task_timeout_ms (int): The timeout in milliseconds after which the task is aborted.
        suspend_task_after_num_failures (int): The number of consecutive failures after which the task is suspended.
        error_integration (string): The integration used for error handling.
        copy_grants (bool): Whether to copy grants from the referenced objects.
        comment (string): A comment for the task.
        after (list): A list of tasks that must be completed before this task runs.
        when (string): A conditional expression that determines when the task runs.
        as_ (string): The SQL statement that the task executes.
        state (string or TaskState, required): The initial state of the task. Defaults to SUSPENDED.

    Python:

        ```python
        task = Task(
            name="some_task",
            warehouse="some_warehouse",
            schedule="USING CRON 0 9 * * * UTC",
            state="SUSPENDED",
            as_="SELECT 1"
        )
        ```

    Yaml:

        ```yaml
        tasks:
          - name: some_task
            warehouse: some_warehouse
            schedule: "USING CRON 0 9 * * * UTC"
            state: SUSPENDED
            as_: |
                SELECT 1
        ```
    """

    resource_type = ResourceType.TASK
    props = Props(
        warehouse=IdentifierProp("warehouse"),
        user_task_managed_initial_warehouse_size=EnumProp("user_task_managed_initial_warehouse_size", WarehouseSize),
        schedule=StringProp("schedule"),
        config=StringProp("config"),
        allow_overlapping_execution=BoolProp("allow_overlapping_execution"),
        user_task_timeout_ms=IntProp("user_task_timeout_ms"),
        suspend_task_after_num_failures=IntProp("suspend_task_after_num_failures"),
        error_integration=StringProp("error_integration"),
        copy_grants=FlagProp("copy grants"),
        comment=StringProp("comment"),
        after=StringListProp("after", eq=False),
        when=ExpressionProp("when"),
        as_=QueryProp("as"),
    )
    scope = SchemaScope()
    spec = _Task

    def __init__(
        self,
        name: str,
        owner: str = "SYSADMIN",
        warehouse: Warehouse = None,
        user_task_managed_initial_warehouse_size: WarehouseSize = None,
        schedule: str = None,
        config: str = None,
        allow_overlapping_execution: bool = None,
        user_task_timeout_ms: int = None,
        suspend_task_after_num_failures: int = None,
        error_integration: str = None,
        copy_grants: bool = None,
        comment: str = None,
        after: list[str] = None,
        when: str = None,
        as_: str = None,
        state: TaskState = TaskState.SUSPENDED,
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._data = _Task(
            name=self._name,
            owner=owner,
            warehouse=warehouse,
            user_task_managed_initial_warehouse_size=user_task_managed_initial_warehouse_size,
            schedule=schedule,
            config=config,
            allow_overlapping_execution=allow_overlapping_execution,
            user_task_timeout_ms=user_task_timeout_ms,
            suspend_task_after_num_failures=suspend_task_after_num_failures,
            error_integration=error_integration,
            copy_grants=copy_grants,
            comment=comment,
            after=after,
            when=when,
            as_=as_,
            state=state,
        )
