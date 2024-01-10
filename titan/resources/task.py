from dataclasses import dataclass

# from .base import Resource, AccountScoped, _fix_class_documentation
from .__resource import Resource, AccountScope, ResourceSpec
from ..enums import ResourceType
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


@dataclass
class _Task(ResourceSpec):
    name: str
    owner: str = "SYSADMIN"
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
    as_: str = None


class Task(Resource):
    props = Props(
        warehouse=IdentifierProp("warehouse"),
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
        after=StringListProp("after", eq=False),
        when=ExpressionProp("when"),
        as_=QueryProp("as"),
    )
    resource_type = ResourceType.TASK
    scope = AccountScope()
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
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._data = _Task(
            name=name,
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
        )
