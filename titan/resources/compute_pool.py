from dataclasses import dataclass, field

from .resource import Resource, ResourceSpec
from ..enums import AccountEdition, ParseableEnum, ResourceType
from ..props import (
    IntProp,
    StringProp,
    BoolProp,
    Props,
    EnumProp,
)
from ..resource_name import ResourceName
from ..scope import AccountScope


class InstanceFamily(ParseableEnum):
    CPU_X64_XS = "CPU_X64_XS"
    CPU_X64_S = "CPU_X64_S"
    CPU_X64_M = "CPU_X64_M"
    CPU_X64_L = "CPU_X64_L"
    CPU_X64_XL = "CPU_X64_XL"
    CPU_X64_2XL = "CPU_X64_2XL"
    CPU_X64_3XL = "CPU_X64_3XL"
    CPU_X64_4XL = "CPU_X64_4XL"


@dataclass(unsafe_hash=True)
class _ComputePool(ResourceSpec):
    name: ResourceName
    min_nodes: int = None
    max_nodes: int = None
    instance_family: InstanceFamily = None
    auto_resume: bool = True
    initially_suspended: bool = field(default_factory=None, metadata={"fetchable": False})
    auto_suspend_secs: int = 3600
    comment: str = None


class ComputePool(Resource):
    """
    A compute pool is a group of compute resources in Snowflake that can be used to execute SQL queries.
    CREATE COMPUTE POOL [ IF NOT EXISTS ] <name>
      MIN_NODES = <num>
      MAX_NODES = <num>
      INSTANCE_FAMILY = <instance_family_name>
      [ AUTO_RESUME = { TRUE | FALSE } ]
      [ INITIALLY_SUSPENDED = { TRUE | FALSE } ]
      [ AUTO_SUSPEND_SECS = <num>  ]
      [ COMMENT = '<string_literal>' ]
    """

    edition = {AccountEdition.ENTERPRISE, AccountEdition.BUSINESS_CRITICAL}
    resource_type = ResourceType.COMPUTE_POOL
    props = Props(
        min_nodes=IntProp("min_nodes"),
        max_nodes=IntProp("max_nodes"),
        instance_family=EnumProp("instance_family", InstanceFamily),
        auto_resume=BoolProp("auto_resume"),
        initially_suspended=BoolProp("initially_suspended"),
        auto_suspend_secs=IntProp("auto_suspend_secs"),
        comment=StringProp("comment"),
    )
    scope = AccountScope()
    spec = _ComputePool

    def __init__(
        self,
        name: str,
        min_nodes: int = None,
        max_nodes: int = None,
        instance_family: InstanceFamily = None,
        auto_resume: bool = True,
        initially_suspended: bool = None,
        auto_suspend_secs: int = 3600,
        comment: str = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self._data = _ComputePool(
            name=name,
            min_nodes=min_nodes,
            max_nodes=max_nodes,
            instance_family=instance_family,
            auto_resume=auto_resume,
            initially_suspended=initially_suspended,
            auto_suspend_secs=auto_suspend_secs,
            comment=comment,
        )
