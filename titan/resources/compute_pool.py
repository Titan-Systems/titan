from dataclasses import dataclass, field

from ..enums import AccountEdition, ParseableEnum, ResourceType
from ..props import (
    BoolProp,
    EnumProp,
    IntProp,
    Props,
    StringProp,
)
from ..resource_name import ResourceName
from ..scope import AccountScope
from .resource import NamedResource, Resource, ResourceSpec
from .role import Role


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
    owner: Role = "ACCOUNTADMIN"
    min_nodes: int = None
    max_nodes: int = None
    instance_family: InstanceFamily = None
    auto_resume: bool = True
    initially_suspended: bool = field(default=None, metadata={"fetchable": False})
    auto_suspend_secs: int = 3600
    comment: str = None


class ComputePool(NamedResource, Resource):
    """
    Description:
        A compute pool is a group of compute resources in Snowflake that can be used to execute SQL queries.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-compute-pool

    Fields:
        name (string, required): The unique name of the compute pool.
        owner (string or Role): The owner of the compute pool. Defaults to "ACCOUNTADMIN".
        min_nodes (int): The minimum number of nodes in the compute pool.
        max_nodes (int): The maximum number of nodes in the compute pool.
        instance_family (string or InstanceFamily): The family of instances to use for the compute nodes.
        auto_resume (bool): Whether the compute pool should automatically resume when queries are submitted. Defaults to True.
        initially_suspended (bool): Whether the compute pool should start in a suspended state.
        auto_suspend_secs (int): The number of seconds of inactivity after which the compute pool should automatically suspend. Defaults to 3600.
        comment (string): An optional comment about the compute pool.

    Python:
        ```python
        compute_pool = ComputePool(
            name="some_compute_pool",
            owner="ACCOUNTADMIN",
            min_nodes=2,
            max_nodes=10,
            instance_family="CPU_X64_S",
            auto_resume=True,
            initially_suspended=False,
            auto_suspend_secs=1800,
            comment="Example compute pool"
        )
        ```

    Yaml:
        ```yaml
        compute_pools:
          - name: some_compute_pool
            owner: ACCOUNTADMIN
            min_nodes: 2
            max_nodes: 10
            instance_family: CPU_X64_S
            auto_resume: true
            initially_suspended: false
            auto_suspend_secs: 1800
            comment: Example compute pool
        ```
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
        owner: str = "ACCOUNTADMIN",
        min_nodes: int = None,
        max_nodes: int = None,
        instance_family: InstanceFamily = None,
        auto_resume: bool = True,
        initially_suspended: bool = None,
        auto_suspend_secs: int = 3600,
        comment: str = None,
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._data: _ComputePool = _ComputePool(
            name=self._name,
            owner=owner,
            min_nodes=min_nodes,
            max_nodes=max_nodes,
            instance_family=instance_family,
            auto_resume=auto_resume,
            initially_suspended=initially_suspended,
            auto_suspend_secs=auto_suspend_secs,
            comment=comment,
        )
