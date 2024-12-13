from dataclasses import dataclass

from titan.enums import AccountEdition, ResourceType
from titan.props import Props, StringProp, BoolProp, ArgsProp, ReturnsProp, QueryProp
from titan.scope import SchemaScope
from titan.resource_name import ResourceName
from titan.resources.resource import Arg, NamedResource, Resource, ResourceSpec
from titan.role_ref import RoleRef
from titan.data_types import convert_to_canonical_data_type


@dataclass(unsafe_hash=True)
class _MaskingPolicy(ResourceSpec):
    name: ResourceName
    args: list[Arg]
    returns: str
    body: str
    comment: str = None
    exempt_other_policies: bool = False
    owner: RoleRef = "SYSADMIN"

    def __post_init__(self):
        super().__post_init__()
        if len(self.args) == 0:
            raise ValueError("At least one argument is required")
        self.returns = convert_to_canonical_data_type(self.returns)


class MaskingPolicy(NamedResource, Resource):
    edition = {AccountEdition.ENTERPRISE, AccountEdition.BUSINESS_CRITICAL}
    resource_type = ResourceType.MASKING_POLICY
    props = Props(
        args=ArgsProp(),
        returns=ReturnsProp("returns", eq=False),
        body=QueryProp("->"),
        comment=StringProp("comment"),
        exempt_other_policies=BoolProp("exempt_other_policies"),
    )
    scope = SchemaScope()
    spec = _MaskingPolicy

    def __init__(
        self,
        name: str,
        args: list[dict],
        returns: str,
        body: str,
        comment: str = None,
        exempt_other_policies: bool = False,
        owner: str = "SYSADMIN",
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._data: _MaskingPolicy = _MaskingPolicy(
            name=self._name,
            args=args,
            returns=returns,
            body=body,
            comment=comment,
            exempt_other_policies=exempt_other_policies,
            owner=owner,
        )
