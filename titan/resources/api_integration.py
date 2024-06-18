from dataclasses import dataclass

from .resource import Resource, ResourceSpec, ResourceNameTrait
from .role import Role
from ..enums import ParseableEnum, ResourceType
from ..resource_name import ResourceName
from ..scope import AccountScope


from ..props import Props, EnumProp, StringProp, BoolProp, StringListProp


class ApiProvider(ParseableEnum):
    AWS_API_GATEWAY = "AWS_API_GATEWAY"
    AWS_PRIVATE_API_GATEWAY = "AWS_PRIVATE_API_GATEWAY"
    AWS_GOV_API_GATEWAY = "AWS_GOV_API_GATEWAY"
    AWS_GOV_PRIVATE_API_GATEWAY = "AWS_GOV_PRIVATE_API_GATEWAY"


@dataclass(unsafe_hash=True)
class _APIIntegration(ResourceSpec):
    name: ResourceName
    api_provider: ApiProvider
    api_aws_role_arn: str
    enabled: bool
    api_allowed_prefixes: list[str]
    api_blocked_prefixes: list[str] = None
    api_key: str = None
    owner: Role = "ACCOUNTADMIN"
    comment: str = None


class APIIntegration(ResourceNameTrait, Resource):
    """
    CREATE [ OR REPLACE ] API INTEGRATION [ IF NOT EXISTS ] <integration_name>
        API_PROVIDER = { aws_api_gateway | aws_private_api_gateway | aws_gov_api_gateway | aws_gov_private_api_gateway }
        API_AWS_ROLE_ARN = '<iam_role>'
        [ API_KEY = '<api_key>' ]
        API_ALLOWED_PREFIXES = ('<...>')
        [ API_BLOCKED_PREFIXES = ('<...>') ]
        ENABLED = { TRUE | FALSE }
        [ COMMENT = '<string_literal>' ]
        ;
    """

    resource_type = ResourceType.API_INTEGRATION
    props = Props(
        api_provider=EnumProp("api_provider", ApiProvider),
        api_aws_role_arn=StringProp("api_aws_role_arn"),
        api_key=StringProp("api_key"),
        api_allowed_prefixes=StringListProp("api_allowed_prefixes", parens=True),
        api_blocked_prefixes=StringListProp("api_blocked_prefixes", parens=True),
        enabled=BoolProp("enabled"),
        comment=StringProp("comment"),
    )
    scope = AccountScope()
    spec = _APIIntegration

    def __init__(
        self,
        name: str,
        api_provider: ApiProvider,
        api_aws_role_arn: str,
        enabled: bool,
        api_allowed_prefixes: list[str],
        api_blocked_prefixes: list[str] = None,
        api_key: str = None,
        owner: str = "ACCOUNTADMIN",
        comment: str = None,
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._data: _APIIntegration = _APIIntegration(
            name=self._name,
            api_provider=api_provider,
            api_aws_role_arn=api_aws_role_arn,
            api_key=api_key,
            api_allowed_prefixes=api_allowed_prefixes,
            api_blocked_prefixes=api_blocked_prefixes,
            enabled=enabled,
            owner=owner,
            comment=comment,
        )
