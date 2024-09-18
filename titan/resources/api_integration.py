from dataclasses import dataclass, field

from ..enums import ParseableEnum, ResourceType
from ..props import BoolProp, EnumProp, Props, StringListProp, StringProp
from ..resource_name import ResourceName
from ..scope import AccountScope
from .resource import NamedResource, Resource, ResourceSpec
from .role import Role


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
    api_key: str = field(default=None, metadata={"fetchable": False})
    owner: Role = "ACCOUNTADMIN"
    comment: str = None


class APIIntegration(NamedResource, Resource):
    """
    Description:
        Manages API integrations in Snowflake, allowing external services to interact with Snowflake resources securely.
        This class supports creating, replacing, and checking the existence of API integrations with various configurations.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-api-integration

    Fields:
        name (string, required): The unique name of the API integration.
        api_provider (string or ApiProvider, required): The provider of the API service. Defaults to AWS_API_GATEway.
        api_aws_role_arn (string, required): The AWS IAM role ARN associated with the API integration.
        api_key (string): The API key used for authentication.
        api_allowed_prefixes (list): The list of allowed prefixes for the API endpoints.
        api_blocked_prefixes (list): The list of blocked prefixes for the API endpoints.
        enabled (bool, required): Specifies if the API integration is enabled. Defaults to TRUE.
        comment (string): A comment or description for the API integration.

    Python:

        ```python
        api_integration = APIIntegration(
            name="some_api_integration",
            api_provider="AWS_API_GATEWAY",
            api_aws_role_arn="arn:aws:iam::123456789012:role/MyRole",
            enabled=True,
            api_allowed_prefixes=["/prod/", "/dev/"],
            api_blocked_prefixes=["/test/"],
            api_key="ABCD1234",
            comment="Example API integration"
        )
        ```

    Yaml:

        ```yaml
        api_integrations:
          - name: some_api_integration
            api_provider: AWS_API_GATEWAY
            api_aws_role_arn: "arn:aws:iam::123456789012:role/MyRole"
            enabled: true
            api_allowed_prefixes: ["/prod/", "/dev/"]
            api_blocked_prefixes: ["/test/"]
            api_key: "ABCD1234"
            comment: "Example API integration"
        ```
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
