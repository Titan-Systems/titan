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
    AZURE_API_MANAGEMENT = "AZURE_API_MANAGEMENT"
    GOOGLE_API_GATEWAY = "GOOGLE_API_GATEWAY"
    GIT_HTTPS_API = "GIT_HTTPS_API"

@dataclass(unsafe_hash=True)
class _AWSAPIIntegration(ResourceSpec):
    name: ResourceName
    api_provider: ApiProvider
    api_aws_role_arn: str
    enabled: bool
    api_allowed_prefixes: list[str]
    api_blocked_prefixes: list[str] = None
    api_key: str = field(default=None, metadata={"fetchable": False})
    owner: Role = "ACCOUNTADMIN"
    comment: str = None


class AWSAPIIntegration(NamedResource, Resource):
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
    spec = _AWSAPIIntegration

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
        self._data: _AWSAPIIntegration = _AWSAPIIntegration(
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

@dataclass(unsafe_hash=True)
class _AzureAPIIntegration(ResourceSpec):
    name: ResourceName
    api_provider: ApiProvider
    azure_tenant_id: str
    azure_ad_application_id: str
    enabled: bool
    api_allowed_prefixes: list[str]
    api_blocked_prefixes: list[str] = None
    api_key: str = field(default=None, metadata={"fetchable": False})
    owner: Role = "ACCOUNTADMIN"
    comment: str = None


class AzureAPIIntegration(NamedResource, Resource):
    """
    Description:
        Manages API integrations in Snowflake, allowing external services to interact with Snowflake resources securely.
        This class supports creating, replacing, and checking the existence of API integrations with various configurations.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-api-integration

    Fields:
        name (string, required): The unique name of the API integration.
        api_provider (string or ApiProvider, required): The provider of the API service.
        google_audience (string, required): The audience claim when generating the JWT to authenticate with the Google API Gateway.
        api_allowed_prefixes (list, required): The list of allowed prefixes for the API endpoints.
        api_blocked_prefixes (list): The list of blocked prefixes for the API endpoints.
        enabled (bool, required): Specifies if the API integration is enabled. Defaults to TRUE.
        comment (string): A comment or description for the API integration.

    Python:

        ```python
        api_integration = APIIntegration(
            name="some_api_integration",
            api_provider="GOOGLE_API_GATEWAU",
            google_audience="<google_audience>",
            enabled=True,
            api_allowed_prefixes=["https://some_url.com"],
            comment="Example GCP API integration"
        )
        ```

    Yaml:

        ```yaml
        api_integrations:
          - name: some_api_integration
            api_provider: GOOGLE_API_GATEWAY
            google_audience: <google_audience>
            enabled: true
            api_allowed_prefixes:
                - https://some_url.com
            comment: "Example GCP API integration"
        ```
    """


    resource_type = ResourceType.API_INTEGRATION
    props = Props(
        api_provider=EnumProp("api_provider", ApiProvider),
        azure_tenant_id=StringProp("azure_tenant_id"),
        azure_ad_application_id=StringProp("azure_ad_application_id"),
        api_key=StringProp("api_key"),
        api_allowed_prefixes=StringListProp("api_allowed_prefixes", parens=True),
        api_blocked_prefixes=StringListProp("api_blocked_prefixes", parens=True),
        enabled=BoolProp("enabled"),
        comment=StringProp("comment"),
    )
    scope = AccountScope()
    spec = _AzureAPIIntegration

    def __init__(
        self,
        name: str,
        api_provider: ApiProvider,
        azure_tenant_id: str,
        azure_ad_application_id: str,
        api_key: str,
        enabled: bool,
        api_allowed_prefixes: list[str],
        api_blocked_prefixes: list[str] = None,
        owner: str = "ACCOUNTADMIN",
        comment: str = None,
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._data: _AzureAPIIntegration = _AzureAPIIntegration(
            name=self._name,
            api_provider=api_provider,
            azure_tenant_id=azure_tenant_id,
            azure_ad_application_id=azure_ad_application_id,
            api_key=api_key,
            api_allowed_prefixes=api_allowed_prefixes,
            api_blocked_prefixes=api_blocked_prefixes,
            enabled=enabled,
            owner=owner,
            comment=comment,
        )

@dataclass(unsafe_hash=True)
class _GCPAPIIntegration(ResourceSpec):
    name: ResourceName
    api_provider: ApiProvider
    google_audience: str
    enabled: bool
    api_allowed_prefixes: list[str]
    api_blocked_prefixes: list[str] = None
    owner: Role = "ACCOUNTADMIN"
    comment: str = None


class GCPAPIIntegration(NamedResource, Resource):
    """
    Description:
        Manages API integrations in Snowflake, allowing external services to interact with Snowflake resources securely.
        This class supports creating, replacing, and checking the existence of API integrations with various configurations.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-api-integration

    Fields:
        name (string, required): The unique name of the API integration.
        api_provider (string or ApiProvider, required): The provider of the API service.
        google_audience (string, required): The audience claim when generating the JWT to authenticate with the Google API Gateway.
        api_allowed_prefixes (list, required): The list of allowed prefixes for the API endpoints.
        api_blocked_prefixes (list): The list of blocked prefixes for the API endpoints.
        enabled (bool, required): Specifies if the API integration is enabled. Defaults to TRUE.
        comment (string): A comment or description for the API integration.

    Python:

        ```python
        api_integration = APIIntegration(
            name="some_api_integration",
            api_provider="GOOGLE_API_GATEWAU",
            google_audience="<google_audience>",
            enabled=True,
            api_allowed_prefixes=["https://some_url.com"],
            comment="Example GCP API integration"
        )
        ```

    Yaml:

        ```yaml
        api_integrations:
          - name: some_api_integration
            api_provider: GOOGLE_API_GATEWAY
            google_audience: <google_audience>
            enabled: true
            api_allowed_prefixes:
                - https://some_url.com
            comment: "Example GCP API integration"
        ```
    """


    resource_type = ResourceType.API_INTEGRATION
    props = Props(
        api_provider=EnumProp("api_provider", ApiProvider),
        google_audience=StringProp("google_audience"),
        api_allowed_prefixes=StringListProp("api_allowed_prefixes", parens=True),
        api_blocked_prefixes=StringListProp("api_blocked_prefixes", parens=True),
        enabled=BoolProp("enabled"),
        comment=StringProp("comment"),
    )
    scope = AccountScope()
    spec = _GCPAPIIntegration

    def __init__(
        self,
        name: str,
        api_provider: ApiProvider,
        google_audience: str,
        enabled: bool,
        api_allowed_prefixes: list[str],
        api_blocked_prefixes: list[str] = None,
        owner: str = "ACCOUNTADMIN",
        comment: str = None,
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._data: _GCPAPIIntegration = _GCPAPIIntegration(
            name=self._name,
            api_provider=api_provider,
            google_audience=google_audience,
            api_allowed_prefixes=api_allowed_prefixes,
            api_blocked_prefixes=api_blocked_prefixes,
            enabled=enabled,
            owner=owner,
            comment=comment,
        )

@dataclass(unsafe_hash=True)
class _GitAPIIntegration(ResourceSpec):
    name: ResourceName
    api_provider: ApiProvider
    enabled: bool
    api_allowed_prefixes: list[str]
    api_blocked_prefixes: list[str] = None
    owner: Role = "ACCOUNTADMIN"
    comment: str = None


class GitAPIIntegration(NamedResource, Resource):
    """
    Description:
        Manages API integrations in Snowflake, allowing external services to interact with Snowflake resources securely.
        This class supports creating, replacing, and checking the existence of API integrations with various configurations.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-api-integration

    Fields:
        name (string, required): The unique name of the API integration.
        api_provider (string or ApiProvider, required): The provider of the API service.
        api_allowed_prefixes (list, required): The list of allowed prefixes for the API endpoints.
        api_blocked_prefixes (list): The list of blocked prefixes for the API endpoints.
        enabled (bool, required): Specifies if the API integration is enabled. Defaults to TRUE.
        comment (string): A comment or description for the API integration.

    Python:

        ```python
        api_integration = APIIntegration(
            name="some_api_integration",
            api_provider="GIT_HTTPS_API",
            enabled=True,
            api_allowed_prefixes=["https://github.com/<org-name>"],
            comment="Example Git API integration"
        )
        ```

    Yaml:

        ```yaml
        api_integrations:
          - name: some_api_integration
            api_provider: GIT_HTTPS_API
            enabled: true
            api_allowed_prefixes:
                - https://github.com/<org-name>
            comment: "Example Git API integration"
        ```
    """


    resource_type = ResourceType.API_INTEGRATION
    props = Props(
        api_provider=EnumProp("api_provider", ApiProvider),
        api_allowed_prefixes=StringListProp("api_allowed_prefixes", parens=True),
        api_blocked_prefixes=StringListProp("api_blocked_prefixes", parens=True),
        enabled=BoolProp("enabled"),
        comment=StringProp("comment"),
    )
    scope = AccountScope()
    spec = _GitAPIIntegration

    def __init__(
        self,
        name: str,
        api_provider: ApiProvider,
        enabled: bool,
        api_allowed_prefixes: list[str],
        api_blocked_prefixes: list[str] = None,
        owner: str = "ACCOUNTADMIN",
        comment: str = None,
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._data: _GitAPIIntegration = _GitAPIIntegration(
            name=self._name,
            api_provider=api_provider,
            api_allowed_prefixes=api_allowed_prefixes,
            api_blocked_prefixes=api_blocked_prefixes,
            enabled=enabled,
            owner=owner,
            comment=comment,
        )

def _api_resolver(data: dict):
    aws_providers = {
        ApiProvider.AWS_API_GATEWAY,
        ApiProvider.AWS_PRIVATE_API_GATEWAY,
        ApiProvider.AWS_GOV_API_GATEWAY,
        ApiProvider.AWS_GOV_PRIVATE_API_GATEWAY,
    }

    api_provider = ApiProvider(data["api_provider"])

    if api_provider in aws_providers:
        return AWSAPIIntegration
    elif api_provider == ApiProvider.AZURE_API_MANAGEMENT:
        return AzureAPIIntegration
    elif api_provider == ApiProvider.GOOGLE_API_GATEWAY:
        return GCPAPIIntegration
    elif api_provider == ApiProvider.GIT_HTTPS_API:
        return GitAPIIntegration


Resource.__resolvers__[ResourceType.API_INTEGRATION] = _api_resolver
