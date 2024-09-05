from dataclasses import dataclass

from ..enums import ParseableEnum, ResourceType
from ..props import BoolProp, EnumProp, Props, StringListProp, StringProp
from ..resource_name import ResourceName
from ..scope import AccountScope
from .resource import NamedResource, Resource, ResourceSpec
from .role import Role


class NotificationType(ParseableEnum):
    EMAIL = "EMAIL"
    QUEUE = "QUEUE"


class NotificationProvider(ParseableEnum):
    AWS_SNS = "AWS_SNS"
    GCP_PUBSUB = "GCP_PUBSUB"
    AZURE_STORAGE_QUEUE = "AZURE_STORAGE_QUEUE"
    AZURE_EVENT_GRID = "AZURE_EVENT_GRID"


class NotificationDirection(ParseableEnum):
    INBOUND = "INBOUND"
    OUTBOUND = "OUTBOUND"


@dataclass(unsafe_hash=True)
class _EmailNotificationIntegration(ResourceSpec):
    name: ResourceName
    enabled: bool
    allowed_recipients: list[str]
    type: NotificationType = NotificationType.EMAIL
    owner: Role = "ACCOUNTADMIN"
    comment: str = None

    def __post_init__(self):
        super().__post_init__()
        if self.allowed_recipients is not None and len(self.allowed_recipients) == 0:
            raise ValueError("allowed_recipients can't be empty")


class EmailNotificationIntegration(NamedResource, Resource):
    """
    Description:
        Manages the configuration for email-based notification integrations within Snowflake. This integration
        allows specifying recipients who will receive notifications via email.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-notification-integration

    Fields:
        name (string, required): The name of the email notification integration.
        enabled (bool, required): Specifies whether the notification integration is enabled.
        allowed_recipients (list): A list of email addresses that are allowed to receive notifications.
        comment (string): An optional comment about the notification integration.
        owner (string or Role): The owner role of the notification integration. Defaults to "ACCOUNTADMIN".

    Python:

        ```python
        email_notification_integration = EmailNotificationIntegration(
            name="some_email_notification_integration",
            enabled=True,
            allowed_recipients=["user1@example.com", "user2@example.com"],
            comment="Example email notification integration"
        )
        ```

    Yaml:

        ```yaml
        email_notification_integrations:
          - name: some_email_notification_integration
            enabled: true
            allowed_recipients:
              - user1@example.com
              - user2@example.com
            comment: "Example email notification integration"
        ```
    """

    resource_type = ResourceType.NOTIFICATION_INTEGRATION
    props = Props(
        type=EnumProp("type", [NotificationType.EMAIL]),
        enabled=BoolProp("enabled"),
        allowed_recipients=StringListProp("allowed_recipients", parens=True),
        comment=StringProp("comment"),
    )
    scope = AccountScope()
    spec = _EmailNotificationIntegration

    def __init__(
        self,
        name: str,
        enabled: bool,
        allowed_recipients: list[str],
        comment: str = None,
        owner: str = "ACCOUNTADMIN",
        **kwargs,
    ):
        kwargs.pop("type", None)
        super().__init__(name, **kwargs)
        self._data: _EmailNotificationIntegration = _EmailNotificationIntegration(
            name=self._name,
            enabled=enabled,
            allowed_recipients=allowed_recipients,
            comment=comment,
            owner=owner,
        )


@dataclass(unsafe_hash=True)
class _AWSOutboundNotificationIntegration(ResourceSpec):
    name: ResourceName
    enabled: bool
    aws_sns_topic_arn: str
    aws_sns_role_arn: str
    direction: NotificationDirection = NotificationDirection.OUTBOUND
    type: str = NotificationType.QUEUE
    notification_provider: str = NotificationProvider.AWS_SNS
    owner: Role = "ACCOUNTADMIN"
    comment: str = None


class AWSOutboundNotificationIntegration(NamedResource, Resource):
    """
    CREATE [ OR REPLACE ] NOTIFICATION INTEGRATION [IF NOT EXISTS]
      <name>
      ENABLED = { TRUE | FALSE }
      DIRECTION = OUTBOUND
      TYPE = QUEUE
      cloudProviderParamsPush
      [ COMMENT = '<string_literal>' ]

    cloudProviderParamsPush (for Amazon SNS) ::=

      NOTIFICATION_PROVIDER = AWS_SNS
      AWS_SNS_TOPIC_ARN = '<topic_arn>'
      AWS_SNS_ROLE_ARN = '<iam_role_arn>'
    """

    resource_type = ResourceType.NOTIFICATION_INTEGRATION
    props = Props(
        enabled=BoolProp("enabled"),
        direction=EnumProp("direction", [NotificationDirection.OUTBOUND]),
        type=EnumProp("type", [NotificationType.QUEUE]),
        notification_provider=EnumProp("notification_provider", [NotificationProvider.AWS_SNS]),
        aws_sns_topic_arn=StringProp("aws_sns_topic_arn"),
        aws_sns_role_arn=StringProp("aws_sns_role_arn"),
        comment=StringProp("comment"),
    )
    scope = AccountScope()
    spec = _AWSOutboundNotificationIntegration

    def __init__(
        self,
        name: str,
        enabled: bool,
        aws_sns_topic_arn: str,
        aws_sns_role_arn: str,
        owner: str = "ACCOUNTADMIN",
        comment: str = None,
        **kwargs,
    ):
        kwargs.pop("type", None)
        kwargs.pop("direction", None)
        kwargs.pop("notification_provider", None)
        super().__init__(name, **kwargs)
        self._data: _AWSOutboundNotificationIntegration = _AWSOutboundNotificationIntegration(
            name=self._name,
            enabled=enabled,
            aws_sns_topic_arn=aws_sns_topic_arn,
            aws_sns_role_arn=aws_sns_role_arn,
            owner=owner,
            comment=comment,
        )


@dataclass(unsafe_hash=True)
class _GCPOutboundNotificationIntegration(ResourceSpec):
    name: ResourceName
    enabled: bool
    gcp_pubsub_topic_name: str
    direction: NotificationDirection = NotificationDirection.OUTBOUND
    type: str = NotificationType.QUEUE
    notification_provider: str = NotificationProvider.GCP_PUBSUB
    owner: Role = "ACCOUNTADMIN"
    comment: str = None


class GCPOutboundNotificationIntegration(NamedResource, Resource):
    """
    CREATE [ OR REPLACE ] NOTIFICATION INTEGRATION [IF NOT EXISTS]
      <name>
      ENABLED = { TRUE | FALSE }
      DIRECTION = OUTBOUND
      TYPE = QUEUE
      cloudProviderParamsPush
      [ COMMENT = '<string_literal>' ]

    cloudProviderParamsPush (for Google Pub/Sub) ::=
      NOTIFICATION_PROVIDER = GCP_PUBSUB
      GCP_PUBSUB_TOPIC_NAME = '<topic_id>'
    """

    resource_type = ResourceType.NOTIFICATION_INTEGRATION
    props = Props(
        enabled=BoolProp("enabled"),
        direction=EnumProp("direction", [NotificationDirection.OUTBOUND]),
        type=EnumProp("type", [NotificationType.QUEUE]),
        notification_provider=EnumProp("notification_provider", [NotificationProvider.GCP_PUBSUB]),
        gcp_pubsub_topic_name=StringProp("gcp_pubsub_topic_name"),
        comment=StringProp("comment"),
    )
    scope = AccountScope()
    spec = _GCPOutboundNotificationIntegration

    def __init__(
        self,
        name: str,
        enabled: bool,
        gcp_pubsub_topic_name: str,
        owner: str = "ACCOUNTADMIN",
        comment: str = None,
        **kwargs,
    ):
        kwargs.pop("type", None)
        kwargs.pop("direction", None)
        kwargs.pop("notification_provider", None)
        super().__init__(name, **kwargs)
        self._data: _GCPOutboundNotificationIntegration = _GCPOutboundNotificationIntegration(
            name=self._name,
            enabled=enabled,
            gcp_pubsub_topic_name=gcp_pubsub_topic_name,
            owner=owner,
            comment=comment,
        )


@dataclass(unsafe_hash=True)
class _AzureOutboundNotificationIntegration(ResourceSpec):
    name: ResourceName
    enabled: bool
    azure_event_grid_topic_endpoint: str
    azure_tenant_id: str
    direction: NotificationDirection = NotificationDirection.OUTBOUND
    type: str = NotificationType.QUEUE
    notification_provider: str = NotificationProvider.AZURE_EVENT_GRID
    owner: Role = "ACCOUNTADMIN"
    comment: str = None


class AzureOutboundNotificationIntegration(NamedResource, Resource):
    """
    CREATE [ OR REPLACE ] NOTIFICATION INTEGRATION [IF NOT EXISTS]
      <name>
      ENABLED = { TRUE | FALSE }
      DIRECTION = OUTBOUND
      TYPE = QUEUE
      cloudProviderParamsPush
      [ COMMENT = '<string_literal>' ]

    cloudProviderParamsPush (for Microsoft Azure Event Grid) ::=
      NOTIFICATION_PROVIDER = AZURE_EVENT_GRID
      AZURE_EVENT_GRID_TOPIC_ENDPOINT = '<event_grid_topic_endpoint>'
      AZURE_TENANT_ID = '<directory_ID>';
    """

    resource_type = ResourceType.NOTIFICATION_INTEGRATION
    props = Props(
        enabled=BoolProp("enabled"),
        direction=EnumProp("direction", [NotificationDirection.OUTBOUND]),
        type=EnumProp("type", [NotificationType.QUEUE]),
        notification_provider=EnumProp("notification_provider", [NotificationProvider.AZURE_EVENT_GRID]),
        azure_event_grid_topic_endpoint=StringProp("azure_event_grid_topic_endpoint"),
        azure_tenant_id=StringProp("azure_tenant_id"),
        comment=StringProp("comment"),
    )
    scope = AccountScope()
    spec = _AzureOutboundNotificationIntegration

    def __init__(
        self,
        name: str,
        enabled: bool,
        azure_event_grid_topic_endpoint: str,
        azure_tenant_id: str,
        owner: str = "ACCOUNTADMIN",
        comment: str = None,
        **kwargs,
    ):
        kwargs.pop("type", None)
        kwargs.pop("direction", None)
        kwargs.pop("notification_provider", None)
        super().__init__(name, **kwargs)
        self._data: _AzureOutboundNotificationIntegration = _AzureOutboundNotificationIntegration(
            name=self._name,
            enabled=enabled,
            azure_event_grid_topic_endpoint=azure_event_grid_topic_endpoint,
            azure_tenant_id=azure_tenant_id,
            owner=owner,
            comment=comment,
        )


@dataclass(unsafe_hash=True)
class _GCPInboundNotificationIntegration(ResourceSpec):
    name: ResourceName
    enabled: bool
    gcp_pubsub_subscription_name: str
    direction: NotificationDirection = NotificationDirection.INBOUND
    type: str = NotificationType.QUEUE
    notification_provider: str = NotificationProvider.GCP_PUBSUB
    owner: Role = "ACCOUNTADMIN"
    comment: str = None


class GCPInboundNotificationIntegration(NamedResource, Resource):
    """
    CREATE [ OR REPLACE ] NOTIFICATION INTEGRATION [IF NOT EXISTS]
      <name>
      ENABLED = { TRUE | FALSE }
      TYPE = QUEUE
      cloudProviderParamsAuto
      [ COMMENT = '<string_literal>' ]

    cloudProviderParamsAuto (for Google Cloud Storage) ::=
      NOTIFICATION_PROVIDER = GCP_PUBSUB
      GCP_PUBSUB_SUBSCRIPTION_NAME = '<subscription_id>'
    """

    resource_type = ResourceType.NOTIFICATION_INTEGRATION
    props = Props(
        enabled=BoolProp("enabled"),
        type=EnumProp("type", [NotificationType.QUEUE]),
        notification_provider=EnumProp("notification_provider", [NotificationProvider.GCP_PUBSUB]),
        gcp_pubsub_subscription_name=StringProp("gcp_pubsub_subscription_name"),
        comment=StringProp("comment"),
    )
    scope = AccountScope()
    spec = _GCPInboundNotificationIntegration

    def __init__(
        self,
        name: str,
        enabled: bool,
        gcp_pubsub_subscription_name: str,
        owner: str = "ACCOUNTADMIN",
        comment: str = None,
        **kwargs,
    ):
        kwargs.pop("type", None)
        kwargs.pop("notification_provider", None)
        super().__init__(name, **kwargs)
        self._data: _GCPInboundNotificationIntegration = _GCPInboundNotificationIntegration(
            name=self._name,
            enabled=enabled,
            gcp_pubsub_subscription_name=gcp_pubsub_subscription_name,
            owner=owner,
            comment=comment,
        )


@dataclass(unsafe_hash=True)
class _AzureInboundNotificationIntegration(ResourceSpec):
    name: ResourceName
    enabled: bool
    azure_storage_queue_primary_uri: str
    azure_tenant_id: str
    direction: NotificationDirection = NotificationDirection.INBOUND
    type: str = NotificationType.QUEUE
    notification_provider: str = NotificationProvider.AZURE_STORAGE_QUEUE
    owner: Role = "ACCOUNTADMIN"
    comment: str = None


class AzureInboundNotificationIntegration(NamedResource, Resource):
    """
    CREATE [ OR REPLACE ] NOTIFICATION INTEGRATION [IF NOT EXISTS]
      <name>
      ENABLED = { TRUE | FALSE }
      TYPE = QUEUE
      cloudProviderParamsAuto
      [ COMMENT = '<string_literal>' ]

    cloudProviderParamsAuto (for Microsoft Azure Storage) ::=
      NOTIFICATION_PROVIDER = AZURE_STORAGE_QUEUE
      AZURE_STORAGE_QUEUE_PRIMARY_URI = '<queue_URL>'
      AZURE_TENANT_ID = '<directory_ID>';
    """

    resource_type = ResourceType.NOTIFICATION_INTEGRATION
    props = Props(
        enabled=BoolProp("enabled"),
        type=EnumProp("type", [NotificationType.QUEUE]),
        notification_provider=EnumProp("notification_provider", [NotificationProvider.AZURE_STORAGE_QUEUE]),
        azure_storage_queue_primary_uri=StringProp("azure_storage_queue_primary_uri"),
        azure_tenant_id=StringProp("azure_tenant_id"),
        comment=StringProp("comment"),
    )
    scope = AccountScope()
    spec = _AzureInboundNotificationIntegration

    def __init__(
        self,
        name: str,
        enabled: bool,
        azure_storage_queue_primary_uri: str,
        azure_tenant_id: str,
        owner: str = "ACCOUNTADMIN",
        comment: str = None,
        **kwargs,
    ):
        kwargs.pop("type", None)
        kwargs.pop("notification_provider", None)
        super().__init__(name, **kwargs)
        self._data: _AzureInboundNotificationIntegration = _AzureInboundNotificationIntegration(
            name=self._name,
            enabled=enabled,
            azure_storage_queue_primary_uri=azure_storage_queue_primary_uri,
            azure_tenant_id=azure_tenant_id,
            owner=owner,
            comment=comment,
        )


def _notification_resolver(data: dict):
    if NotificationType(data["type"]) == NotificationType.EMAIL:
        return EmailNotificationIntegration
    if "direction" in data and "notification_provider" in data:
        direction = NotificationDirection(data["direction"])
        provider = NotificationProvider(data["notification_provider"])
        if direction == NotificationDirection.INBOUND:
            if provider == NotificationProvider.AZURE_STORAGE_QUEUE:
                return AzureInboundNotificationIntegration
            elif provider == NotificationProvider.GCP_PUBSUB:
                return GCPInboundNotificationIntegration
        elif direction == NotificationDirection.OUTBOUND:
            if provider == NotificationProvider.AWS_SNS:
                return AWSOutboundNotificationIntegration
            elif provider == NotificationProvider.AZURE_EVENT_GRID:
                return AzureOutboundNotificationIntegration
            elif provider == NotificationProvider.GCP_PUBSUB:
                return GCPOutboundNotificationIntegration
    raise ValueError("Invalid direction or notification provider")


Resource.__resolvers__[ResourceType.NOTIFICATION_INTEGRATION] = _notification_resolver
