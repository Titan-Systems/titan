from dataclasses import dataclass

from inflection import camelize

from .__resource import Resource, ResourceSpec
from ..enums import ParseableEnum, ResourceType
from ..parse import _resolve_resource_class
from ..props import Props, StringProp, BoolProp, EnumProp, StringListProp
from ..scope import AccountScope


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


@dataclass
class _EmailNotificationIntegration(ResourceSpec):
    name: str
    enabled: bool
    allowed_recipients: list[str]
    type: NotificationType = NotificationType.EMAIL
    owner: str = "ACCOUNTADMIN"
    comment: str = None

    def __post_init__(self):
        super().__post_init__()
        if self.allowed_recipients is not None and len(self.allowed_recipients) == 0:
            raise ValueError("allowed_recipients can't be empty")


class EmailNotificationIntegration(Resource):
    """
    CREATE [ OR REPLACE ] NOTIFICATION INTEGRATION [IF NOT EXISTS]
      <name>
      TYPE = EMAIL
      ENABLED = { TRUE | FALSE }
      ALLOWED_RECIPIENTS = ( '<email_address_1>' [ , ... '<email_address_N>' ] )
      [ COMMENT = '<string_literal>' ]
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
        super().__init__(**kwargs)
        self._data = _EmailNotificationIntegration(
            name=name,
            enabled=enabled,
            allowed_recipients=allowed_recipients,
            comment=comment,
            owner=owner,
        )


@dataclass
class _AWSOutboundNotificationIntegration(ResourceSpec):
    name: str
    enabled: bool
    aws_sns_topic_arn: str
    aws_sns_role_arn: str
    direction: NotificationDirection = NotificationDirection.OUTBOUND
    type: str = NotificationType.QUEUE
    notification_provider: str = NotificationProvider.AWS_SNS
    owner: str = "ACCOUNTADMIN"
    comment: str = None


class AWSOutboundNotificationIntegration(Resource):
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
        super().__init__(**kwargs)
        self._data = _AWSOutboundNotificationIntegration(
            name=name,
            enabled=enabled,
            aws_sns_topic_arn=aws_sns_topic_arn,
            aws_sns_role_arn=aws_sns_role_arn,
            owner=owner,
            comment=comment,
        )


@dataclass
class _GCPOutboundNotificationIntegration(ResourceSpec):
    name: str
    enabled: bool
    gcp_pubsub_topic_name: str
    direction: NotificationDirection = NotificationDirection.OUTBOUND
    type: str = NotificationType.QUEUE
    notification_provider: str = NotificationProvider.GCP_PUBSUB
    owner: str = "ACCOUNTADMIN"
    comment: str = None


class GCPOutboundNotificationIntegration(Resource):
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
        super().__init__(**kwargs)
        self._data = _GCPOutboundNotificationIntegration(
            name=name,
            enabled=enabled,
            gcp_pubsub_topic_name=gcp_pubsub_topic_name,
            owner=owner,
            comment=comment,
        )


@dataclass
class _AzureOutboundNotificationIntegration(ResourceSpec):
    name: str
    enabled: bool
    azure_event_grid_topic_endpoint: str
    azure_tenant_id: str
    direction: NotificationDirection = NotificationDirection.OUTBOUND
    type: str = NotificationType.QUEUE
    notification_provider: str = NotificationProvider.AZURE_EVENT_GRID
    owner: str = "ACCOUNTADMIN"
    comment: str = None


class AzureOutboundNotificationIntegration(Resource):
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
        super().__init__(**kwargs)
        self._data = _AzureOutboundNotificationIntegration(
            name=name,
            enabled=enabled,
            azure_event_grid_topic_endpoint=azure_event_grid_topic_endpoint,
            azure_tenant_id=azure_tenant_id,
            owner=owner,
            comment=comment,
        )


@dataclass
class _GCPInboundNotificationIntegration(ResourceSpec):
    name: str
    enabled: bool
    gcp_pubsub_subscription_name: str
    direction: NotificationDirection = NotificationDirection.INBOUND
    type: str = NotificationType.QUEUE
    notification_provider: str = NotificationProvider.GCP_PUBSUB
    owner: str = "ACCOUNTADMIN"
    comment: str = None


class GCPInboundNotificationIntegration(Resource):
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
        super().__init__(**kwargs)
        self._data = _GCPInboundNotificationIntegration(
            name=name,
            enabled=enabled,
            gcp_pubsub_subscription_name=gcp_pubsub_subscription_name,
            owner=owner,
            comment=comment,
        )


@dataclass
class _AzureInboundNotificationIntegration(ResourceSpec):
    name: str
    enabled: bool
    azure_storage_queue_primary_uri: str
    azure_tenant_id: str
    direction: NotificationDirection = NotificationDirection.INBOUND
    type: str = NotificationType.QUEUE
    notification_provider: str = NotificationProvider.AZURE_STORAGE_QUEUE
    owner: str = "ACCOUNTADMIN"
    comment: str = None


class AzureInboundNotificationIntegration(Resource):
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
        super().__init__(**kwargs)
        self._data = _AzureInboundNotificationIntegration(
            name=name,
            enabled=enabled,
            azure_storage_queue_primary_uri=azure_storage_queue_primary_uri,
            azure_tenant_id=azure_tenant_id,
            owner=owner,
            comment=comment,
        )


class NotificationIntegration:
    def __new__(
        cls,
        type: NotificationType,
        direction: NotificationDirection = None,
        notification_provider: NotificationProvider = None,
        **kwargs,
    ) -> Resource:
        if isinstance(type, str):
            type = NotificationType(type)
        if type == NotificationType.EMAIL:
            return EmailNotificationIntegration(**kwargs)
        elif type == NotificationType.QUEUE:
            if direction == NotificationDirection.INBOUND:
                if notification_provider == NotificationProvider.GCP_PUBSUB:
                    return GCPInboundNotificationIntegration(**kwargs)
                elif notification_provider == NotificationProvider.AZURE_STORAGE_QUEUE:
                    return AzureInboundNotificationIntegration(**kwargs)
            elif direction == NotificationDirection.OUTBOUND:
                if notification_provider == NotificationProvider.AWS_SNS:
                    return AWSOutboundNotificationIntegration(**kwargs)
                elif notification_provider == NotificationProvider.GCP_PUBSUB:
                    return GCPOutboundNotificationIntegration(**kwargs)
                elif notification_provider == NotificationProvider.AZURE_EVENT_GRID:
                    return AzureOutboundNotificationIntegration(**kwargs)
        raise Exception("Invalid Notification Integration")

    @classmethod
    def from_sql(cls, sql) -> Resource:
        # camelize(_resolve_resource_class(sql))
        # resource_cls = Resource.classes[]
        # return resource_cls.from_sql(sql)
        raise NotImplementedError
