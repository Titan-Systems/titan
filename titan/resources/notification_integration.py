from typing import List

from . import Resource
from .base import AccountScoped
from ..props import Props, StringProp, BoolProp, EnumProp, StringListProp
from ..enums import ParseableEnum
from ..parse import _resolve_resource_class


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


class EmailNotificationIntegration(Resource, AccountScoped):
    """
    CREATE [ OR REPLACE ] NOTIFICATION INTEGRATION [IF NOT EXISTS]
      <name>
      TYPE = EMAIL
      ENABLED = { TRUE | FALSE }
      ALLOWED_RECIPIENTS = ( '<email_address_1>' [ , ... '<email_address_N>' ] )
      [ COMMENT = '<string_literal>' ]
    """

    resource_type = "NOTIFICATION INTEGRATION"
    props = Props(
        type=EnumProp("type", [NotificationType.EMAIL]),
        enabled=BoolProp("enabled"),
        allowed_recipients=StringListProp("allowed_recipients"),
        comment=StringProp("comment"),
    )

    name: str
    owner: str = "SYSADMIN"
    type: NotificationType = NotificationType.EMAIL
    enabled: bool
    allowed_recipients: List[str]
    comment: str = None


class AWSOutboundNotificationIntegration(Resource, AccountScoped):
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

    resource_type = "NOTIFICATION INTEGRATION"
    props = Props(
        enabled=BoolProp("enabled"),
        direction=EnumProp("direction", [NotificationDirection.OUTBOUND]),
        type=EnumProp("type", [NotificationType.QUEUE]),
        notification_provider=EnumProp("notification_provider", [NotificationProvider.AWS_SNS]),
        aws_sns_topic_arn=StringProp("aws_sns_topic_arn"),
        aws_sns_role_arn=StringProp("aws_sns_role_arn"),
        comment=StringProp("comment"),
    )

    name: str
    owner: str = "SYSADMIN"
    enabled: bool
    direction: NotificationDirection = NotificationDirection.OUTBOUND
    type: str = NotificationType.QUEUE
    notification_provider: str = NotificationProvider.AWS_SNS
    aws_sns_topic_arn: str
    aws_sns_role_arn: str
    comment: str = None


class GCPOutboundNotificationIntegration(Resource, AccountScoped):
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

    resource_type = "NOTIFICATION INTEGRATION"
    props = Props(
        enabled=BoolProp("enabled"),
        direction=EnumProp("direction", [NotificationDirection.OUTBOUND]),
        type=EnumProp("type", [NotificationType.QUEUE]),
        notification_provider=EnumProp("notification_provider", [NotificationProvider.GCP_PUBSUB]),
        gcp_pubsub_topic_name=StringProp("gcp_pubsub_topic_name"),
        comment=StringProp("comment"),
    )

    name: str
    owner: str = "SYSADMIN"
    enabled: bool
    direction: NotificationDirection = NotificationDirection.OUTBOUND
    type: str = NotificationType.QUEUE
    notification_provider: str = NotificationProvider.AWS_SNS
    gcp_pubsub_topic_name: str
    comment: str = None


class AzureOutboundNotificationIntegration(Resource, AccountScoped):
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

    resource_type = "NOTIFICATION INTEGRATION"
    props = Props(
        enabled=BoolProp("enabled"),
        direction=EnumProp("direction", [NotificationDirection.OUTBOUND]),
        type=EnumProp("type", [NotificationType.QUEUE]),
        notification_provider=EnumProp("notification_provider", [NotificationProvider.AZURE_EVENT_GRID]),
        azure_event_grid_topic_endpoint=StringProp("azure_event_grid_topic_endpoint"),
        azure_tenant_id=StringProp("azure_tenant_id"),
        comment=StringProp("comment"),
    )

    name: str
    owner: str = "SYSADMIN"
    enabled: bool
    direction: NotificationDirection = NotificationDirection.OUTBOUND
    type: str = NotificationType.QUEUE
    notification_provider: str = NotificationProvider.AZURE_EVENT_GRID
    azure_event_grid_topic_endpoint: str
    azure_tenant_id: str
    comment: str = None


class GCPInboundNotificationIntegration(Resource, AccountScoped):
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

    resource_type = "NOTIFICATION INTEGRATION"
    props = Props(
        enabled=BoolProp("enabled"),
        type=EnumProp("type", [NotificationType.QUEUE]),
        notification_provider=EnumProp("notification_provider", [NotificationProvider.GCP_PUBSUB]),
        gcp_pubsub_subscription_name=StringProp("gcp_pubsub_subscription_name"),
        comment=StringProp("comment"),
    )

    name: str
    owner: str = "SYSADMIN"
    enabled: bool
    direction: NotificationDirection = NotificationDirection.INBOUND
    type: str = NotificationType.QUEUE
    notification_provider: str = NotificationProvider.GCP_PUBSUB
    gcp_pubsub_subscription_name: str
    comment: str = None


class AzureInboundNotificationIntegration(Resource, AccountScoped):
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

    resource_type = "NOTIFICATION INTEGRATION"
    props = Props(
        enabled=BoolProp("enabled"),
        type=EnumProp("type", [NotificationType.QUEUE]),
        notification_provider=EnumProp("notification_provider", [NotificationProvider.AZURE_STORAGE_QUEUE]),
        azure_storage_queue_primary_uri=StringProp("azure_storage_queue_primary_uri"),
        azure_tenant_id=StringProp("azure_tenant_id"),
        comment=StringProp("comment"),
    )

    name: str
    owner: str = "SYSADMIN"
    enabled: bool
    direction: NotificationDirection = NotificationDirection.INBOUND
    type: str = NotificationType.QUEUE
    notification_provider: str = NotificationProvider.AZURE_STORAGE_QUEUE
    azure_storage_queue_primary_uri: str
    azure_tenant_id: str
    comment: str = None


class NotificationIntegration(Resource):
    def __init__(self, *args, **kwargs):
        raise NotImplementedError

    def __new__(
        cls,
        type: NotificationType,
        direction: NotificationDirection = None,
        notification_provider: NotificationProvider = None,
        **kwargs,
    ):
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
    def from_sql(cls, sql):
        resource_cls = Resource.classes[_resolve_resource_class(sql)]
        return resource_cls.from_sql(sql)
