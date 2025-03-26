---
description: >-
  
---

# GCPOutboundNotificationIntegration

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-notification-integration)

Manages the configuration for Google Pub/Sub notification integrations within Snowflake. This integration
allows specifying an Google Pub/Sub topic that will receive a notification.


## Examples

### Python

```python
pubsub_notification_integration = NotificationIntegration(
    name="some_pubsub_notification_integration",
    type="queue",
    notification_provider="gcp_pubsub",
    enabled=True,
    direction="outbound"
    gcp_pubsub_topic_name="<topic_id>",
    comment="Example outbound pubsub notification integration"
)
```


### YAML

```yaml
notification_integrations:
  - name: some_pubsub_notification_integration
    type: QUEUE
    notification_provider: GCP_PUBSUB
    enabled: true
    direction: OUTBOUND
    gcp_pubsub_topic_name: <topic_id>
    comment: Example outbound pubsub notification integration
```


## Fields

* `name` (string, required) - The name of the notification integration.
* `type` (string, required) - Specifies that this is a notification integration between Snowflake and a 3rd party cloud message-queuing service.
* `notification_provider` (string, required) - Specifies Google Pub/Sub as the 3rd party cloud message-queuing service.
* `enabled` (bool, required) - Specifies whether the notification integration is enabled.
* `direction` (string, required) - The direction of the notification integration ("OUTBOUND").
* `gcp_pubsub_topic_name` (string, required) - The ID of the Pub/Sub topic that notifications will be sent to.
* `comment` (string) - An optional comment about the notification integration.
* `owner` (string or [Role](role.md)) - The owner role of the notification integration. Defaults to "ACCOUNTADMIN".


