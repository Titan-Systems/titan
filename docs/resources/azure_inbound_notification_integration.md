---
description: >-
  
---

# AzureInboundNotificationIntegration

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-notification-integration)

Manages the configuration for Azure Event Grid notification integrations within Snowflake. This integration
allows specifying an Azure Event Grid topic that will publish a notification to Snowflake.


## Examples

### Python

```python
event_grid_notification_integration = NotificationIntegration(
    name="some_event_grid_notification_integration",
    type="queue",
    notification_provider="azure_event_grid",
    enabled=True,
    azure_storage_queue_primary_uri="https://<storage_queue_account>.queue.core.windows.net/<storage_queue_name>",
    azure_tenant_id="<ad_directory_id>",
    comment="Example inbound event grid notification integration"
)
```


### YAML

```yaml
notification_integrations:
  - name: some_event_grid_notification_integration
    type: QUEUE
    notification_provider: AZURE_EVENT_GRID
    enabled: true
    azure_storage_queue_primary_uri: https://<storage_queue_account>.queue.core.windows.net/<storage_queue_name>
    azure_tenant_id: <ad_directory_id>
    comment: Example inbound event grid notification integration
```


## Fields

* `name` (string, required) - The name of the notification integration.
* `type` (string, required) - Specifies that this is a notification integration between Snowflake and a 3rd party cloud message-queuing service.
* `notification_provider` (string, required) - Specifies Azure Event Grid as the 3rd party cloud message-queuing service.
* `enabled` (bool, required) - Specifies whether the notification integration is enabled.
* `azure_storage_queue_primary_uri` (string, required) - The URL for the Azure Queue Storage queue create for Event Grid notifications.
* `azure_tenant_id` (string, required) - The ID of the Azure AD tenant used for identity management.
* `comment` (string) - An optional comment about the notification integration.
* `owner` (string or [Role](role.md)) - The owner role of the notification integration. Defaults to "ACCOUNTADMIN".


