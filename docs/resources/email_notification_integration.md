---
description: >-
  
---

# EmailNotificationIntegration

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-notification-integration)

Manages the configuration for email-based notification integrations within Snowflake. This integration
allows specifying recipients who will receive notifications via email.


## Examples

### Python

```python
email_notification_integration = NotificationIntegration(
    name="some_email_notification_integration",
    type="email",
    enabled=True,
    allowed_recipients=["user1@example.com", "user2@example.com"],
    comment="Example email notification integration"
)
```


### YAML

```yaml
notification_integrations:
  - name: some_email_notification_integration
    type: EMAIL
    enabled: true
    allowed_recipients:
      - user1@example.com
      - user2@example.com
    comment: "Example email notification integration"
```


## Fields

* `name` (string, required) - The name of the email notification integration.
* `type` (string, required) - Specifies that this is an email notification integration.
* `enabled` (bool, required) - Specifies whether the notification integration is enabled.
* `allowed_recipients` (list) - A list of email addresses that are allowed to receive notifications.
* `comment` (string) - An optional comment about the notification integration.
* `owner` (string or [Role](role.md)) - The owner role of the notification integration. Defaults to "ACCOUNTADMIN".


