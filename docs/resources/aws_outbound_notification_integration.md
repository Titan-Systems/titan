---
description: >-
  
---

# AWSOutboundNotificationIntegration

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-notification-integration)

Manages the configuration for AWS SNS notification integrations within Snowflake. This integration
allows specifying an AWS SNS topic that will receive a notification.


## Examples

### Python

```python
sns_notification_integration = NotificationIntegration(
    name="some_sns_notification_integration",
    type="queue",
    notification_provider="aws_sns",
    enabled=True,
    direction="outbound"
    aws_sns_topic_arn="arn:aws:sns:<region>:<account>:<sns_topic_name>",
    aws_sns_role_arn="arn:aws:iam::<account>:role/<iam_role_name>",
    comment="Example email notification integration"
)
```


### YAML

```yaml
notification_integrations:
  - name: some_sns_notification_integration
    type: QUEUE
    notification_provider: AWS_SNS
    enabled: true
    direction: OUTBOUND
    aws_sns_topic_arn: arn:aws:sns:<region>:<account>:<sns_topic_name>
    aws_sns_role_arn: arn:aws:iam::<account>:role/<iam_role_name>
    comment: Example sns notification integration
```


## Fields

* `name` (string, required) - The name of the AWS SNS notification integration.
* `type` (string, required) - Specifies that this is a notification integration between Snowflake and a 3rd party cloud message-queuing service.
* `notification_provider` (string, required) - Specifies AWS SNS as the 3rd party cloud message-queuing service.
* `enabled` (bool, required) - Specifies whether the notification integration is enabled.
* `direction` (string, required) - The direction of the notification integration ("OUTBOUND").
* `aws_sns_topic_arn` (string, required) - The ARN of the SNS topic that notifications will be sent to.
* `aws_sns_role_arn` (string, required) - The ARN of the IAM role that has permissions to push messages to the SNS topic.
* `comment` (string) - An optional comment about the notification integration.
* `owner` (string or [Role](role.md)) - The owner role of the notification integration. Defaults to "ACCOUNTADMIN".


