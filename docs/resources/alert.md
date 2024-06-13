---
description: >-
  
---

# Alert

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-alert)

Alerts trigger notifications when certain conditions are met.


## Examples

### Python

```python
alert = Alert(
    name="some_alert",
    warehouse="some_warehouse",
    schedule="USING CRON * * * * *",
    condition="SELECT COUNT(*) FROM some_table",
    then="CALL SYSTEM$SEND_EMAIL('example@example.com', 'Alert Triggered', 'The alert condition was met.')",
)
```


### YAML

```yaml
alerts:
  - name: some_alert
    warehouse: some_warehouse
    schedule: USING CRON * * * * *
    condition: SELECT COUNT(*) FROM some_table
    then: CALL SYSTEM$SEND_EMAIL('example@example.com', 'Alert Triggered', 'The alert condition was met.')
```


## Fields

* `name` (string, required) - The name of the alert.
* `warehouse` (string or [Warehouse](warehouse.md)) - The name of the warehouse to run the query on.
* `schedule` (string) - The schedule for the alert to run on.
* `condition` (string) - The condition for the alert to trigger on.
* `then` (string) - The query to run when the alert triggers.
* `owner` (string or [Role](role.md)) - The owner role of the alert. Defaults to "SYSADMIN".
* `comment` (string) - A comment for the alert. Defaults to None.
* `tags` (dict) - Tags for the alert. Defaults to None.


