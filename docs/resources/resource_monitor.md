---
description: >-
  
---

# ResourceMonitor

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-resource-monitor)

Manages the monitoring of resource usage within an account.


## Examples

### Python

```python
resource_monitor = ResourceMonitor(
    name="some_resource_monitor",
    credit_quota=1000,
    frequency="DAILY",
    start_timestamp="2049-01-01 00:00",
    end_timestamp="2049-12-31 23:59",
    notify_users=["user1", "user2"]
)
```


### YAML

```yaml
resource_monitors:
  - name: some_resource_monitor
    credit_quota: 1000
    frequency: DAILY
    start_timestamp: "2049-01-01 00:00"
    end_timestamp: "2049-12-31 23:59"
    notify_users:
      - user1
      - user2
```


## Fields

* `name` (string, required) - The name of the resource monitor.
* `credit_quota` (int) - The amount of credits that can be used by this monitor. Defaults to None.
* `frequency` (string or [ResourceMonitorFrequency](resource_monitor_frequency.md)) - The frequency of monitoring. Defaults to None.
* `start_timestamp` (string) - The start time for the monitoring period. Defaults to None.
* `end_timestamp` (string) - The end time for the monitoring period. Defaults to None.
* `notify_users` (list) - A list of users to notify when thresholds are reached. Defaults to None.


