---
description: >-
  
---

# Task

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-task)

Represents a scheduled task in Snowflake that performs a specified SQL statement at a recurring interval.


## Examples

### Python

```python
task = Task(
    name="some_task",
    warehouse="some_warehouse",
    schedule="USING CRON 0 9 * * * UTC",
    state="SUSPENDED",
    as_="SELECT 1"
)
```


### YAML

```yaml
tasks:
  - name: some_task
    warehouse: some_warehouse
    schedule: "USING CRON 0 9 * * * UTC"
    state: SUSPENDED
    as_: |
        SELECT 1
```


## Fields

* `warehouse` (string or [Warehouse](warehouse.md)) - The warehouse used by the task.
* `user_task_managed_initial_warehouse_size` (string or [WarehouseSize](warehouse_size.md)) - The initial warehouse size when the task is managed by the user. Defaults to None.
* `schedule` (string) - The schedule on which the task runs.
* `config` (string) - Configuration settings for the task.
* `allow_overlapping_execution` (bool) - Whether the task can have overlapping executions.
* `user_task_timeout_ms` (int) - The timeout in milliseconds after which the task is aborted.
* `suspend_task_after_num_failures` (int) - The number of consecutive failures after which the task is suspended.
* `error_integration` (string) - The integration used for error handling.
* `copy_grants` (bool) - Whether to copy grants from the referenced objects.
* `comment` (string) - A comment for the task.
* `after` (list) - A list of tasks that must be completed before this task runs.
* `when` (string) - A conditional expression that determines when the task runs.
* `as_` (string) - The SQL statement that the task executes.
* `state` (string or [TaskState](task_state.md), required) - The initial state of the task. Defaults to SUSPENDED.


