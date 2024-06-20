---
description: >-
  
---

# Warehouse

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-warehouse)

A virtual warehouse, often referred to simply as a "warehouse", is a cluster of compute resources in Snowflake. It provides the necessary CPU, memory, and temporary storage to execute SQL SELECT statements, perform DML operations such as INSERT, UPDATE, DELETE, and manage data loading and unloading.


## Examples

### Python

```python
warehouse = Warehouse(
    name="some_warehouse",
    owner="SYSADMIN",
    warehouse_type="STANDARD",
    warehouse_size="XSMALL",
    max_cluster_count=10,
    min_cluster_count=1,
    scaling_policy="STANDARD",
    auto_suspend=600,
    auto_resume=True,
    initially_suspended=False,
    resource_monitor=None,
    comment="This is a test warehouse",
    enable_query_acceleration=False,
    query_acceleration_max_scale_factor=1,
    max_concurrency_level=8,
    statement_queued_timeout_in_seconds=0,
    statement_timeout_in_seconds=172800,
    tags={"env": "test"},
)
```


### YAML

```yaml
warehouses:
  - name: some_warehouse
    owner: SYSADMIN
    warehouse_type: STANDARD
    warehouse_size: XSMALL
    max_cluster_count: 10
    min_cluster_count: 1
    scaling_policy: STANDARD
    auto_suspend: 600
    auto_resume: true
    initially_suspended: false
    resource_monitor: null
    comment: This is a test warehouse
    enable_query_acceleration: false
    query_acceleration_max_scale_factor: 1
    max_concurrency_level: 8
    statement_queued_timeout_in_seconds: 0
    statement_timeout_in_seconds: 172800
    tags:
      env: test
```


## Fields

* `name` (string, required) - The name of the warehouse.
* `owner` (string) - The owner of the warehouse. Defaults to "SYSADMIN".
* `warehouse_type` (string or [WarehouseType](warehouse_type.md)) - The type of the warehouse, either STANDARD or SNOWPARK-OPTIMIZED. Defaults to STANDARD.
* `warehouse_size` (string or [WarehouseSize](warehouse_size.md)) - The size of the warehouse which defines the compute and storage capacity.
* `max_cluster_count` (int) - The maximum number of clusters for the warehouse.
* `min_cluster_count` (int) - The minimum number of clusters for the warehouse.
* `scaling_policy` (string or [WarehouseScalingPolicy](warehouse_scaling_policy.md)) - The policy that defines how the warehouse scales.
* `auto_suspend` (int) - The time in seconds of inactivity after which the warehouse is automatically suspended.
* `auto_resume` (bool) - Whether the warehouse should automatically resume when queries are submitted.
* `initially_suspended` (bool) - Whether the warehouse should start in a suspended state.
* `resource_monitor` (string or [ResourceMonitor](resource_monitor.md)) - The resource monitor that tracks the warehouse's credit usage and other metrics.
* `comment` (string) - A comment about the warehouse.
* `enable_query_acceleration` (bool) - Whether query acceleration is enabled to improve performance.
* `query_acceleration_max_scale_factor` (int) - The maximum scale factor for query acceleration.
* `max_concurrency_level` (int) - The maximum number of concurrent queries that the warehouse can handle.
* `statement_queued_timeout_in_seconds` (int) - The time in seconds a statement can be queued before it times out.
* `statement_timeout_in_seconds` (int) - The time in seconds a statement can run before it times out.
* `tags` (dict) - Tags for the warehouse.


