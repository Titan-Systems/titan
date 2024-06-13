---
description: >-
  
---

# Service

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-service)

Service is a managed resource in Snowflake that allows users to run instances of their applications
as a collection of containers on a compute pool. Each service instance can handle incoming traffic
with the help of a load balancer if multiple instances are run.


## Examples

### Python

```python
service = Service(
    name="some_service",
    compute_pool="some_compute_pool",
    stage="@tutorial_stage",
    yaml_file_stage_path="echo_spec.yaml",
    specification="FROM SPECIFICATION $$some_specification$$",
    external_access_integrations=["some_integration"],
    auto_resume=True,
    min_instances=1,
    max_instances=2,
    query_warehouse="some_warehouse",
    tags={"key": "value"},
    comment="This is a sample service."
)
```


### YAML

```yaml
services:
  - name: some_service
    compute_pool: some_compute_pool
    stage: @tutorial_stage
    yaml_file_stage_path: echo_spec.yaml
    specification: FROM SPECIFICATION $$some_specification$$
    external_access_integrations:
      - some_integration
    auto_resume: true
    min_instances: 1
    max_instances: 2
    query_warehouse: some_warehouse
    tags:
      key: value
    comment: This is a sample service.
```


## Fields

* `name` (string, required) - The unique identifier for the service within the schema.
* `compute_pool` (string or [ComputePool](compute_pool.md), required) - The compute pool on which the service runs.
* `stage` (string) - The Snowflake internal stage where the specification file is stored.
* `yaml_file_stage_path` (string) - The path to the service specification file on the stage.
* `specification` (string) - The service specification as a string.
* `external_access_integrations` (list) - The names of external access integrations for the service.
* `auto_resume` (bool) - Specifies whether to automatically resume the service when a function or ingress is called. Defaults to True.
* `min_instances` (int) - The minimum number of service instances to run.
* `max_instances` (int) - The maximum number of service instances to run.
* `query_warehouse` (string or [Warehouse](warehouse.md)) - The warehouse to use if a service container connects to Snowflake to execute a query.
* `tags` (dict) - Tags associated with the service.
* `comment` (string) - A comment for the service.


