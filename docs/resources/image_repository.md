---
description: >-
  
---

# ImageRepository

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-image-repository)

An image repository in Snowflake is a storage unit within a schema that allows for the management of OCIv2-compliant container images.


## Examples

### Python

```python
image_repository = ImageRepository(
    name="some_image_repository",
)
```


### YAML

```yaml
image_repositories:
  - name: some_image_repository
```


## Fields

* `name` (string, required) - The unique identifier for the image repository within the schema.
* `owner` (string or [Role](role.md)) - The owner role of the image repository. Defaults to "SYSADMIN".


