---
description: >-
  
---

# Tag

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-tag)

Represents a tag in Snowflake, which can be used to label various resources for better management and categorization.


## Examples

### Python

```python
tag = Tag(
    name="cost_center",
    allowed_values=["finance", "engineering", "sales"],
    comment="This is a sample tag",
)
```


### YAML

```yaml
tags:
  - name: cost_center
    comment: This is a sample tag
    allowed_values:
      - finance
      - engineering
      - sales
```


## Fields

* `name` (string, required) - The name of the tag.
* `allowed_values` (list) - A list of allowed values for the tag.
* `comment` (string) - A comment or description for the tag.


