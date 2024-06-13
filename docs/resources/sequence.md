---
description: >-
  
---

# Sequence

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-sequence)

Manages the creation and configuration of sequences in Snowflake, which are objects that generate numeric values according to a specified sequence.


## Examples

### Python

```python
sequence = Sequence(
    name="some_sequence",
    owner="SYSADMIN",
    start=100,
    increment=10,
    comment="This is a sample sequence."
)
```


### YAML

```yaml
sequences:
  - name: some_sequence
    owner: SYSADMIN
    start: 100
    increment: 10
    comment: This is a sample sequence.
```


## Fields

* `name` (string, required) - The name of the sequence.
* `owner` (string or [Role](role.md)) - The owner role of the sequence. Defaults to "SYSADMIN".
* `start` (int) - The starting value of the sequence.
* `increment` (int) - The value by which the sequence is incremented.
* `comment` (string) - A comment for the sequence.


