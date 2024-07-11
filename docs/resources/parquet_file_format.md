---
description: >-
  
---

# ParquetFileFormat

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-file-format)

A Parquet file format in Snowflake.


## Examples

### Python

```python
file_format = ParquetFileFormat(
    name="some_file_format",
    owner="SYSADMIN",
    compression="AUTO",
    binary_as_text=True,
    trim_space=False,
    replace_invalid_characters=False,
    null_if=["NULL"],
    comment="This is a Parquet file format."
)
```


### YAML

```yaml
file_formats:
  - name: some_file_format
    owner: SYSADMIN
    compression: AUTO
    binary_as_text: true
    trim_space: false
    replace_invalid_characters: false
    null_if:
      - NULL
    comment: This is a Parquet file format.
```


## Fields

* `name` (string, required) - The name of the file format.
* `owner` (string or [Role](role.md)) - The owner role of the file format. Defaults to "SYSADMIN".
* `compression` (string) - The compression type for the file format. Defaults to "AUTO".
* `binary_as_text` (bool) - Whether to interpret binary data as text. Defaults to True.
* `trim_space` (bool) - Whether to trim spaces. Defaults to False.
* `replace_invalid_characters` (bool) - Whether to replace invalid characters. Defaults to False.
* `null_if` (list) - A list of strings to be interpreted as NULL.
* `comment` (string) - A comment for the file format.


