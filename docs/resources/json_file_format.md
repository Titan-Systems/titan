---
description: >-
  
---

# JSONFileFormat

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-file-format)

A JSON file format in Snowflake.


## Examples

### Python

```python
file_format = JSONFileFormat(
    name="some_json_file_format",
    owner="SYSADMIN",
    compression="AUTO",
    date_format="AUTO",
    time_format="AUTO",
    timestamp_format="AUTO",
    binary_format=BinaryFormat.HEX,
    trim_space=False,
    null_if=["NULL"],
    file_extension="json",
    enable_octal=False,
    allow_duplicate=False,
    strip_outer_array=False,
    strip_null_values=False,
    replace_invalid_characters=False,
    ignore_utf8_errors=False,
    skip_byte_order_mark=True,
    comment="This is a JSON file format."
)
```


### YAML

```yaml
file_formats:
  - name: some_json_file_format
    owner: SYSADMIN
    compression: AUTO
    date_format: AUTO
    time_format: AUTO
    timestamp_format: AUTO
    binary_format: HEX
    trim_space: false
    null_if:
      - NULL
    file_extension: json
    enable_octal: false
    allow_duplicate: false
    strip_outer_array: false
    strip_null_values: false
    replace_invalid_characters: false
    ignore_utf8_errors: false
    skip_byte_order_mark: true
    comment: This is a JSON file format.
```


## Fields

* `name` (string, required) - The name of the file format.
* `owner` (string or [Role](role.md)) - The owner role of the file format. Defaults to "SYSADMIN".
* `compression` (string) - The compression type for the file format. Defaults to "AUTO".
* `date_format` (string) - The format used for date values. Defaults to "AUTO".
* `time_format` (string) - The format used for time values. Defaults to "AUTO".
* `timestamp_format` (string) - The format used for timestamp values. Defaults to "AUTO".
* `binary_format` ([BinaryFormat](binary_format.md)) - The format used for binary data. Defaults to HEX.
* `trim_space` (bool) - Whether to trim spaces. Defaults to False.
* `null_if` (list) - A list of strings to be interpreted as NULL.
* `file_extension` (string) - The file extension used for files of this format.
* `enable_octal` (bool) - Whether to enable octal values. Defaults to False.
* `allow_duplicate` (bool) - Whether to allow duplicate keys. Defaults to False.
* `strip_outer_array` (bool) - Whether to strip the outer array. Defaults to False.
* `strip_null_values` (bool) - Whether to strip null values. Defaults to False.
* `replace_invalid_characters` (bool) - Whether to replace invalid characters. Defaults to False.
* `ignore_utf8_errors` (bool) - Whether to ignore UTF-8 errors. Defaults to False.
* `skip_byte_order_mark` (bool) - Whether to skip the byte order mark. Defaults to True.
* `comment` (string) - A comment for the file format.


