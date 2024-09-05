from dataclasses import dataclass

from ..enums import BinaryFormat, Compression, FileType, ResourceType
from ..props import (
    BoolProp,
    EnumProp,
    IntProp,
    Props,
    StringListProp,
    StringProp,
)
from ..resource_name import ResourceName
from ..role_ref import RoleRef
from ..scope import SchemaScope
from .resource import NamedResource, Resource, ResourceSpec


@dataclass(unsafe_hash=True)
class _CSVFileFormat(ResourceSpec):
    name: ResourceName
    owner: RoleRef = "SYSADMIN"
    type: FileType = "CSV"
    compression: Compression = "AUTO"
    record_delimiter: str = "\n"
    field_delimiter: str = ","
    file_extension: str = None
    parse_header: bool = False
    skip_header: int = 0
    skip_blank_lines: bool = False
    date_format: str = "AUTO"
    time_format: str = "AUTO"
    timestamp_format: str = "AUTO"
    binary_format: BinaryFormat = "HEX"
    escape: str = None
    escape_unenclosed_field: str = "\\"
    trim_space: bool = False
    field_optionally_enclosed_by: str = None
    null_if: list[str] = None
    error_on_column_count_mismatch: bool = True
    replace_invalid_characters: bool = False
    empty_field_as_null: bool = True
    skip_byte_order_mark: bool = True
    encoding: str = "UTF8"
    comment: str = None

    def __post_init__(self):
        super().__post_init__()
        if self.null_if is None:
            self.null_if = ["\n"]


class CSVFileFormat(NamedResource, Resource):
    """
    Description:
        Defines the specifications for a CSV file format in Snowflake, including delimiters, encoding, and compression options.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-file-format

    Fields:
        compression (string or Compression): The compression type used for the file format.
        record_delimiter (string): Specifies the character that delimits records. Defaults to "\n".
        field_delimiter (string): Specifies the character that delimits fields.
        file_extension (string): The file extension used for files of this format.
        parse_header (bool): Whether to parse the first line of the file as a header. Defaults to False.
        skip_header (int): The number of header lines to skip before parsing.
        skip_blank_lines (bool): Whether to skip over blank lines. Defaults to False.
        date_format (string): The format used for date values. Defaults to "AUTO".
        time_format (string): The format used for time values. Defaults to "AUTO".
        timestamp_format (string): The format used for timestamp values. Defaults to "AUTO".
        binary_format (string or BinaryFormat): The format used for binary data. Defaults to HEX.
        escape (string): The escape character used in the file format.
        escape_unenclosed_field (string): The escape character for unenclosed fields. Defaults to "\\".
        trim_space (bool): Whether to trim spaces from fields. Defaults to False.
        field_optionally_enclosed_by (string): A character that may optionally enclose fields.
        null_if (list): A list of string values that should be interpreted as NULL.
        error_on_column_count_mismatch (bool, required): Whether to raise an error on column count mismatch. Defaults to True.
        replace_invalid_characters (bool): Whether to replace invalid characters. Defaults to False.
        empty_field_as_null (bool): Whether to treat empty fields as NULL.
        skip_byte_order_mark (bool): Whether to skip the byte order mark. Defaults to True.
        encoding (string): The file encoding. Defaults to "UTF8".
        comment (string): A comment regarding the file format.

    Python:

        ```python
        csv_file_format = CSVFileFormat(
            name="some_csv_file_format",
            compression=Compression.GZIP,
            field_delimiter=',',
            file_extension='csv',
            parse_header=True
        )
        ```

    Yaml:

        ```yaml
        csv_file_formats:
          - name: some_csv_file_format
            compression: GZIP
            field_delimiter: ','
            file_extension: 'csv'
            parse_header: true
        ```
    """

    resource_type = ResourceType.FILE_FORMAT
    props = Props(
        type=EnumProp("type", [FileType.CSV]),
        compression=EnumProp("compression", Compression),
        record_delimiter=StringProp("record_delimiter", alt_tokens=["NONE"]),
        field_delimiter=StringProp("field_delimiter", alt_tokens=["NONE"]),
        file_extension=StringProp("file_extension"),
        parse_header=BoolProp("parse_header"),
        skip_header=IntProp("skip_header"),
        skip_blank_lines=BoolProp("skip_blank_lines"),
        date_format=StringProp("date_format", alt_tokens=["AUTO"]),
        time_format=StringProp("time_format", alt_tokens=["AUTO"]),
        timestamp_format=StringProp("timestamp_format", alt_tokens=["AUTO"]),
        binary_format=EnumProp("binary_format", BinaryFormat),
        escape=StringProp("escape", alt_tokens=["NONE"]),
        escape_unenclosed_field=StringProp("escape_unenclosed_field", alt_tokens=["NONE"]),
        trim_space=BoolProp("trim_space"),
        field_optionally_enclosed_by=StringProp("field_optionally_enclosed_by", alt_tokens=["NONE"]),
        null_if=StringListProp("null_if", parens=True),
        error_on_column_count_mismatch=BoolProp("error_on_column_count_mismatch"),
        replace_invalid_characters=BoolProp("replace_invalid_characters"),
        empty_field_as_null=BoolProp("empty_field_as_null"),
        skip_byte_order_mark=BoolProp("skip_byte_order_mark"),
        encoding=StringProp("encoding", alt_tokens=["UTF8"]),
        comment=StringProp("comment"),
    )
    scope = SchemaScope()
    spec = _CSVFileFormat

    def __init__(
        self,
        name: str,
        owner: str = "SYSADMIN",
        compression: Compression = "AUTO",
        record_delimiter: str = "\n",
        field_delimiter: str = ",",
        file_extension: str = None,
        parse_header: bool = False,
        skip_header: int = 0,
        skip_blank_lines: bool = False,
        date_format: str = "AUTO",
        time_format: str = "AUTO",
        timestamp_format: str = "AUTO",
        binary_format: BinaryFormat = "HEX",
        escape: str = None,
        escape_unenclosed_field: str = "\\",
        trim_space: bool = False,
        field_optionally_enclosed_by: str = None,
        null_if: list[str] = None,
        error_on_column_count_mismatch: bool = True,
        replace_invalid_characters: bool = False,
        empty_field_as_null: bool = True,
        skip_byte_order_mark: bool = True,
        encoding: str = "UTF8",
        comment: str = None,
        **kwargs,
    ):
        kwargs.pop("type", None)
        super().__init__(name, **kwargs)
        self._data: _CSVFileFormat = _CSVFileFormat(
            name=self._name,
            owner=owner,
            compression=compression,
            record_delimiter=record_delimiter,
            field_delimiter=field_delimiter,
            file_extension=file_extension,
            parse_header=parse_header,
            skip_header=skip_header,
            skip_blank_lines=skip_blank_lines,
            date_format=date_format,
            time_format=time_format,
            timestamp_format=timestamp_format,
            binary_format=binary_format,
            escape=escape,
            escape_unenclosed_field=escape_unenclosed_field,
            trim_space=trim_space,
            field_optionally_enclosed_by=field_optionally_enclosed_by,
            null_if=null_if,
            error_on_column_count_mismatch=error_on_column_count_mismatch,
            replace_invalid_characters=replace_invalid_characters,
            empty_field_as_null=empty_field_as_null,
            skip_byte_order_mark=skip_byte_order_mark,
            encoding=encoding,
            comment=comment,
        )


@dataclass(unsafe_hash=True)
class _JSONFileFormat(ResourceSpec):
    name: ResourceName
    owner: RoleRef = "SYSADMIN"
    type: FileType = FileType.JSON
    compression: Compression = Compression.AUTO
    date_format: str = "AUTO"
    time_format: str = "AUTO"
    timestamp_format: str = "AUTO"
    binary_format: BinaryFormat = BinaryFormat.HEX
    trim_space: bool = False
    null_if: list[str] = None
    file_extension: str = None
    enable_octal: bool = False
    allow_duplicate: bool = False
    strip_outer_array: bool = False
    strip_null_values: bool = False
    replace_invalid_characters: bool = False
    ignore_utf8_errors: bool = False
    skip_byte_order_mark: bool = True
    comment: str = None

    def __post_init__(self):
        super().__post_init__()
        if self.null_if is None:
            self.null_if = []


class JSONFileFormat(NamedResource, Resource):
    """
    Description:
        A JSON file format in Snowflake.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-file-format

    Fields:
        name (string, required): The name of the file format.
        owner (string or Role): The owner role of the file format. Defaults to "SYSADMIN".
        compression (string): The compression type for the file format. Defaults to "AUTO".
        date_format (string): The format used for date values. Defaults to "AUTO".
        time_format (string): The format used for time values. Defaults to "AUTO".
        timestamp_format (string): The format used for timestamp values. Defaults to "AUTO".
        binary_format (BinaryFormat): The format used for binary data. Defaults to HEX.
        trim_space (bool): Whether to trim spaces. Defaults to False.
        null_if (list): A list of strings to be interpreted as NULL.
        file_extension (string): The file extension used for files of this format.
        enable_octal (bool): Whether to enable octal values. Defaults to False.
        allow_duplicate (bool): Whether to allow duplicate keys. Defaults to False.
        strip_outer_array (bool): Whether to strip the outer array. Defaults to False.
        strip_null_values (bool): Whether to strip null values. Defaults to False.
        replace_invalid_characters (bool): Whether to replace invalid characters. Defaults to False.
        ignore_utf8_errors (bool): Whether to ignore UTF-8 errors. Defaults to False.
        skip_byte_order_mark (bool): Whether to skip the byte order mark. Defaults to True.
        comment (string): A comment for the file format.

    Python:

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

    Yaml:

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
    """

    resource_type = ResourceType.FILE_FORMAT
    props = Props(
        type=EnumProp("type", [FileType.JSON]),
        compression=EnumProp("compression", Compression),
        date_format=StringProp("date_format", alt_tokens=["AUTO"]),
        time_format=StringProp("time_format", alt_tokens=["AUTO"]),
        timestamp_format=StringProp("timestamp_format", alt_tokens=["AUTO"]),
        binary_format=EnumProp("binary_format", BinaryFormat),
        trim_space=BoolProp("trim_space"),
        null_if=StringListProp("null_if", parens=True),
        file_extension=StringProp("file_extension"),
        enable_octal=BoolProp("enable_octal"),
        allow_duplicate=BoolProp("allow_duplicate"),
        strip_outer_array=BoolProp("strip_outer_array"),
        strip_null_values=BoolProp("strip_null_values"),
        replace_invalid_characters=BoolProp("replace_invalid_characters"),
        ignore_utf8_errors=BoolProp("ignore_utf8_errors"),
        skip_byte_order_mark=BoolProp("skip_byte_order_mark"),
        comment=StringProp("comment"),
    )
    scope = SchemaScope()
    spec = _JSONFileFormat

    def __init__(
        self,
        name: str,
        owner: str = "SYSADMIN",
        compression: Compression = Compression.AUTO,
        date_format: str = "AUTO",
        time_format: str = "AUTO",
        timestamp_format: str = "AUTO",
        binary_format: BinaryFormat = BinaryFormat.HEX,
        trim_space: bool = False,
        null_if: list[str] = None,
        file_extension: str = "json",
        enable_octal: bool = False,
        allow_duplicate: bool = False,
        strip_outer_array: bool = False,
        strip_null_values: bool = False,
        replace_invalid_characters: bool = False,
        ignore_utf8_errors: bool = False,
        skip_byte_order_mark: bool = True,
        comment: str = None,
        **kwargs,
    ):
        kwargs.pop("type", None)
        super().__init__(name, **kwargs)
        self._data: _JSONFileFormat = _JSONFileFormat(
            name=self._name,
            owner=owner,
            compression=compression,
            date_format=date_format,
            time_format=time_format,
            timestamp_format=timestamp_format,
            binary_format=binary_format,
            trim_space=trim_space,
            null_if=null_if,
            file_extension=file_extension,
            enable_octal=enable_octal,
            allow_duplicate=allow_duplicate,
            strip_outer_array=strip_outer_array,
            strip_null_values=strip_null_values,
            replace_invalid_characters=replace_invalid_characters,
            ignore_utf8_errors=ignore_utf8_errors,
            skip_byte_order_mark=skip_byte_order_mark,
            comment=comment,
        )


@dataclass(unsafe_hash=True)
class _ParquetFileFormat(ResourceSpec):
    name: ResourceName
    owner: RoleRef = "SYSADMIN"
    type: FileType = FileType.PARQUET
    compression: Compression = Compression.AUTO
    binary_as_text: bool = True
    trim_space: bool = False
    replace_invalid_characters: bool = False
    null_if: list[str] = None
    comment: str = None

    def __post_init__(self):
        super().__post_init__()
        if self.null_if is None:
            self.null_if = []


class ParquetFileFormat(NamedResource, Resource):
    """
    Description:
        A Parquet file format in Snowflake.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-file-format

    Fields:
        name (string, required): The name of the file format.
        owner (string or Role): The owner role of the file format. Defaults to "SYSADMIN".
        compression (string): The compression type for the file format. Defaults to "AUTO".
        binary_as_text (bool): Whether to interpret binary data as text. Defaults to True.
        trim_space (bool): Whether to trim spaces. Defaults to False.
        replace_invalid_characters (bool): Whether to replace invalid characters. Defaults to False.
        null_if (list): A list of strings to be interpreted as NULL.
        comment (string): A comment for the file format.

    Python:

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

    Yaml:

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
    """

    resource_type = ResourceType.FILE_FORMAT
    props = Props(
        type=EnumProp("type", [FileType.PARQUET]),
        compression=EnumProp("compression", Compression),
        binary_as_text=BoolProp("binary_as_text"),
        trim_space=BoolProp("trim_space"),
        replace_invalid_characters=BoolProp("replace_invalid_characters"),
        null_if=StringListProp("null_if", parens=True),
        comment=StringProp("comment"),
    )
    scope = SchemaScope()
    spec = _ParquetFileFormat

    def __init__(
        self,
        name: str,
        owner: str = "SYSADMIN",
        compression: str = "AUTO",
        binary_as_text: bool = True,
        trim_space: bool = False,
        replace_invalid_characters: bool = False,
        null_if: list[str] = None,
        comment: str = None,
        **kwargs,
    ):
        kwargs.pop("type", None)
        super().__init__(name, **kwargs)
        self._data: _ParquetFileFormat = _ParquetFileFormat(
            name=self._name,
            owner=owner,
            compression=compression,
            binary_as_text=binary_as_text,
            trim_space=trim_space,
            replace_invalid_characters=replace_invalid_characters,
            null_if=null_if,
            comment=comment,
        )


FileFormatMap = {
    FileType.CSV: CSVFileFormat,
    FileType.JSON: JSONFileFormat,
    # FileType.AVRO: AvroFileFormat,
    # FileType.ORC: OrcFileFormat,
    FileType.PARQUET: ParquetFileFormat,
    # FileType.XML: XMLFileFormat,
}


def _resolver(data: dict):
    return FileFormatMap[FileType(data["type"])]


Resource.__resolvers__[ResourceType.FILE_FORMAT] = _resolver
