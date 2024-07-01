from dataclasses import dataclass

from .resource import Resource, ResourceSpec, NamedResource
from .role import Role
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
from ..scope import SchemaScope


@dataclass(unsafe_hash=True)
class _CSVFileFormat(ResourceSpec):
    name: ResourceName
    owner: Role = "SYSADMIN"
    type: FileType = FileType.CSV
    compression: Compression = None
    record_delimiter: str = "\n"
    field_delimiter: str = None
    file_extension: str = None
    parse_header: bool = False
    skip_header: int = None
    skip_blank_lines: bool = False
    date_format: str = "AUTO"
    time_format: str = "AUTO"
    timestamp_format: str = "AUTO"
    binary_format: BinaryFormat = BinaryFormat.HEX
    escape: str = None
    escape_unenclosed_field: str = "\\"
    trim_space: bool = False
    field_optionally_enclosed_by: str = None
    null_if: list[str] = None
    error_on_column_count_mismatch: bool = True
    replace_invalid_characters: bool = False
    empty_field_as_null: bool = None
    skip_byte_order_mark: bool = True
    encoding: str = "UTF8"
    comment: str = None


class CSVFileFormat(NamedResource, Resource):
    """
    Description:
        Defines the specifications for a CSV file format in Snowflake, including delimiters, encoding, and compression options.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-file-format

    Fields:
        type (FileType, required): The type of file format, which is CSV for this class. Defaults to CSV.
        compression (Compression): The compression type used for the file format.
        record_delimiter (string): Specifies the character that delimits records. Defaults to "\n".
        field_delimiter (string): Specifies the character that delimits fields.
        file_extension (string): The file extension used for files of this format.
        parse_header (bool): Whether to parse the first line of the file as a header. Defaults to False.
        skip_header (int): The number of header lines to skip before parsing.
        skip_blank_lines (bool): Whether to skip over blank lines. Defaults to False.
        date_format (string): The format used for date values. Defaults to "AUTO".
        time_format (string): The format used for time values. Defaults to "AUTO".
        timestamp_format (string): The format used for timestamp values. Defaults to "AUTO".
        binary_format (BinaryFormat): The format used for binary data. Defaults to HEX.
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
        compression: Compression = None,
        record_delimiter: str = "\n",
        field_delimiter: str = None,
        file_extension: str = None,
        parse_header: bool = False,
        skip_header: int = None,
        skip_blank_lines: bool = False,
        date_format: str = "AUTO",
        time_format: str = "AUTO",
        timestamp_format: str = "AUTO",
        binary_format: BinaryFormat = BinaryFormat.HEX,
        escape: str = None,
        escape_unenclosed_field: str = "\\",
        trim_space: bool = False,
        field_optionally_enclosed_by: str = None,
        null_if: list[str] = None,
        error_on_column_count_mismatch: bool = True,
        replace_invalid_characters: bool = False,
        empty_field_as_null: bool = None,
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


FileFormatMap = {
    FileType.CSV: CSVFileFormat,
    # FileType.JSON: JSONFileFormat,
    # FileType.AVRO: AvroFileFormat,
    # FileType.ORC: OrcFileFormat,
    # FileType.PARQUET: ParquetFileFormat,
    # FileType.XML: XMLFileFormat,
}


def _resolver(data: dict):
    return FileFormatMap[FileType(data["type"])]


Resource.__resolvers__[ResourceType.FILE_FORMAT] = _resolver
