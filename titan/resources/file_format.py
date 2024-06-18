from dataclasses import dataclass

from .resource import Resource, ResourceSpec, ResourceNameTrait
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


class CSVFileFormat(ResourceNameTrait, Resource):
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


# @_fix_class_documentation
# class CSVFileFormat(SchemaScoped, Resource):
#     """
#     CREATE [ OR REPLACE ] [ { TEMP | TEMPORARY | VOLATILE } ] FILE FORMAT [ IF NOT EXISTS ] <name>
#       TYPE = CSV
#       [ formatTypeOptions ]
#       [ COMMENT = '<string_literal>' ]

#     formatTypeOptions ::=
#     -- If TYPE = CSV
#          COMPRESSION = AUTO | GZIP | BZ2 | BROTLI | ZSTD | DEFLATE | RAW_DEFLATE | NONE
#          RECORD_DELIMITER = '<character>' | NONE
#          FIELD_DELIMITER = '<character>' | NONE
#          FILE_EXTENSION = '<string>'
#          PARSE_HEADER = TRUE | FALSE
#          SKIP_HEADER = <integer>
#          SKIP_BLANK_LINES = TRUE | FALSE
#          DATE_FORMAT = '<string>' | AUTO
#          TIME_FORMAT = '<string>' | AUTO
#          TIMESTAMP_FORMAT = '<string>' | AUTO
#          BINARY_FORMAT = HEX | BASE64 | UTF8
#          ESCAPE = '<character>' | NONE
#          ESCAPE_UNENCLOSED_FIELD = '<character>' | NONE
#          TRIM_SPACE = TRUE | FALSE
#          FIELD_OPTIONALLY_ENCLOSED_BY = '<character>' | NONE
#          NULL_IF = ( '<string>' [ , '<string>' ... ] )
#          ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE | FALSE
#          REPLACE_INVALID_CHARACTERS = TRUE | FALSE
#          EMPTY_FIELD_AS_NULL = TRUE | FALSE
#          SKIP_BYTE_ORDER_MARK = TRUE | FALSE
#          ENCODING = '<string>' | UTF8
#     """

#     resource_type = "FILE FORMAT"
# props = Props(
#     type=EnumProp("type", [FileType.CSV]),
#     compression=EnumProp("compression", Compression),
#     record_delimiter=StringProp("record_delimiter", alt_tokens=["NONE"]),
#     field_delimiter=StringProp("field_delimiter", alt_tokens=["NONE"]),
#     file_extension=StringProp("file_extension"),
#     parse_header=BoolProp("parse_header"),
#     skip_header=IntProp("skip_header"),
#     skip_blank_lines=BoolProp("skip_blank_lines"),
#     date_format=StringProp("date_format", alt_tokens=["AUTO"]),
#     time_format=StringProp("time_format", alt_tokens=["AUTO"]),
#     timestamp_format=StringProp("timestamp_format", alt_tokens=["AUTO"]),
#     binary_format=EnumProp("binary_format", BinaryFormat),
#     escape=StringProp("escape", alt_tokens=["NONE"]),
#     escape_unenclosed_field=StringProp("escape_unenclosed_field", alt_tokens=["NONE"]),
#     trim_space=BoolProp("trim_space"),
#     field_optionally_enclosed_by=StringProp("field_optionally_enclosed_by", alt_tokens=["NONE"]),
#     null_if=StringListProp("null_if", parens=True),
#     error_on_column_count_mismatch=BoolProp("error_on_column_count_mismatch"),
#     replace_invalid_characters=BoolProp("replace_invalid_characters"),
#     empty_field_as_null=BoolProp("empty_field_as_null"),
#     skip_byte_order_mark=BoolProp("skip_byte_order_mark"),
#     encoding=StringProp("encoding", alt_tokens=["UTF8"]),
#     comment=StringProp("comment"),
# )

#     name: str
#     owner: str = "SYSADMIN"
#     type: FileType = FileType.CSV
#     compression: Compression = None
#     record_delimiter: str = None
#     field_delimiter: str = None
#     file_extension: str = None
#     parse_header: bool = None
#     skip_header: int = None
#     skip_blank_lines: bool = None
#     date_format: str = None
#     time_format: str = None
#     timestamp_format: str = None
#     binary_format: BinaryFormat = None
#     escape: str = None
#     escape_unenclosed_field: str = None
#     trim_space: bool = None
#     field_optionally_enclosed_by: str = None
#     null_if: List[str] = []
#     error_on_column_count_mismatch: bool = None
#     replace_invalid_characters: bool = None
#     empty_field_as_null: bool = None
#     skip_byte_order_mark: bool = None
#     encoding: str = None
#     comment: str = None


# @_fix_class_documentation
# class JSONFileFormat(SchemaScoped, Resource):
#     """
#     CREATE [ OR REPLACE ] [ { TEMP | TEMPORARY | VOLATILE } ] FILE FORMAT [ IF NOT EXISTS ] <name>
#       TYPE = JSON
#       [ formatTypeOptions ]
#       [ COMMENT = '<string_literal>' ]

#     formatTypeOptions ::=
#     -- If TYPE = JSON
#          COMPRESSION = AUTO | GZIP | BZ2 | BROTLI | ZSTD | DEFLATE | RAW_DEFLATE | NONE
#          DATE_FORMAT = '<string>' | AUTO
#          TIME_FORMAT = '<string>' | AUTO
#          TIMESTAMP_FORMAT = '<string>' | AUTO
#          BINARY_FORMAT = HEX | BASE64 | UTF8
#          TRIM_SPACE = TRUE | FALSE
#          NULL_IF = ( '<string>' [ , '<string>' ... ] )
#          FILE_EXTENSION = '<string>'
#          ENABLE_OCTAL = TRUE | FALSE
#          ALLOW_DUPLICATE = TRUE | FALSE
#          STRIP_OUTER_ARRAY = TRUE | FALSE
#          STRIP_NULL_VALUES = TRUE | FALSE
#          REPLACE_INVALID_CHARACTERS = TRUE | FALSE
#          IGNORE_UTF8_ERRORS = TRUE | FALSE
#          SKIP_BYTE_ORDER_MARK = TRUE | FALSE
#     """

#     resource_type = "FILE FORMAT"
#     props = Props(
#         type=EnumProp("type", [FileType.JSON]),
#         compression=EnumProp("compression", Compression),
#         date_format=StringProp("date_format", alt_tokens=["AUTO"]),
#         time_format=StringProp("time_format", alt_tokens=["AUTO"]),
#         timestamp_format=StringProp("timestamp_format", alt_tokens=["AUTO"]),
#         binary_format=EnumProp("binary_format", BinaryFormat),
#         trim_space=BoolProp("trim_space"),
#         null_if=StringListProp("null_if", parens=True),
#         file_extension=StringProp("file_extension"),
#         enable_octal=BoolProp("enable_octal"),
#         allow_duplicate=BoolProp("allow_duplicate"),
#         strip_outer_array=BoolProp("strip_outer_array"),
#         strip_null_values=BoolProp("strip_null_values"),
#         replace_invalid_characters=BoolProp("replace_invalid_characters"),
#         ignore_utf8_errors=BoolProp("ignore_utf8_errors"),
#         skip_byte_order_mark=BoolProp("skip_byte_order_mark"),
#         comment=StringProp("comment"),
#     )

#     name: str
#     owner: str = "SYSADMIN"
#     type: FileType = FileType.JSON
#     compression: Compression = None
#     date_format: str = None
#     time_format: str = None
#     timestamp_format: str = None
#     binary_format: BinaryFormat = None
#     trim_space: bool = None
#     null_if: List[str] = []
#     file_extension: str = None
#     enable_octal: bool = None
#     allow_duplicate: bool = None
#     strip_outer_array: bool = None
#     strip_null_values: bool = None
#     replace_invalid_characters: bool = None
#     ignore_utf8_errors: bool = None
#     skip_byte_order_mark: bool = None
#     comment: str = None


# @_fix_class_documentation
# class AvroFileFormat(SchemaScoped, Resource):
#     """
#     CREATE [ OR REPLACE ] [ { TEMP | TEMPORARY | VOLATILE } ] FILE FORMAT [ IF NOT EXISTS ] <name>
#       TYPE = AVRO
#       [ formatTypeOptions ]
#       [ COMMENT = '<string_literal>' ]

#     formatTypeOptions ::=
#     -- If TYPE = AVRO
#          COMPRESSION = AUTO | GZIP | BROTLI | ZSTD | DEFLATE | RAW_DEFLATE | NONE
#          TRIM_SPACE = TRUE | FALSE
#          REPLACE_INVALID_CHARACTERS = TRUE | FALSE
#          NULL_IF = ( '<string>' [ , '<string>' ... ] )
#     """

#     resource_type = "FILE FORMAT"
#     props = Props(
#         type=EnumProp("type", [FileType.AVRO]),
#         compression=EnumProp("compression", Compression),
#         trim_space=BoolProp("trim_space"),
#         replace_invalid_characters=BoolProp("replace_invalid_characters"),
#         null_if=StringListProp("null_if", parens=True),
#         comment=StringProp("comment"),
#     )

#     name: str
#     owner: str = "SYSADMIN"
#     type: FileType = FileType.AVRO
#     compression: Compression = None
#     trim_space: bool = None
#     replace_invalid_characters: bool = None
#     null_if: List[str] = []
#     comment: str = None


# @_fix_class_documentation
# class OrcFileFormat(SchemaScoped, Resource):
#     """
#     CREATE [ OR REPLACE ] [ { TEMP | TEMPORARY | VOLATILE } ] FILE FORMAT [ IF NOT EXISTS ] <name>
#       TYPE = ORC
#       [ formatTypeOptions ]
#       [ COMMENT = '<string_literal>' ]

#     formatTypeOptions ::=
#     -- If TYPE = ORC
#          TRIM_SPACE = TRUE | FALSE
#          REPLACE_INVALID_CHARACTERS = TRUE | FALSE
#          NULL_IF = ( '<string>' [ , '<string>' ... ] )
#     """

#     resource_type = "FILE FORMAT"
#     props = Props(
#         type=EnumProp("type", [FileType.ORC]),
#         trim_space=BoolProp("trim_space"),
#         replace_invalid_characters=BoolProp("replace_invalid_characters"),
#         null_if=StringListProp("null_if", parens=True),
#         comment=StringProp("comment"),
#     )

#     name: str
#     owner: str = "SYSADMIN"
#     type: FileType = FileType.ORC
#     trim_space: bool = None
#     replace_invalid_characters: bool = None
#     null_if: List[str] = []
#     comment: str = None


# @_fix_class_documentation
# class ParquetFileFormat(SchemaScoped, Resource):
#     """
#     CREATE [ OR REPLACE ] [ { TEMP | TEMPORARY | VOLATILE } ] FILE FORMAT [ IF NOT EXISTS ] <name>
#       TYPE = PARQUET
#       [ formatTypeOptions ]
#       [ COMMENT = '<string_literal>' ]

#     formatTypeOptions ::=
#     -- If TYPE = PARQUET
#          COMPRESSION = AUTO | LZO | SNAPPY | NONE
#          SNAPPY_COMPRESSION = TRUE | FALSE
#          BINARY_AS_TEXT = TRUE | FALSE
#          TRIM_SPACE = TRUE | FALSE
#          REPLACE_INVALID_CHARACTERS = TRUE | FALSE
#          NULL_IF = ( '<string>' [ , '<string>' ... ] )
#     """

#     resource_type = "FILE FORMAT"
#     props = Props(
#         type=EnumProp("type", [FileType.PARQUET]),
#         compression=EnumProp("compression", [Compression.AUTO, Compression.LZO, Compression.SNAPPY, Compression.NONE]),
#         snappy_compression=BoolProp("snappy_compression"),
#         binary_as_text=BoolProp("binary_as_text"),
#         trim_space=BoolProp("trim_space"),
#         replace_invalid_characters=BoolProp("replace_invalid_characters"),
#         null_if=StringListProp("null_if", parens=True),
#         comment=StringProp("comment"),
#     )

#     name: str
#     owner: str = "SYSADMIN"
#     type: FileType = FileType.PARQUET
#     compression: Compression = None
#     snappy_compression: bool = None
#     binary_as_text: bool = None
#     trim_space: bool = None
#     replace_invalid_characters: bool = None
#     null_if: List[str] = []
#     comment: str = None


# @_fix_class_documentation
# class XMLFileFormat(SchemaScoped, Resource):
#     """
#     CREATE [ OR REPLACE ] [ { TEMP | TEMPORARY | VOLATILE } ] FILE FORMAT [ IF NOT EXISTS ] <name>
#       TYPE = XML
#       [ formatTypeOptions ]
#       [ COMMENT = '<string_literal>' ]

#     formatTypeOptions ::=
#     -- If TYPE = XML
#          COMPRESSION = AUTO | GZIP | BZ2 | BROTLI | ZSTD | DEFLATE | RAW_DEFLATE | NONE
#          IGNORE_UTF8_ERRORS = TRUE | FALSE
#          PRESERVE_SPACE = TRUE | FALSE
#          STRIP_OUTER_ELEMENT = TRUE | FALSE
#          DISABLE_SNOWFLAKE_DATA = TRUE | FALSE
#          DISABLE_AUTO_CONVERT = TRUE | FALSE
#          REPLACE_INVALID_CHARACTERS = TRUE | FALSE
#          SKIP_BYTE_ORDER_MARK = TRUE | FALSE
#     """

#     resource_type = "FILE FORMAT"
#     props = Props(
#         type=EnumProp("type", [FileType.XML]),
#         compression=EnumProp("compression", Compression),
#         ignore_utf8_errors=BoolProp("ignore_utf8_errors"),
#         preserve_space=BoolProp("preserve_space"),
#         strip_outer_element=BoolProp("strip_outer_element"),
#         disable_snowflake_data=BoolProp("disable_snowflake_data"),
#         disable_auto_convert=BoolProp("disable_auto_convert"),
#         replace_invalid_characters=BoolProp("replace_invalid_characters"),
#         skip_byte_order_mark=BoolProp("skip_byte_order_mark"),
#         comment=StringProp("comment"),
#     )

#     name: str
#     owner: str = "SYSADMIN"
#     type: FileType = FileType.XML
#     compression: Compression = None
#     ignore_utf8_errors: bool = None
#     preserve_space: bool = None
#     strip_outer_element: bool = None
#     disable_snowflake_data: bool = None
#     disable_auto_convert: bool = None
#     replace_invalid_characters: bool = None
#     skip_byte_order_mark: bool = None
#     comment: str = None


# FileTypeMap = {
#     FileType.CSV: CSVFileFormat,
#     FileType.JSON: JSONFileFormat,
#     FileType.AVRO: AvroFileFormat,
#     FileType.ORC: OrcFileFormat,
#     FileType.PARQUET: ParquetFileFormat,
#     FileType.XML: XMLFileFormat,
# }


# class FileFormatProp(Prop):
#     """
#     FILE_FORMAT = my_named_ff
#     FILE_FORMAT = (FORMAT_NAME = my_named_ff)
#     FILE_FORMAT = (TYPE = CSV ...)
#     """

#     def __init__(self, label):
#         value_expr = (
#             _in_parens(IdentifierProp("format_name").parser("prop_value"))
#             | pp.original_text_for(pp.nested_expr())("prop_value")
#             | FullyQualifiedIdentifier("prop_value")
#             | StringLiteral("prop_value")
#         )

#         super().__init__(label, value_expr, eq=True)

#     def typecheck(self, prop_value):
#         # Prop is an identifier name
#         if isinstance(prop_value, list):
#             return ".".join(prop_value)

#         prop_value = prop_value.strip("()")
#         file_type = EnumProp("type", FileType).parse(prop_value)

#         if file_type is None:
#             raise ValueError(f"Invalid inline file format: {prop_value}")

#         # Prop is an inline file format
#         file_type_cls = FileTypeMap[file_type]
#         return _parse_props(file_type_cls.props, prop_value)

#     def render(self, value):
#         if value is None:
#             return ""
#         eq = " = " if self.eq else " "
#         if isinstance(value, dict):
#             value_str = ", ".join(f"{k} = {v}" for k, v in value.items())
#             value_str = f"({value_str})"
#         else:
#             value_str = str(value)
#         return f"{self.label}{eq}{value_str}"


# class FileFormat(Resource, ABC):
#     def __new__(
#         cls, type: Union[str, FileType], **kwargs
#     ) -> Union[CSVFileFormat, JSONFileFormat, AvroFileFormat, OrcFileFormat, ParquetFileFormat, XMLFileFormat]:
#         file_type = FileType.parse(type)
#         file_type_cls = FileTypeMap[file_type]
#         return file_type_cls(type=file_type, **kwargs)

#     @classmethod
#     def from_sql(cls, sql):
#         resource_cls = Resource.classes[_resolve_resource_class(sql)]
#         return resource_cls.from_sql(sql)

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
