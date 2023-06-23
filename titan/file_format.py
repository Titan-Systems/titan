import re

from typing import Union, Optional, List, Type

from .resource import SchemaLevelResource
from .props import Identifier, BoolProp, EnumProp, ParsableEnum, StringProp, IntProp, StringListProp


class FileType(ParsableEnum):
    CSV = "CSV"
    JSON = "JSON"
    AVRO = "AVRO"
    ORC = "ORC"
    PARQUET = "PARQUET"
    XML = "XML"


class Compression(ParsableEnum):
    AUTO = "AUTO"
    GZIP = "GZIP"
    BZ2 = "BZ2"
    BROTLI = "BROTLI"
    ZSTD = "ZSTD"
    DEFLATE = "DEFLATE"
    RAW_DEFLATE = "RAW_DEFLATE"
    LZO = "LZO"
    SNAPPY = "SNAPPY"
    NONE = "NONE"


class BinaryFormat(ParsableEnum):
    HEX = "HEX"
    BASE64 = "BASE64"
    UTF8 = "UTF8"


class FileFormat(SchemaLevelResource):
    """
    CREATE [ OR REPLACE ] [ { TEMP | TEMPORARY | VOLATILE } ] FILE FORMAT [ IF NOT EXISTS ] <name>
      TYPE = { CSV | JSON | AVRO | ORC | PARQUET | XML } [ formatTypeOptions ]
      [ COMMENT = '<string_literal>' ]

    formatTypeOptions ::=
    -- If TYPE = CSV
         COMPRESSION = AUTO | GZIP | BZ2 | BROTLI | ZSTD | DEFLATE | RAW_DEFLATE | NONE
         RECORD_DELIMITER = '<character>' | NONE
         FIELD_DELIMITER = '<character>' | NONE
         FILE_EXTENSION = '<string>'
         PARSE_HEADER = TRUE | FALSE
         SKIP_HEADER = <integer>
         SKIP_BLANK_LINES = TRUE | FALSE
         DATE_FORMAT = '<string>' | AUTO
         TIME_FORMAT = '<string>' | AUTO
         TIMESTAMP_FORMAT = '<string>' | AUTO
         BINARY_FORMAT = HEX | BASE64 | UTF8
         ESCAPE = '<character>' | NONE
         ESCAPE_UNENCLOSED_FIELD = '<character>' | NONE
         TRIM_SPACE = TRUE | FALSE
         FIELD_OPTIONALLY_ENCLOSED_BY = '<character>' | NONE
         NULL_IF = ( '<string>' [ , '<string>' ... ] )
         ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE | FALSE
         REPLACE_INVALID_CHARACTERS = TRUE | FALSE
         EMPTY_FIELD_AS_NULL = TRUE | FALSE
         SKIP_BYTE_ORDER_MARK = TRUE | FALSE
         ENCODING = '<string>' | UTF8
    -- If TYPE = JSON
         COMPRESSION = AUTO | GZIP | BZ2 | BROTLI | ZSTD | DEFLATE | RAW_DEFLATE | NONE
         DATE_FORMAT = '<string>' | AUTO
         TIME_FORMAT = '<string>' | AUTO
         TIMESTAMP_FORMAT = '<string>' | AUTO
         BINARY_FORMAT = HEX | BASE64 | UTF8
         TRIM_SPACE = TRUE | FALSE
         NULL_IF = ( '<string>' [ , '<string>' ... ] )
         FILE_EXTENSION = '<string>'
         ENABLE_OCTAL = TRUE | FALSE
         ALLOW_DUPLICATE = TRUE | FALSE
         STRIP_OUTER_ARRAY = TRUE | FALSE
         STRIP_NULL_VALUES = TRUE | FALSE
         REPLACE_INVALID_CHARACTERS = TRUE | FALSE
         IGNORE_UTF8_ERRORS = TRUE | FALSE
         SKIP_BYTE_ORDER_MARK = TRUE | FALSE
    -- If TYPE = AVRO
         COMPRESSION = AUTO | GZIP | BROTLI | ZSTD | DEFLATE | RAW_DEFLATE | NONE
         TRIM_SPACE = TRUE | FALSE
         REPLACE_INVALID_CHARACTERS = TRUE | FALSE
         NULL_IF = ( '<string>' [ , '<string>' ... ] )
    -- If TYPE = ORC
         TRIM_SPACE = TRUE | FALSE
         REPLACE_INVALID_CHARACTERS = TRUE | FALSE
         NULL_IF = ( '<string>' [ , '<string>' ... ] )
    -- If TYPE = PARQUET
         COMPRESSION = AUTO | LZO | SNAPPY | NONE
         SNAPPY_COMPRESSION = TRUE | FALSE
         BINARY_AS_TEXT = TRUE | FALSE
         TRIM_SPACE = TRUE | FALSE
         REPLACE_INVALID_CHARACTERS = TRUE | FALSE
         NULL_IF = ( '<string>' [ , '<string>' ... ] )
    -- If TYPE = XML
         COMPRESSION = AUTO | GZIP | BZ2 | BROTLI | ZSTD | DEFLATE | RAW_DEFLATE | NONE
         IGNORE_UTF8_ERRORS = TRUE | FALSE
         PRESERVE_SPACE = TRUE | FALSE
         STRIP_OUTER_ELEMENT = TRUE | FALSE
         DISABLE_SNOWFLAKE_DATA = TRUE | FALSE
         DISABLE_AUTO_CONVERT = TRUE | FALSE
         REPLACE_INVALID_CHARACTERS = TRUE | FALSE
         SKIP_BYTE_ORDER_MARK = TRUE | FALSE
    """

    create_statement = re.compile(
        rf"""
            CREATE\s+
            (?:OR\s+REPLACE\s+)?
            (?:(?:TEMP|TEMPORARY|VOLATILE)\s+)?
            FILE\s+FORMAT\s+
            (?:IF\s+NOT\s+EXISTS\s+)?
            ({Identifier.pattern})
        """,
        re.IGNORECASE | re.VERBOSE,
    )

    ownable = True

    def __init__(self, file_type: FileType, **kwargs):
        if type(self) == FileFormat:
            raise TypeError(f"only children of '{type(self).__name__}' may be instantiated")
        self.file_type = FileType.parse(file_type)
        super().__init__(**kwargs)

    @classmethod
    def from_sql(cls, sql: str):
        match = re.search(cls.create_statement, sql)

        if match is None:
            raise Exception
        name = match.group(1)
        file_type = EnumProp("TYPE", FileType).search(sql[match.end() :])
        file_format_class: Optional[Type[FileFormat]] = None
        if file_type is None:
            raise Exception("No type specified for CREATE FILE FORMAT statement")
        elif file_type == "CSV":
            file_format_class = CSVFileFormat
        elif file_type == "JSON":
            file_format_class = JSONFileFormat
        elif file_type == "AVRO":
            file_format_class = AvroFileFormat
        elif file_type == "ORC":
            file_format_class = OrcFileFormat
        elif file_type == "PARQUET":
            file_format_class = ParquetFileFormat
        elif file_type == "XML":
            file_format_class = XMLFileFormat
        else:
            raise Exception(f"Unknown file format type {file_type}")

        props = file_format_class.parse_props(sql[match.end() :])
        del props["type"]
        return file_format_class(name=name, **props)

    @property
    def sql(self):
        return f"""
            CREATE FILE FORMAT {self.fully_qualified_name}
        """.strip()


class CSVFileFormat(FileFormat):
    props = {
        "TYPE": EnumProp("TYPE", [FileType.CSV]),
        "COMPRESSION": EnumProp("COMPRESSION", Compression),
        "RECORD_DELIMITER": StringProp("RECORD_DELIMITER", alt_tokens=["NONE"]),
        "FIELD_DELIMITER": StringProp("FIELD_DELIMITER", alt_tokens=["NONE"]),
        "FILE_EXTENSION": StringProp("FILE_EXTENSION"),
        "PARSE_HEADER": BoolProp("PARSE_HEADER"),
        "SKIP_HEADER": IntProp("SKIP_HEADER"),
        "SKIP_BLANK_LINES": BoolProp("SKIP_BLANK_LINES"),
        "DATE_FORMAT": StringProp("DATE_FORMAT", alt_tokens=["AUTO"]),
        "TIME_FORMAT": StringProp("TIME_FORMAT", alt_tokens=["AUTO"]),
        "TIMESTAMP_FORMAT": StringProp("TIMESTAMP_FORMAT", alt_tokens=["AUTO"]),
        "BINARY_FORMAT": EnumProp("BINARY_FORMAT", BinaryFormat),
        "ESCAPE": StringProp("ESCAPE", alt_tokens=["NONE"]),
        "ESCAPE_UNENCLOSED_FIELD": StringProp("ESCAPE_UNENCLOSED_FIELD", alt_tokens=["NONE"]),
        "TRIM_SPACE": BoolProp("TRIM_SPACE"),
        "FIELD_OPTIONALLY_ENCLOSED_BY": StringProp("FIELD_OPTIONALLY_ENCLOSED_BY", alt_tokens=["NONE"]),
        "NULL_IF": StringListProp("NULL_IF"),
        "ERROR_ON_COLUMN_COUNT_MISMATCH": BoolProp("ERROR_ON_COLUMN_COUNT_MISMATCH"),
        "REPLACE_INVALID_CHARACTERS": BoolProp("REPLACE_INVALID_CHARACTERS"),
        "EMPTY_FIELD_AS_NULL": BoolProp("EMPTY_FIELD_AS_NULL"),
        "SKIP_BYTE_ORDER_MARK": BoolProp("SKIP_BYTE_ORDER_MARK"),
        "ENCODING": StringProp("ENCODING", alt_tokens=["UTF8"]),
        "COMMENT": StringProp("COMMENT"),
    }

    def __init__(
        self,
        compression=None,
        record_delimiter=None,
        field_delimiter=None,
        file_extension=None,
        parse_header=None,
        skip_header=None,
        skip_blank_lines=None,
        date_format=None,
        time_format=None,
        timestamp_format=None,
        binary_format=None,
        escape=None,
        escape_unenclosed_field=None,
        trim_space=None,
        field_optionally_enclosed_by=None,
        null_if=None,
        error_on_column_count_mismatch=None,
        replace_invalid_characters=None,
        empty_field_as_null=None,
        skip_byte_order_mark=None,
        encoding=None,
        **kwargs,
    ):
        super().__init__(file_type=FileType.CSV, **kwargs)
        self.compression = Compression.parse(compression) if compression else None
        self.record_delimiter = record_delimiter
        self.field_delimiter = field_delimiter
        self.file_extension = file_extension
        self.parse_header = parse_header
        self.skip_header = skip_header
        self.skip_blank_lines = skip_blank_lines
        self.date_format = date_format
        self.time_format = time_format
        self.timestamp_format = timestamp_format
        self.binary_format = BinaryFormat.parse(binary_format) if binary_format else None
        self.escape = escape
        self.escape_unenclosed_field = escape_unenclosed_field
        self.trim_space = trim_space
        self.field_optionally_enclosed_by = field_optionally_enclosed_by
        self.null_if = null_if
        self.error_on_column_count_mismatch = error_on_column_count_mismatch
        self.replace_invalid_characters = replace_invalid_characters
        self.empty_field_as_null = empty_field_as_null
        self.skip_byte_order_mark = skip_byte_order_mark
        self.encoding = encoding


class JSONFileFormat(FileFormat):
    props = {
        "TYPE": EnumProp("TYPE", [FileType.JSON]),
        "COMPRESSION": EnumProp("COMPRESSION", Compression),
        "DATE_FORMAT": StringProp("DATE_FORMAT", alt_tokens=["AUTO"]),
        "TIME_FORMAT": StringProp("TIME_FORMAT", alt_tokens=["AUTO"]),
        "TIMESTAMP_FORMAT": StringProp("TIMESTAMP_FORMAT", alt_tokens=["AUTO"]),
        "BINARY_FORMAT": EnumProp("BINARY_FORMAT", BinaryFormat),
        "TRIM_SPACE": BoolProp("TRIM_SPACE"),
        "NULL_IF": StringListProp("NULL_IF"),
        "FILE_EXTENSION": StringProp("FILE_EXTENSION"),
        "ENABLE_OCTAL": BoolProp("ENABLE_OCTAL"),
        "ALLOW_DUPLICATE": BoolProp("ALLOW_DUPLICATE"),
        "STRIP_OUTER_ARRAY": BoolProp("STRIP_OUTER_ARRAY"),
        "STRIP_NULL_VALUES": BoolProp("STRIP_NULL_VALUES"),
        "REPLACE_INVALID_CHARACTERS": BoolProp("REPLACE_INVALID_CHARACTERS"),
        "IGNORE_UTF8_ERRORS": BoolProp("IGNORE_UTF8_ERRORS"),
        "SKIP_BYTE_ORDER_MARK": BoolProp("SKIP_BYTE_ORDER_MARK"),
        "COMMENT": StringProp("COMMENT"),
    }

    def __init__(
        self,
        compression=None,
        date_format=None,
        time_format=None,
        timestamp_format=None,
        binary_format=None,
        trim_space=None,
        null_if=None,
        file_extension=None,
        enable_octal=None,
        allow_duplicate=None,
        strip_outer_array=None,
        strip_null_values=None,
        replace_invalid_characters=None,
        ignore_utf8_errors=None,
        skip_byte_order_mark=None,
        **kwargs,
    ):
        super().__init__(file_type=FileType.JSON, **kwargs)
        self.compression = Compression.parse(compression) if compression else None
        self.date_format = date_format
        self.time_format = time_format
        self.timestamp_format = timestamp_format
        self.binary_format = BinaryFormat.parse(binary_format) if binary_format else None
        self.trim_space = trim_space
        self.null_if = null_if
        self.file_extension = file_extension
        self.enable_octal = enable_octal
        self.allow_duplicate = allow_duplicate
        self.strip_outer_array = strip_outer_array
        self.strip_null_values = strip_null_values
        self.replace_invalid_characters = replace_invalid_characters
        self.ignore_utf8_errors = ignore_utf8_errors
        self.skip_byte_order_mark = skip_byte_order_mark


class AvroFileFormat(FileFormat):
    props = {
        "TYPE": EnumProp("TYPE", [FileType.AVRO]),
        "COMPRESSION": EnumProp("COMPRESSION", Compression),
        "TRIM_SPACE": BoolProp("TRIM_SPACE"),
        "REPLACE_INVALID_CHARACTERS": BoolProp("REPLACE_INVALID_CHARACTERS"),
        "NULL_IF": StringListProp("NULL_IF"),
        "COMMENT": StringProp("COMMENT"),
    }

    def __init__(self, compression=None, trim_space=None, replace_invalid_characters=None, null_if=None, **kwargs):
        super().__init__(file_type=FileType.AVRO, **kwargs)
        self.compression = Compression.parse(compression) if compression else None
        self.trim_space = trim_space
        self.replace_invalid_characters = replace_invalid_characters
        self.null_if = null_if


class OrcFileFormat(FileFormat):
    props = {
        "TYPE": EnumProp("TYPE", FileType),
        "TRIM_SPACE": BoolProp("TRIM_SPACE"),
        "REPLACE_INVALID_CHARACTERS": BoolProp("REPLACE_INVALID_CHARACTERS"),
        "NULL_IF": StringListProp("NULL_IF"),
        "COMMENT": StringProp("COMMENT"),
    }

    def __init__(self, trim_space=None, replace_invalid_characters=None, null_if=None, **kwargs):
        super().__init__(file_type=FileType.ORC, **kwargs)
        self.trim_space = trim_space
        self.replace_invalid_characters = replace_invalid_characters
        self.null_if = null_if


class ParquetFileFormat(FileFormat):
    props = {
        "TYPE": EnumProp("TYPE", FileType),
        "COMPRESSION": EnumProp(
            "COMPRESSION", [Compression.AUTO, Compression.LZO, Compression.SNAPPY, Compression.NONE]
        ),
        "SNAPPY_COMPRESSION": BoolProp("SNAPPY_COMPRESSION"),
        "BINARY_AS_TEXT": BoolProp("BINARY_AS_TEXT"),
        "TRIM_SPACE": BoolProp("TRIM_SPACE"),
        "REPLACE_INVALID_CHARACTERS": BoolProp("REPLACE_INVALID_CHARACTERS"),
        "NULL_IF": StringListProp("NULL_IF"),
        "COMMENT": StringProp("COMMENT"),
    }

    def __init__(
        self,
        compression=None,
        snappy_compression=None,
        binary_as_text=None,
        trim_space=None,
        replace_invalid_characters=None,
        null_if=None,
        **kwargs,
    ):
        super().__init__(file_type=FileType.PARQUET, **kwargs)
        self.compression = Compression.parse(compression) if compression else None
        self.snappy_compression = snappy_compression
        self.binary_as_text = binary_as_text
        self.trim_space = trim_space
        self.replace_invalid_characters = replace_invalid_characters
        self.null_if = null_if


class XMLFileFormat(FileFormat):
    props = {
        "TYPE": EnumProp("TYPE", FileType),
        "COMPRESSION": EnumProp("COMPRESSION", Compression),
        "IGNORE_UTF8_ERRORS": BoolProp("IGNORE_UTF8_ERRORS"),
        "PRESERVE_SPACE": BoolProp("PRESERVE_SPACE"),
        "STRIP_OUTER_ELEMENT": BoolProp("STRIP_OUTER_ELEMENT"),
        "DISABLE_SNOWFLAKE_DATA": BoolProp("DISABLE_SNOWFLAKE_DATA"),
        "DISABLE_AUTO_CONVERT": BoolProp("DISABLE_AUTO_CONVERT"),
        "REPLACE_INVALID_CHARACTERS": BoolProp("REPLACE_INVALID_CHARACTERS"),
        "SKIP_BYTE_ORDER_MARK": BoolProp("SKIP_BYTE_ORDER_MARK"),
        "COMMENT": StringProp("COMMENT"),
    }

    def __init__(
        self,
        compression=None,
        ignore_utf8_errors=None,
        preserve_space=None,
        strip_outer_element=None,
        disable_snowflake_data=None,
        disable_auto_convert=None,
        replace_invalid_characters=None,
        skip_byte_order_mark=None,
        **kwargs,
    ):
        super().__init__(file_type=FileType.XML, **kwargs)
        self.compression = Compression.parse(compression) if compression else None
        self.ignore_utf8_errors = ignore_utf8_errors
        self.preserve_space = preserve_space
        self.strip_outer_element = strip_outer_element
        self.disable_snowflake_data = disable_snowflake_data
        self.disable_auto_convert = disable_auto_convert
        self.replace_invalid_characters = replace_invalid_characters
        self.skip_byte_order_mark = skip_byte_order_mark
