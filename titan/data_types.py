from typing import Union

from .enums import DataType


def convert_to_canonical_data_type(data_type: Union[str, DataType]) -> str:
    if isinstance(data_type, DataType):
        data_type = str(data_type)
    data_type = data_type.upper()
    if data_type in (
        "NUMBER",
        "DECIMAL",
        "DEC",
        "NUMERIC",
        "INT",
        "INTEGER",
        "BIGINT",
        "SMALLINT",
        "TINYINT",
        "BYTEINT",
    ):
        return "NUMBER(38,0)"
    if data_type in (
        "FLOAT",
        "FLOAT4",
        "FLOAT8",
        "REAL",
        "DOUBLE",
        "DOUBLE PRECISION",
        "REAL",
    ):
        return "FLOAT"
    if data_type in ("BOOLEAN", "BOOL"):
        return "BOOLEAN"
    if data_type in (
        "VARCHAR",
        "STRING",
        "TEXT",
        "NVARCHAR",
        "NVARCHAR2",
        "CHAR VARYING",
        "NCHAR VARYING",
    ):
        return "VARCHAR(16777216)"
    if data_type in ("CHAR", "CHARACTER", "NCHAR"):
        return "VARCHAR(1)"
    if data_type in ("BINARY", "VARBINARY"):
        return "BINARY(8388608)"
    if data_type in ("DATETIME", "TIMESTAMP", "TIMESTAMP_NTZ"):
        return "TIMESTAMP_NTZ(9)"
    if data_type in ("TIMESTAMP_LTZ", "TIMESTAMP_TZ"):
        return "TIMESTAMP_LTZ(9)"
    if data_type in ("TIME"):
        return "TIME(9)"
    return data_type
