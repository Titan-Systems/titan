from typing import Optional, Union

from .enums import DataType

NUMBER_TYPES = ("NUMBER", "DECIMAL", "DEC", "NUMERIC", "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT")
FLOAT_TYPES = ("FLOAT", "FLOAT4", "FLOAT8", "REAL", "DOUBLE", "DOUBLE PRECISION", "REAL")
VARCHAR_TYPES = ("VARCHAR", "STRING", "TEXT", "NVARCHAR", "NVARCHAR2", "CHAR VARYING", "NCHAR VARYING")


def convert_to_canonical_data_type(data_type: Union[str, DataType, None]) -> Optional[str]:
    if data_type is None:
        return None
    if isinstance(data_type, DataType):
        data_type = str(data_type)
    data_type = data_type.upper()
    if data_type in NUMBER_TYPES:
        return "NUMBER(38,0)"
    if data_type in FLOAT_TYPES:
        return "FLOAT"
    if data_type in ("BOOLEAN", "BOOL"):
        return "BOOLEAN"
    if data_type in VARCHAR_TYPES:
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


def convert_to_simple_data_type(data_type: str) -> str:
    if data_type in NUMBER_TYPES:
        return "NUMBER"
    if data_type in FLOAT_TYPES:
        return "FLOAT"
    if data_type in ("BOOLEAN", "BOOL"):
        return "BOOLEAN"
    if data_type in VARCHAR_TYPES:
        return "VARCHAR"
    return data_type
