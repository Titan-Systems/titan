from .base import Resource, SchemaScoped, _fix_class_documentation
from ..enums import DataType, NullHandling, ParseableEnum, Volatility
from ..props import (
    ArgsProp,
    EnumFlagProp,
    EnumProp,
    FlagProp,
    Props,
    StringProp,
)


class UDFLanguage(ParseableEnum):
    JAVA = "JAVA"
    JAVASCRIPT = "JAVASCRIPT"
    PYTHON = "PYTHON"
    SCALA = "SCALA"
    SQL = "SQL"


class Function(SchemaScoped, Resource):
    resource_type = "FUNCTION"


@_fix_class_documentation
class JavascriptUDF(Function):
    """
    CREATE [ OR REPLACE ] [ { TEMP | TEMPORARY } ] [ SECURE ] FUNCTION <name> ( [ <arg_name> <arg_data_type> ] [ , ... ] )
    [ COPY GRANTS ]
    RETURNS { <result_data_type> | TABLE ( <col_name> <col_data_type> [ , ... ] ) }
    [ [ NOT ] NULL ]
    LANGUAGE JAVASCRIPT
    [ { CALLED ON NULL INPUT | { RETURNS NULL ON NULL INPUT | STRICT } } ]
    [ { VOLATILE | IMMUTABLE } ]
    [ COMMENT = '<string_literal>' ]
    AS '<function_definition>'
    """

    props = Props(
        secure=FlagProp("secure"),
        args=ArgsProp(),
        # Specifies to retain the access privileges from the original function when a new
        # function is created using CREATE OR REPLACE FUNCTION.
        # copy_grants=FlagProp("copy_grants"),
        returns=EnumProp("returns", DataType, eq=False),
        # not_null=BoolProp("not_null"),
        language=EnumProp("language", [UDFLanguage.JAVASCRIPT], eq=False),
        null_handling=EnumFlagProp(NullHandling),
        volatility=EnumFlagProp(Volatility),
        comment=StringProp("comment"),
        as_=StringProp("as", eq=False),
    )

    name: str
    owner: str = "SYSADMIN"
    secure: bool = False
    args: list = []
    returns: DataType
    language: UDFLanguage = UDFLanguage.JAVASCRIPT
    null_handling: NullHandling = None
    volatility: Volatility = None
    comment: str = None
    as_: str
