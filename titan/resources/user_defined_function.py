from typing import Dict

from .base import Resource, SchemaScoped
from ..builder import tidy_sql
from ..enums import DataType, NullHandling, ParseableEnum, Volatility
from ..props import (
    BoolProp,
    ColumnsProp,
    DictProp,
    EnumFlagProp,
    EnumProp,
    FlagProp,
    IdentifierProp,
    Props,
    StringProp,
)


class UDFLanguage(ParseableEnum):
    JAVA = "JAVA"
    JAVASCRIPT = "JAVASCRIPT"
    PYTHON = "PYTHON"
    SCALA = "SCALA"
    SQL = "SQL"


class UserDefinedFunction(Resource, SchemaScoped):
    resource_type = "FUNCTION"


class JavascriptUDF(UserDefinedFunction):
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
        args=ColumnsProp(),
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

    def create_sql(self, or_replace=False, if_not_exists=False):
        return tidy_sql(
            "CREATE",
            "OR REPLACE" if or_replace else "",
            "FUNCTION",
            "IF NOT EXISTS" if if_not_exists else "",
            self.fqn,
            self.props.render(self),
        )
