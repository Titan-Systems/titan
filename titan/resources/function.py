from abc import ABC
from typing import Union

from .base import Resource, SchemaScoped, _fix_class_documentation
from ..enums import DataType, Language, NullHandling, Volatility
from ..parse import _resolve_resource_class
from ..props import (
    ArgsProp,
    EnumFlagProp,
    EnumProp,
    FlagProp,
    Props,
    StringProp,
)


@_fix_class_documentation
class JavascriptUDF(SchemaScoped, Resource):
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

    resource_type = "FUNCTION"

    props = Props(
        secure=FlagProp("secure"),
        args=ArgsProp(),
        # Specifies to retain the access privileges from the original function when a new
        # function is created using CREATE OR REPLACE FUNCTION.
        # copy_grants=FlagProp("copy_grants"),
        returns=EnumProp("returns", DataType, eq=False),
        # not_null=BoolProp("not_null"),
        language=EnumProp("language", [Language.JAVASCRIPT], eq=False),
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
    language: Language = Language.JAVASCRIPT
    null_handling: NullHandling = None
    volatility: Volatility = None
    comment: str = None
    as_: str


FunctionMap = {
    Language.JAVASCRIPT: JavascriptUDF,
}


class Function(Resource, ABC):
    def __new__(cls, type: Union[str, Language], **kwargs) -> JavascriptUDF:
        language = Language.parse(type)
        sproc_cls = FunctionMap[language]
        return sproc_cls(language=language, **kwargs)

    @classmethod
    def from_sql(cls, sql):
        resource_cls = Resource.classes[_resolve_resource_class(sql)]
        return resource_cls.from_sql(sql)
