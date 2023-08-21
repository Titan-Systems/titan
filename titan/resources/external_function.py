from typing import Dict

from .base import Resource, AccountScoped
from ..enums import DataType, NullHandling, Volatility
from ..props import (
    BoolProp,
    ColumnsProp,
    DictProp,
    EnumProp,
    EnumFlagProp,
    FlagProp,
    IdentifierProp,
    IntProp,
    Props,
    StringProp,
)


class ExternalFunction(Resource, AccountScoped):
    """
    CREATE [ OR REPLACE ] [ SECURE ] EXTERNAL FUNCTION <name> ( [ <arg_name> <arg_data_type> ] [ , ... ] )
      RETURNS <result_data_type>
      [ [ NOT ] NULL ]
      [ { CALLED ON NULL INPUT | { RETURNS NULL ON NULL INPUT | STRICT } } ]
      [ VOLATILE | IMMUTABLE ]
      [ COMMENT = '<string_literal>' ]
      API_INTEGRATION = <api_integration_name>
      [ HEADERS = ( '<header_1>' = '<value_1>' [ , '<header_2>' = '<value_2>' ... ] ) ]
      [ MAX_BATCH_ROWS = <integer> ]
      [ COMPRESSION = <compression_type> ]
      [ REQUEST_TRANSLATOR = <request_translator_udf_name> ]
      [ RESPONSE_TRANSLATOR = <response_translator_udf_name> ]
      AS <url_of_proxy_and_resource>;
    """

    resource_type = "EXTERNAL FUNCTION"
    props = Props(
        secure=FlagProp("secure"),
        columns=ColumnsProp(),
        returns=EnumProp("returns", DataType, eq=False),
        # not_null=BoolProp("not_null"),
        null_handling=EnumFlagProp(NullHandling),
        volatility=EnumFlagProp(Volatility),
        comment=StringProp("comment"),
        api_integration=StringProp("api_integration"),
        headers=DictProp("headers", parens=True),
        max_batch_rows=IntProp("max_batch_rows"),
        compression=StringProp("compression"),
        request_translator=IdentifierProp("request_translator"),
        response_translator=IdentifierProp("response_translator"),
        as_=StringProp("as", eq=False),
    )

    name: str
    secure: bool = False
    columns: list = []
    returns: DataType
    # not_null: bool = False
    null_handling: NullHandling = None
    volatility: Volatility = None
    comment: str = None
    api_integration: str
    headers: Dict[str, str] = {}
    max_batch_rows: int = None
    compression: str = None
    request_translator: str = None
    response_translator: str = None
    as_: str
