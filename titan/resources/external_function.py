from typing import Dict

from . import Resource
from .base import AccountScoped
from ..enums import DataType
from ..props import Props, IntProp, StringProp, BoolProp, EnumProp, FlagProp, DictProp, ColumnsProp, IdentifierProp


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
        secure=BoolProp("secure"),
        columns=ColumnsProp(),
        returns=EnumProp("returns", DataType, eq=False),
        # not_null=BoolProp("not_null"),
        called_on_null_input=FlagProp("called_on_null_input"),
        returns_null_on_null_input=FlagProp("returns_null_on_null_input"),
        strict=FlagProp("strict"),
        volatile=FlagProp("volatile"),
        immutable=FlagProp("immutable"),
        comment=StringProp("comment"),
        api_integration=StringProp("api_integration"),
        headers=DictProp("headers"),
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
    not_null: bool = False
    on_null_input: str = None
    called_on_null_input: bool = None
    returns_null_on_null_input: bool = None
    strict: bool = None
    volatile: bool = None
    immutable: bool = None
    comment: str = None
    api_integration: str
    headers: Dict[str, str] = {}
    max_batch_rows: int = None
    compression: str = None
    request_translator: str = None
    response_translator: str = None
    as_: str
