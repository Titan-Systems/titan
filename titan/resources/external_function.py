from typing import Dict

from . import Resource
from .base import AccountScoped
from ..props import Props, IntProp, StringProp, BoolProp, ResourceListProp, EnumProp
from .column import ColumnType, Column


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
        columns=ResourceListProp(Column),
        returns=EnumProp("returns", ColumnType),
        # not_null=BoolProp("not_null"),
        # on_null_input=StringProp("on_null_input"),
        # volatile=BoolProp("volatile"),
        # immutable=BoolProp("immutable"),
        comment=StringProp("comment"),
        api_integration=StringProp("api_integration"),
        # headers=DictProp("headers"),
        max_batch_rows=IntProp("max_batch_rows"),
        compression=StringProp("compression"),
        request_translator=StringProp("request_translator"),
        response_translator=StringProp("response_translator"),
        url_of_proxy_and_resource=StringProp("url_of_proxy_and_resource"),
        as_=StringProp("as"),
    )

    name: str
    secure: bool = False
    columns: list = []
    returns: ColumnType
    not_null: bool = False
    on_null_input: str = None
    volatile: bool = False
    immutable: bool = False
    comment: str = None
    api_integration: str
    headers: Dict[str, str] = {}
    max_batch_rows: int = None
    compression: str = None
    request_translator: str = None
    response_translator: str = None
    as_: str
