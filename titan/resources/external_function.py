from dataclasses import dataclass

from .resource import Resource, ResourceSpec, Arg, NamedResource
from ..role_ref import RoleRef
from ..enums import DataType, NullHandling, Volatility, ResourceType
from ..scope import SchemaScope
from ..resource_name import ResourceName

from ..props import (
    ArgsProp,
    DictProp,
    EnumProp,
    EnumFlagProp,
    FlagProp,
    IdentifierProp,
    IntProp,
    Props,
    StringProp,
)


@dataclass(unsafe_hash=True)
class _ExternalFunction(ResourceSpec):
    name: ResourceName
    returns: DataType
    api_integration: str
    as_: str
    secure: bool = False
    args: list[Arg] = None
    not_null: bool = False
    null_handling: NullHandling = None
    volatility: Volatility = None
    comment: str = None
    headers: dict[str, str] = None
    max_batch_rows: int = None
    compression: str = None
    request_translator: str = None
    response_translator: str = None
    owner: RoleRef = "SYSADMIN"


class ExternalFunction(NamedResource, Resource):
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

    resource_type = ResourceType.EXTERNAL_FUNCTION
    props = Props(
        secure=FlagProp("secure"),
        args=ArgsProp(),
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
    scope = SchemaScope()
    spec = _ExternalFunction

    def __init__(
        self,
        name: str,
        returns: DataType,
        api_integration: str,
        as_: str,
        secure: bool = False,
        args: list = None,
        not_null: bool = False,
        null_handling: NullHandling = None,
        volatility: Volatility = None,
        comment: str = None,
        headers: dict[str, str] = None,
        max_batch_rows: int = None,
        compression: str = None,
        request_translator: str = None,
        response_translator: str = None,
        owner: str = "SYSADMIN",
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._data: _ExternalFunction = _ExternalFunction(
            name=self._name,
            returns=returns,
            api_integration=api_integration,
            as_=as_,
            secure=secure,
            args=args,
            not_null=not_null,
            null_handling=null_handling,
            volatility=volatility,
            comment=comment,
            headers=headers,
            max_batch_rows=max_batch_rows,
            compression=compression,
            request_translator=request_translator,
            response_translator=response_translator,
            owner=owner,
        )
