from dataclasses import dataclass

from .resource import Arg, Resource, ResourceSpec
from ..scope import SchemaScope
from ..enums import DataType, Language, NullHandling, ResourceType, Volatility
from ..props import (
    ArgsProp,
    EnumFlagProp,
    EnumProp,
    FlagProp,
    IdentifierListProp,
    Props,
    StringListProp,
    StringProp,
)


@dataclass
class _JavascriptUDF(ResourceSpec):
    name: str
    returns: str
    as_: str
    language: Language = Language.JAVASCRIPT
    args: list[Arg] = None
    comment: str = None
    copy_grants: bool = False
    external_access_integrations: list[str] = None
    handler: str = None
    imports: list[str] = None
    null_handling: NullHandling = None
    owner: str = "SYSADMIN"
    packages: list[str] = None
    runtime_version: str = None
    secrets: dict[str, str] = None
    secure: bool = None
    volatility: Volatility = None


class JavascriptUDF(Resource):
    resource_type = ResourceType.FUNCTION

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
    scope = SchemaScope()
    spec = _JavascriptUDF

    def __init__(
        self,
        name: str,
        returns: DataType,
        as_: str,
        copy_grants: bool = False,
        owner: str = "SYSADMIN",
        secure: bool = False,
        args: list = [],
        null_handling: NullHandling = None,
        volatility: Volatility = None,
        comment: str = None,
        **kwargs,
    ):
        kwargs.pop("language", None)
        super().__init__(**kwargs)
        self._data = _JavascriptUDF(
            name=name,
            returns=returns,
            as_=as_,
            copy_grants=copy_grants,
            owner=owner,
            secure=secure,
            args=args,
            null_handling=null_handling,
            volatility=volatility,
            comment=comment,
        )


@dataclass
class _PythonUDF(ResourceSpec):
    name: str
    returns: str
    runtime_version: str
    handler: str
    language: Language = Language.PYTHON
    args: list[Arg] = None
    as_: str = None
    comment: str = None
    copy_grants: bool = False
    external_access_integrations: list[str] = None
    imports: list[str] = None
    null_handling: NullHandling = None
    owner: str = "SYSADMIN"
    packages: list[str] = None
    secrets: dict[str, str] = None
    secure: bool = False
    volatility: Volatility = None


class PythonUDF(Resource):
    resource_type = ResourceType.FUNCTION
    props = Props(
        secure=FlagProp("secure"),
        args=ArgsProp(),
        returns=EnumProp("returns", DataType, eq=False),
        language=EnumProp("language", [Language.PYTHON], eq=False),
        null_handling=EnumFlagProp(NullHandling),
        volatility=EnumFlagProp(Volatility),
        runtime_version=StringProp("runtime_version"),
        comment=StringProp("comment"),
        imports=StringListProp("imports", parens=True),
        packages=StringListProp("packages", parens=True),
        handler=StringProp("handler"),
        external_access_integrations=IdentifierListProp("external_access_integrations", parens=True),
        as_=StringProp("as", eq=False),
    )
    scope = SchemaScope()
    spec = _PythonUDF

    def __init__(
        self,
        name: str,
        returns: str,
        runtime_version: str,
        handler: str,
        args: list = None,
        as_: str = None,
        comment: str = None,
        copy_grants: bool = False,
        external_access_integrations: list[str] = None,
        imports: list[str] = None,
        null_handling: NullHandling = None,
        owner: str = "SYSADMIN",
        packages: list[str] = None,
        secrets: dict[str, str] = None,
        secure: bool = None,
        volatility: Volatility = None,
        **kwargs,
    ):
        kwargs.pop("language", None)
        super().__init__(**kwargs)
        self._data: _PythonUDF = _PythonUDF(
            name=name,
            returns=returns,
            runtime_version=runtime_version,
            handler=handler,
            args=args,
            as_=as_,
            comment=comment,
            copy_grants=copy_grants,
            external_access_integrations=external_access_integrations,
            imports=imports,
            null_handling=null_handling,
            owner=owner,
            packages=packages,
            secrets=secrets,
            secure=secure,
            volatility=volatility,
        )

    @property
    def fqn(self):
        name = f"{self._data.name}({', '.join([str(arg['data_type']) for arg in self._data.args])})"
        return self.scope.fully_qualified_name(self._container, name)


FunctionMap = {
    Language.JAVASCRIPT: JavascriptUDF,
    Language.PYTHON: PythonUDF,
}
