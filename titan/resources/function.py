from dataclasses import dataclass, field
from typing import Union

from ..enums import Language, NullHandling, ResourceType, Volatility
from ..identifiers import FQN
from ..props import (
    ArgsProp,
    EnumFlagProp,
    EnumProp,
    FlagProp,
    IdentifierListProp,
    Props,
    ReturnsProp,
    StringListProp,
    StringProp,
)
from ..resource_name import ResourceName
from ..role_ref import RoleRef
from ..scope import SchemaScope
from .resource import Arg, NamedResource, Resource, ResourceSpec


@dataclass(unsafe_hash=True)
class _JavascriptUDF(ResourceSpec):
    name: ResourceName
    args: list[Arg]
    returns: str
    as_: str
    language: Language = Language.JAVASCRIPT
    comment: str = None
    copy_grants: bool = field(default=None, metadata={"fetchable": False})
    external_access_integrations: list[str] = None
    handler: str = None
    imports: list[str] = None
    null_handling: NullHandling = None
    owner: RoleRef = "SYSADMIN"
    packages: list[str] = None
    runtime_version: str = None
    secrets: dict[str, str] = None
    secure: bool = None
    volatility: Volatility = None


class JavascriptUDF(NamedResource, Resource):
    """
    Description:
        A Javascript user-defined function (UDF) in Snowflake allows you to define a function using the JavaScript programming language.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-function

    Fields:
        name (string, required): The name of the function.
        returns (string or DataType, required): The data type of the function's return value.
        as_ (string, required): The JavaScript code to execute when the function is called.
        args (list): The arguments that the function takes.
        comment (string): A comment for the function.
        copy_grants (bool): Specifies whether to retain the access privileges from the original function when a new function is created using CREATE OR REPLACE FUNCTION. Defaults to False.
        external_access_integrations (list): External integrations accessible by the function.
        handler (string): The entry point for the function within the JavaScript code.
        imports (list): The list of JavaScript files to import.
        null_handling (string or NullHandling): How the function handles NULL input.
        owner (string or Role): The owner of the function. Defaults to "SYSADMIN".
        packages (list): The list of npm packages that the function depends on.
        runtime_version (string): The JavaScript runtime version to use.
        secrets (dict of string to string): Key-value pairs of secrets available to the function.
        secure (bool): Specifies whether the function is secure. Defaults to False.
        volatility (string or Volatility): The volatility of the function.

    Python:

        ```python
        js_udf = JavascriptUDF(
            name="some_function",
            returns="STRING",
            as_="function(x) { return x.toUpperCase(); }",
            args=[{"name": "x", "data_type": "STRING"}],
            comment="Converts a string to uppercase",
        )
        ```

    Yaml:

        ```yaml
        functions:
          - name: some_function
            returns: STRING
            as_: function(x) { return x.toUpperCase(); }
            args:
              - name: x
                data_type: STRING
            comment: Converts a string to uppercase
        ```
    """

    resource_type = ResourceType.FUNCTION

    props = Props(
        secure=FlagProp("secure"),
        args=ArgsProp(),
        # Specifies to retain the access privileges from the original function when a new
        # function is created using CREATE OR REPLACE FUNCTION.
        # copy_grants=FlagProp("copy_grants"),
        # returns=EnumProp("returns", DataType, eq=False),
        returns=ReturnsProp("returns", eq=False),
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
        args: list,
        returns: str,
        as_: str,
        copy_grants: bool = False,
        owner: str = "SYSADMIN",
        secure: bool = False,
        null_handling: NullHandling = None,
        volatility: Volatility = None,
        comment: str = None,
        **kwargs,
    ):
        kwargs.pop("language", None)
        super().__init__(name, **kwargs)
        self._data: _JavascriptUDF = _JavascriptUDF(
            name=self._name,
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

    @property
    def fqn(self):
        return udf_fqn(self)


@dataclass(unsafe_hash=True)
class _PythonUDF(ResourceSpec):
    name: ResourceName
    returns: str
    runtime_version: str
    handler: str
    args: list[Arg]
    language: Language = Language.PYTHON
    as_: str = None
    comment: str = None
    copy_grants: bool = field(default=None, metadata={"fetchable": False})
    external_access_integrations: list[str] = None
    imports: list[str] = None
    null_handling: NullHandling = None
    owner: RoleRef = "SYSADMIN"
    packages: list[str] = None
    secrets: dict[str, str] = None
    secure: bool = False
    volatility: Volatility = None


class PythonUDF(NamedResource, Resource):
    """
    Description:
        A Python user-defined function (UDF) in Snowflake allows users to define their own custom functions in Python.
        These functions can be used to perform operations that are not available as standard SQL functions.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-function

    Fields:
        name (string, required): The name of the function.
        returns (string, required): The data type of the function's return value.
        runtime_version (string, required): The version of the Python runtime to use.
        handler (string, required): The name of the method to call in the Python script.
        args (list, required): A list of arguments that the function takes.
        as_ (string): The Python code to execute when the function is called.
        comment (string): A comment for the function.
        copy_grants (bool): Whether to copy grants from the existing function. Defaults to False.
        external_access_integrations (list): List of external integrations accessible by the function.
        imports (list): List of modules to import in the function.
        null_handling (NullHandling): Specifies how NULL values are handled by the function.
        owner (string or Role): The owner role of the function. Defaults to "SYSADMIN".
        packages (list): List of Python packages that the function can use.
        secrets (dict): Secrets that can be accessed by the function.
        secure (bool): Whether the function is secure. Defaults to False.
        volatility (string or Volatility): The volatility of the function.

    Python:

        ```python
        python_udf = PythonUDF(
            name="some_python_udf",
            returns="string",
            runtime_version="3.8",
            handler="process_data",
            args=[{"name": "input_data", "data_type": "string"}],
            as_="process_data_function",
            comment="This function processes data.",
            copy_grants=False,
            external_access_integrations=["s3_integration"],
            imports=["pandas", "numpy"],
            null_handling="CALLED_ON_NULL_INPUT",
            owner="SYSADMIN",
            packages=["pandas", "numpy"],
            secrets={"api_key": "secret_value"},
            secure=False,
            volatility="IMMUTABLE"
        )
        ```

    Yaml:

        ```yaml
        python_udfs:
          - name: some_python_udf
            returns: string
            runtime_version: 3.8
            handler: process_data
            args:
              - name: input_data
                data_type: string
            as_: process_data_function
            comment: This function processes data.
            copy_grants: false
            external_access_integrations:
              - s3_integration
            imports:
              - pandas
              - numpy
            null_handling: CALLED_ON_NULL_INPUT
            owner: SYSADMIN
            packages:
              - pandas
              - numpy
            secrets:
              api_key: secret_value
            secure: false
            volatility: IMMUTABLE
        ```
    """

    resource_type = ResourceType.FUNCTION
    props = Props(
        secure=FlagProp("secure"),
        args=ArgsProp(),
        # returns=EnumProp("returns", DataType, eq=False),
        returns=ReturnsProp("returns", eq=False),
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
        args: list,
        returns: str,
        runtime_version: str,
        handler: str,
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
        super().__init__(name, **kwargs)
        self._data: _PythonUDF = _PythonUDF(
            name=self._name,
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
        return udf_fqn(self)


def udf_fqn(udf: Union[JavascriptUDF, PythonUDF]):
    schema = udf.container
    database = schema.container if schema else None
    return FQN(
        name=udf.name,
        database=database.name if database else None,
        schema=schema.name if schema else None,
        arg_types=[str(arg["data_type"]) for arg in udf._data.args],
    )


FunctionMap = {
    Language.JAVASCRIPT: JavascriptUDF,
    Language.PYTHON: PythonUDF,
}


def _resolver(data: dict):
    return FunctionMap[Language(data["language"])]


Resource.__resolvers__[ResourceType.FUNCTION] = _resolver
