from dataclasses import dataclass, field

from ..enums import DataType, ExecutionRights, Language, NullHandling, ResourceType
from ..identifiers import FQN
from ..props import (
    ArgsProp,
    EnumProp,
    FlagProp,
    IdentifierListProp,
    Props,
    StringListProp,
    StringProp,
)
from ..resource_name import ResourceName
from ..role_ref import RoleRef
from ..scope import SchemaScope
from .resource import Arg, NamedResource, Resource, ResourceSpec


@dataclass(unsafe_hash=True)
class _PythonStoredProcedure(ResourceSpec):
    name: ResourceName
    args: list[Arg]
    returns: DataType
    runtime_version: str
    packages: list  # = ["snowflake-snowpark-python"]
    handler: str
    language: Language = Language.PYTHON
    as_: str = None
    comment: str = "user-defined procedure"
    copy_grants: bool = field(default=None, metadata={"fetchable": False})
    execute_as: ExecutionRights = ExecutionRights.OWNER
    external_access_integrations: list = None
    imports: list = field(default=None, metadata={"triggers_replacement": True})
    null_handling: NullHandling = NullHandling.CALLED_ON_NULL_INPUT
    owner: RoleRef = "SYSADMIN"
    secure: bool = False

    def __post_init__(self):
        super().__post_init__()
        if self.packages is not None and len(self.packages) == 0:
            raise ValueError("packages can't be empty")


class PythonStoredProcedure(NamedResource, Resource):
    """
    Description:
        Represents a Python stored procedure in Snowflake, allowing for the execution of Python code within the Snowflake environment.

    Snowflake Docs:
    https://docs.snowflake.com/en/sql-reference/sql/create-procedure

    Fields:
        name (str, required): The name of the procedure.
        args (list): The arguments of the procedure.
        returns (DataType): The data type of the return value.
        runtime_version (str, required): The Python runtime version.
        packages (list): The list of packages required by the procedure.
        handler (str, required): The handler function for the procedure.
        as_ (str): The procedure definition.
        comment (str): A comment about the procedure. Defaults to "user-defined procedure".
        copy_grants (bool): Whether to copy grants. Defaults to False.
        execute_as (ExecutionRights): The execution rights. Defaults to ExecutionRights.CALLER.
        external_access_integrations (list): External access integrations if any.
        imports (list): Files to import.
        null_handling (NullHandling): How nulls are handled. Defaults to NullHandling.CALLED_ON_NULL_INPUT.
        owner (string or Role): The owner of the procedure. Defaults to "SYSADMIN".
        secure (bool): Whether the procedure is secure. Defaults to False.

    Python:

        ```python
        procedure = PythonStoredProcedure(
            name="some_procedure",
            args=[],
            returns="STRING",
            runtime_version="3.8",
            packages=["snowflake-snowpark-python"],
            handler="process_data",
            as_="def process_data(): return 'Hello, World!'",
            comment="A simple procedure",
            copy_grants=False,
            execute_as="CALLER",
            external_access_integrations=None,
            imports=None,
            null_handling="CALLED_ON_NULL_INPUT",
            owner="SYSADMIN",
            secure=False
        )
        ```

    Yaml:

        ```yaml
        procedures:
        - name: some_procedure
            args: []
            returns: STRING
            runtime_version: "3.8"
            packages:
            - snowflake-snowpark-python
            handler: process_data
            as_: "def process_data(): return 'Hello, World!'"
            comment: "A simple procedure"
            copy_grants: false
            execute_as: CALLER
            external_access_integrations: null
            imports: null
            null_handling: CALLED_ON_NULL_INPUT
            owner: SYSADMIN
            secure: false
        ```
    """

    resource_type = ResourceType.PROCEDURE

    props = Props(
        secure=FlagProp("secure"),
        args=ArgsProp(),
        copy_grants=FlagProp("copy grants"),
        returns=EnumProp("returns", DataType, eq=False),
        language=EnumProp("language", [Language.PYTHON], eq=False),
        runtime_version=StringProp("runtime_version"),
        packages=StringListProp("packages", parens=True),
        imports=StringListProp("imports", parens=True),
        handler=StringProp("handler"),
        external_access_integrations=IdentifierListProp("external_access_integrations", parens=True),
        comment=StringProp("comment"),
        execute_as=EnumProp("execute as", ExecutionRights, eq=False),
        as_=StringProp("as", eq=False),
    )

    scope = SchemaScope()
    spec = _PythonStoredProcedure

    def __init__(
        self,
        name: str,
        args: list,
        returns: DataType,
        runtime_version: str,
        packages: list,
        handler: str,
        as_: str = None,
        comment: str = "user-defined procedure",
        copy_grants: bool = False,
        execute_as: ExecutionRights = ExecutionRights.CALLER,
        external_access_integrations: list = None,
        imports: list = None,
        null_handling: NullHandling = NullHandling.CALLED_ON_NULL_INPUT,
        owner: str = "SYSADMIN",
        secure: bool = False,
        **kwargs,
    ):
        kwargs.pop("language", None)
        super().__init__(name, **kwargs)

        self._data: _PythonStoredProcedure = _PythonStoredProcedure(
            name=self._name,
            args=args,
            returns=returns,
            runtime_version=runtime_version,
            packages=packages,
            handler=handler,
            as_=as_,
            comment=comment,
            copy_grants=copy_grants,
            execute_as=execute_as,
            external_access_integrations=external_access_integrations,
            imports=imports,
            null_handling=null_handling,
            owner=owner,
            secure=secure,
        )

    @property
    def fqn(self):
        return sproc_fqn(self)


def sproc_fqn(sproc: PythonStoredProcedure):
    schema = sproc.container
    database = schema.container if schema else None
    return FQN(
        name=sproc.name,
        database=database.name if database else None,
        schema=schema.name if schema else None,
        arg_types=[str(arg["data_type"]) for arg in sproc._data.args],
    )


ProcedureMap = {
    Language.PYTHON: PythonStoredProcedure,
}
