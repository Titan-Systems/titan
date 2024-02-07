from dataclasses import dataclass, field

from .resource import Arg, Resource, ResourceSpec
from ..scope import SchemaScope
from ..enums import DataType, ExecutionRights, NullHandling, Language, ResourceType
from ..props import (
    ArgsProp,
    EnumProp,
    FlagProp,
    IdentifierListProp,
    Props,
    StringListProp,
    StringProp,
)


@dataclass
class _PythonStoredProcedure(ResourceSpec):
    name: str
    args: list[Arg]
    returns: DataType
    runtime_version: str
    packages: list  # = ["snowflake-snowpark-python"]
    handler: str
    language: Language = Language.PYTHON
    as_: str = None
    comment: str = "user-defined procedure"
    copy_grants: bool = False
    execute_as: ExecutionRights = ExecutionRights.OWNER
    external_access_integrations: list = None
    imports: list = field(default_factory=None, metadata={"triggers_replacement": True})
    null_handling: NullHandling = NullHandling.CALLED_ON_NULL_INPUT
    owner: str = "SYSADMIN"
    secure: bool = False

    def __post_init__(self):
        super().__post_init__()
        if self.packages is not None and len(self.packages) == 0:
            raise ValueError("packages can't be empty")


class PythonStoredProcedure(Resource):
    """
    CREATE [ OR REPLACE ] [ SECURE ] PROCEDURE <name> (
        [ <arg_name> <arg_data_type> [ DEFAULT <default_value> ] ] [ , ... ] )
    [ COPY GRANTS ]
    RETURNS { <result_data_type> [ [ NOT ] NULL ] | TABLE ( [ <col_name> <col_data_type> [ , ... ] ] ) }
    LANGUAGE PYTHON
    RUNTIME_VERSION = '<python_version>'
    PACKAGES = ( 'snowflake-snowpark-python[==<version>]'[, '<package_name>[==<version>]' ... ])
    [ IMPORTS = ( '<stage_path_and_file_name_to_read>' [, '<stage_path_and_file_name_to_read>' ...] ) ]
    HANDLER = '<function_name>'
    [ EXTERNAL_ACCESS_INTEGRATIONS = ( <name_of_integration> [ , ... ] ) ]
    [ SECRETS = ('<secret_variable_name>' = <secret_name> [ , ... ] ) ]
    [ { CALLED ON NULL INPUT | { RETURNS NULL ON NULL INPUT | STRICT } } ]
    [ { VOLATILE | IMMUTABLE } ] -- Note: VOLATILE and IMMUTABLE are deprecated.
    [ COMMENT = '<string_literal>' ]
    [ EXECUTE AS { CALLER | OWNER } ]
    AS '<procedure_definition>'
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
        # secrets
        # Not working in Snowflake
        # null_handling=EnumFlagProp(NullHandling),
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
        # for import_location in imports:
        #     stage = _parse_stage_path(import_location)
        #     # TODO: fix polymorphic resources
        kwargs.pop("language", None)
        super().__init__(**kwargs)

        self._data: _PythonStoredProcedure = _PythonStoredProcedure(
            name=name,
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
        name = f"{self._data.name}({', '.join([str(arg['data_type']) for arg in self._data.args])})"
        return self.scope.fully_qualified_name(self._container, name)


ProcedureMap = {
    Language.PYTHON: PythonStoredProcedure,
}
