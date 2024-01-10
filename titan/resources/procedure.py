from abc import ABC
from typing import Union

from .base import Resource, SchemaScoped, _fix_class_documentation
from ..enums import DataType, ExecutionRights, NullHandling, Language
from ..parse import _resolve_resource_class, _parse_stage_path
from ..props import (
    ArgsProp,
    EnumFlagProp,
    EnumProp,
    FlagProp,
    Props,
    StringProp,
    StringListProp,
)
from .stage import Stage


@_fix_class_documentation
class PythonStoredProcedure(SchemaScoped, Resource):
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

    resource_type = "PROCEDURE"

    props = Props(
        secure=FlagProp("secure"),
        args=ArgsProp(),
        copy_grants=FlagProp("copy_grants"),
        returns=EnumProp("returns", DataType, eq=False),
        language=EnumProp("language", [Language.PYTHON], eq=False),
        runtime_version=StringProp("runtime_version"),
        packages=StringListProp("packages", parens=True),
        imports=StringListProp("imports", parens=True),
        handler=StringProp("handler"),
        # external_access_integrations=IdentifierListProp("external_access_integrations"),
        # secrets
        null_handling=EnumFlagProp(NullHandling),
        comment=StringProp("comment"),
        execute_as=EnumProp("execute as", ExecutionRights, eq=False),
        as_=StringProp("as", eq=False),
    )

    name: str
    owner: str = "SYSADMIN"
    secure: bool = False
    args: list
    returns: DataType
    copy_grants: bool = False
    language: Language = Language.PYTHON
    runtime_version: str
    # FIXME: this is a situation where scrubbing defaults when an object is serialized is bad
    packages: list  # = ["snowflake-snowpark-python"]
    imports: list = []
    handler: str
    external_access_integrations: list = []
    null_handling: NullHandling = None
    comment: str = None
    execute_as: ExecutionRights = None
    as_: str = None

    def __init__(
        self,
        name: str,
        owner: str = "SYSADMIN",
        secure: bool = False,
        args: list = None,
        returns: DataType = None,
        copy_grants: bool = False,
        language: Language = Language.PYTHON,
        runtime_version: str = None,
        packages: list = None,
        imports: list = [],
        handler: str = None,
        external_access_integrations: list = [],
        null_handling: NullHandling = NullHandling.CALLED_ON_NULL_INPUT,
        comment: str = "",
        execute_as: ExecutionRights = ExecutionRights.CALLER,
        as_: str = None,
    ):
        for import_location in imports:
            stage = _parse_stage_path(import_location)
            # TODO: fix polymorphic resources
            # self.requires(Stage(name="stage", stub=True))

        super().__init__(
            name=name,
            owner=owner,
            secure=secure,
            args=args,
            returns=returns,
            copy_grants=copy_grants,
            language=language,
            runtime_version=runtime_version,
            packages=packages,
            imports=imports,
            handler=handler,
            external_access_integrations=external_access_integrations,
            null_handling=null_handling,
            comment=comment,
            execute_as=execute_as,
            as_=as_,
        )


ProcedureMap = {
    Language.PYTHON: PythonStoredProcedure,
}


class Procedure(Resource, ABC):
    def __new__(cls, type: Union[str, Language], **kwargs) -> PythonStoredProcedure:
        language = Language.parse(type)
        sproc_cls = ProcedureMap[language]
        return sproc_cls(language=language, **kwargs)

    @classmethod
    def from_sql(cls, sql):
        resource_cls = Resource.classes[_resolve_resource_class(sql)]
        return resource_cls.from_sql(sql)
