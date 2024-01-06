from .base import Resource, SchemaScoped, _fix_class_documentation
from ..enums import DataType, ExecutionRights, NullHandling, Language
from ..props import (
    ArgsProp,
    EnumFlagProp,
    EnumProp,
    FlagProp,
    IdentifierListProp,
    Props,
    StringProp,
    StringListProp,
)


class StoredProcedure(SchemaScoped, Resource):
    resource_type = "PROCEDURE"


@_fix_class_documentation
class PythonStoredProcedure(StoredProcedure):
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
