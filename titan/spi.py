# Stored Procedure Interface (spi)
import inspect
import pydoc
import re
import sys

from snowflake.snowpark.exceptions import SnowparkSQLException

from . import data_provider as dp
from . import lifecycle, resources, __version__
from .blueprint import Action, Blueprint
from .diff import diff
from .enums import DataType, ResourceType
from .identifiers import FQN, URN
from .parse import parse_identifier
from .scope import DatabaseScope, SchemaScope

SNOWPARK_TELEMETRY_ID = "titan_titan"

try:
    import _snowflake  # type: ignore

    _snowflake.snowflake_partner_attribution().append(SNOWPARK_TELEMETRY_ID)
except ModuleNotFoundError as err:
    # raise ModuleNotFoundError("The titan spi module can only be run from a Snowpark UDF or stored procedure") from err
    pass

__this__ = sys.modules[__name__]

_PYTHON_TO_SNOWFLAKE_TYPE = {
    int: DataType.INTEGER,
    float: DataType.FLOAT,
    str: DataType.VARCHAR,
    bool: DataType.BOOLEAN,
    dict: DataType.OBJECT,
    list: DataType.ARRAY,
}


def _python_args_to_sproc_args(parameters):
    """
    Converts a dictionary of Python arguments to a list of Snowflake stored procedure arguments.
    """
    sproc_args = []
    for name, param in parameters.items():
        if name == "sp_session":
            continue
        arg = {"name": name, "data_type": _PYTHON_TO_SNOWFLAKE_TYPE[param.annotation]}
        if param.default is not inspect.Parameter.empty:
            arg["default"] = param.default
        sproc_args.append(arg)
    return sproc_args


def procedure(schema="PUBLIC"):
    def decorator(func):
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)

        wrapper.is_procedure = True
        wrapper.sproc_args = _python_args_to_sproc_args(inspect.signature(func).parameters)
        wrapper.schema = schema
        return wrapper

    return decorator


def install(sp_session):
    """
    Installs the titan spi functions and procedures into the current database.
    """

    conn = sp_session.connection

    session_ctx = dp.fetch_session(conn)
    if "SYSADMIN" != session_ctx["role"]:
        # TODO: make dynamic role selection work
        raise Exception(f"You must use the SYSADMIN role to install the Titan SPI [role={session_ctx['role']}]")

    visible_stages = dp.list_stages(conn)
    stage = None
    for s in visible_stages:
        if s["url"] == "s3://titan-snowflake/":
            stage = s
            break

    if stage is None:
        raise Exception(
            "Cannot find Titan stage. Did you forget to run `CREATE STAGE titan_aws URL = 's3://titan-snowflake/'`?"
        )

    titan_db = stage["database_name"]

    blueprint = Blueprint("titan", database=titan_db, allow_role_switching=False)

    sprocs = []
    for name, func in vars(__this__).items():
        if callable(func) and getattr(func, "is_procedure", False):
            sprocs.append(
                resources.PythonStoredProcedure(
                    name=name,
                    owner="SYSADMIN",
                    args=func.sproc_args,
                    returns=DataType.OBJECT,
                    runtime_version="3.9",
                    packages=["snowflake-snowpark-python", "inflection", "pyparsing"],
                    imports=[f"@{stage['fqn']}/releases/titan-{__version__}.zip"],
                    handler=f"titan.spi.{name}",
                    execute_as="CALLER",
                    schema=func.schema,
                ),
            )

    blueprint.add(
        resources.Schema(name="BLUEPRINT", comment="Blueprint functions"),
        *sprocs,
    )
    plan = blueprint.plan(conn)
    result = blueprint.apply(conn, plan)
    return _to_object(
        {
            "plan": plan,
            "actions": result,
        }
    )


def _execute(sf_session, sql: list):
    for sql_text in sql:
        try:
            sf_session.execute_string(sql_text)
        except SnowparkSQLException as err:
            raise SnowparkSQLException(f"failed to execute sql, [{sql_text}]", error_code=err.error_code) from err


def _to_object(obj):
    if isinstance(obj, dict):
        return {_to_object(k): _to_object(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_to_object(v) for v in obj]
    if isinstance(obj, (Action, URN)):
        return str(obj)
    if obj is None:
        return obj
    if not isinstance(obj, (int, float, str, bool)):
        raise Exception(f"Cannot convert {type(obj)} to JSON")
    return obj


###############################################################################
# Blueprint functions
###############################################################################


# @procedure()(schema="BLUEPRINT")
# def blueprint_plan(sp_session, blueprint: dict):
#     conn = sp_session.connection
#     bp = Blueprint(**blueprint)
#     plan = blueprint.plan(conn)
#     return _to_object(
#         {
#             "plan": plan,
#         }
#     )


###############################################################################
# Create or Update functions
###############################################################################


def _create_or_update_resource(
    sf_session,
    resource_type: ResourceType,
    config: dict,
    dry_run: bool = False,
):
    fqn = parse_identifier(config["name"], is_db_scoped=(resource_type == ResourceType.SCHEMA))

    resource_cls = resources.Resource.resolve_resource_cls(resource_type)
    session_ctx = dp.fetch_session(sf_session)
    if isinstance(resource_cls.scope, (DatabaseScope, SchemaScope)) and fqn.database is None:
        fqn.database = config.get("database", session_ctx["database"])
    urn = URN(resource_type=resource_type, fqn=fqn, account_locator=session_ctx["account_locator"])
    original = dp.fetch_resource(sf_session, urn)
    sql = []
    if original:
        resource_cls = resources.Resource.resolve_resource_cls(resource_type)
        original = {str(urn): dp.remove_none_values(original)}
        new = {str(urn): dp.remove_none_values(resource_cls.defaults() | config)}
        for resource_diff in diff(original, new):
            # TODO: replication groups need both sides of the diff (old and new values).
            # There are most likely other resources that need this as well.
            sql.append(lifecycle.update_resource(urn, resource_diff.new_value, resource_cls.props))
    else:
        sql = [lifecycle.create_resource(urn, config, if_not_exists=True)]
    if not dry_run:
        _execute(sf_session, sql)
    return {"sql": sql}


@procedure()
def create_or_update_database(sp_session, config: dict, dry_run: bool = False):
    return _create_or_update_resource(sp_session.connection, ResourceType.DATABASE, config, dry_run)


@procedure()
def create_or_update_schema(sp_session, config: dict, dry_run: bool = False):
    """
    Takes configuration (either as an OBJECT or a YAML string) and creates or updates a schema.
    Use the `dry_run` parameter to test the operation without executing any SQL.

    Parameters
    ----------
    config : OBJECT
        A dictionary containing the schema configuration
    dry_run : BOOLEAN
        If True, do not execute any SQL

    Schema Config
    -------------
    name : STRING
        The name of the schema
    database : STRING
        The name of the database in which to create the schema
    comment : STRING
        A comment to attach to the schema
    data_retention_time_in_days : INTEGER
        The number of days to retain data in the schema
    default_ddl_collation : STRING
        The default collation for the schema
    max_data_extension_time_in_days : INTEGER
        The maximum number of days to extend data in the schema
    owner : STRING
        The name of the user or role that owns the schema
    transient : BOOLEAN
        If True, the schema is transient
    managed_access : BOOLEAN
        If True, the schema is managed
    """
    return _create_or_update_resource(sp_session.connection, ResourceType.SCHEMA, config, dry_run)


@procedure()
def create_or_update_user(sp_session, config: dict, dry_run: bool = False):
    """
    Takes configuration (either as an OBJECT or a YAML string) and creates or updates a user.
    Use the `dry_run` parameter to test the operation without executing any SQL.

    Parameters
    ----------
    config : OBJECT
        A dictionary containing the schema configuration
    dry_run : BOOLEAN
        If True, do not execute any SQL

    User Config
    -----------
    name : STRING
        The name of the user
    comment : STRING
        A comment to attach to the user

    """
    return _create_or_update_resource(sp_session.connection, ResourceType.USER, config, dry_run)


@procedure()
def create_or_update_warehouse(sp_session, config: dict, dry_run: bool = False):
    return _create_or_update_resource(sp_session.connection, ResourceType.WAREHOUSE, config, dry_run)


@procedure()
def create_or_update_role(sp_session, config: dict, dry_run: bool = False):
    return _create_or_update_resource(sp_session.connection, ResourceType.ROLE, config, dry_run)


###############################################################################
# Fetch functions
###############################################################################


@procedure()
def fetch_database(sp_session, name: str) -> dict:
    """
    Returns a database's configuration.
    """
    return dp.fetch_database(sp_session.connection, FQN(name))


@procedure()
def fetch_schema(sp_session, name: str) -> dict:
    """
    Returns a schema's configuration.
    """
    fqn = parse_identifier(name, is_db_scoped=True)
    if fqn.database is None:
        fqn.database = sp_session.get_current_database()
    return dp.fetch_schema(sp_session.connection, fqn)


@procedure()
def fetch_user(sp_session, name: str) -> dict:
    """
    Returns a user's configuration.
    """
    return dp.fetch_user(sp_session.connection, FQN(name))


@procedure()
def fetch_warehouse(sp_session, name: str) -> dict:
    """
    Returns a user's configuration.
    """
    return dp.fetch_warehouse(sp_session.connection, FQN(name))


@procedure()
def fetch_role(sp_session, name: str) -> dict:
    """
    Returns a user's configuration.
    """
    return dp.fetch_role(sp_session.connection, FQN(name))


# def git_export(sp_session, locator: str, repo: str, path: str) -> dict:
#     access_token = _snowflake.get_generic_secret_string("github_access_token")
#     return git.export(
#         sp_session.connection,
#         repo=repo,  # Can this be configured?
#         path=path,
#         locator_str="database:TITAN",
#         access_token=access_token,
#     )


def help(_):
    txt = re.sub("[^\b]\b", "", pydoc.render_doc(__this__, "Help on %s"))
    txt = txt.split("\n\nNAME\n    ")[1].split("\n\nFILE\n")[0]
    return txt
