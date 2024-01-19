# Stored Procedure Interface (spi)
import os
import pydoc
import re
import sys

os.environ["PYTHONIOENCODING"] = "utf-8"

from yaml import safe_load

from snowflake.snowpark.exceptions import SnowparkSQLException

from . import data_provider as dp
from . import lifecycle, resources
from .blueprint import Blueprint
from .diff import diff
from .enums import DataType, ResourceType
from .identifiers import FQN, URN
from .parse import parse_identifier

SNOWPARK_TELEMETRY_ID = "titan_titan"

try:
    import _snowflake  # type: ignore

    _snowflake.snowflake_partner_attribution().append(SNOWPARK_TELEMETRY_ID)
except ModuleNotFoundError as err:
    # raise ModuleNotFoundError("The titan spi module can only be run from a Snowpark UDF or stored procedure") from err
    pass

__this__ = sys.modules[__name__]


def install(sp_session):
    """
    Installs the titan spi functions and procedures into the current database.
    """
    blueprint = Blueprint("titan", database="titan")
    blueprint.add(
        resources.Role(name="TITAN_ADMIN", comment="Role for Titan administrators"),
        resources.RoleGrant(role="TITAN_ADMIN", to_role="SYSADMIN"),
        resources.PythonStoredProcedure(
            name="fetch_database",
            owner="TITAN_ADMIN",
            args=[("name", DataType.VARCHAR)],
            returns=DataType.OBJECT,
            runtime_version="3.9",
            packages=["snowflake-snowpark-python"],
            imports=["@TITAN/titan-latest.zip"],
            handler="titan.spi.fetch_database",
            execute_as="CALLER",
        ),
    )
    plan = blueprint.plan(sp_session.connection)
    blueprint.apply(sp_session.connection, plan)


def _execute(sp_session, sql: list):
    for sql_text in sql:
        try:
            sp_session.sql(sql_text).collect()
        except SnowparkSQLException as err:
            raise SnowparkSQLException(f"failed to execute sql, [{sql_text}]", error_code=err.error_code) from err


# _schema_defaults = {
#     "database": None,
#     "transient": False,
#     "owner": "SYSADMIN",
#     "managed_access": False,
#     "data_retention_time_in_days": None,
#     "max_data_extension_time_in_days": 14,
#     "default_ddl_collation": None,
#     "tags": None,
#     "comment": None,
# }


def create_or_update_schema(sp_session, config: dict = None, yaml: str = None, dry_run: bool = False):
    """
    Takes configuration (either as an OBJECT or a YAML string) and creates or updates a schema.
    Use the `dry_run` parameter to test the operation without executing any SQL.

    Parameters
    ----------
    config : OBJECT
        A dictionary containing the schema configuration
    yaml : VARCHAR
        A YAML string containing the schema configuration
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
    if yaml and config is None:
        config = safe_load(yaml)
    _create_or_update_resource(sp_session.connection, ResourceType.SCHEMA, config, dry_run)


def create_or_update_user(sp_session, config: dict = None, yaml: str = None, dry_run: bool = False):
    """
    Takes configuration (either as an OBJECT or a YAML string) and creates or updates a user.
    Use the `dry_run` parameter to test the operation without executing any SQL.

    Parameters
    ----------
    config : OBJECT
        A dictionary containing the schema configuration
    yaml : VARCHAR
        A YAML string containing the schema configuration
    dry_run : BOOLEAN
        If True, do not execute any SQL

    User Config
    -----------
    name : STRING
        The name of the user
    comment : STRING

    """
    if yaml and config is None:
        config = safe_load(yaml)
    _create_or_update_resource(sp_session.connection, ResourceType.USER, config, dry_run)


def _create_or_update_resource(
    sf_session,
    resource_type: ResourceType,
    config: dict,
    dry_run: bool = False,
):
    fqn = parse_identifier(config["name"], is_schema=(resource_type == ResourceType.SCHEMA))

    session_ctx = dp.fetch_session()
    fqn.database = config.get("database", session_ctx["database"])
    urn = URN(resource_type=str(resource_type), fqn=fqn, account_locator=session_ctx["account_locator"])
    original = dp.fetch_resource(sf_session, fqn)
    sql = []
    if original:
        resource_cls = resources.Resource.resolve_resource_cls(resource_type)
        original = {str(urn): dp.remove_none_values(original)}
        new = {str(urn): dp.remove_none_values(resource_cls.defaults() | config)}
        for _, _, change in diff(original, new):
            sql.append(lifecycle.update_resource(urn, change, resource_cls.props))
    else:
        sql = [lifecycle.create_resource(urn, config, if_not_exists=True)]
    if not dry_run:
        _execute(sf_session, sql)
    return {"sql": sql}


def fetch_schema(sp_session, name) -> dict:
    """
    Returns a schema's configuration.
    """
    fqn = FQN.from_str(name, resource_key="schema")
    if fqn.database is None:
        fqn.database = sp_session.get_current_database()
    return dp.fetch_schema(sp_session.connection, fqn)


def fetch_database(sp_session, name) -> dict:
    """
    Returns a database's configuration.
    """
    fqn = FQN.from_str(name, resource_key="database")
    return dp.fetch_database(sp_session.connection, fqn)


def fetch(sp_session, name) -> dict:
    """
    Returns a resource's configuration.
    """
    fqn = parse_identifier(name)
    urn = None
    return dp.fetch_resource(sp_session.connection, urn)
    # if fqn.resource_type == "schema":
    #     return fetch_schema(sp_session, name)
    # elif fqn.resource_key == "database":
    #     return fetch_database(sp_session, name)
    # else:
    #     raise Exception(f"Unsupported resource type: {fqn.resource_key}")


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
