import os

# from snowflake.snowpark import Session
import snowflake.connector
from snowflake.connector import DictCursor

from snowflake.connector.errors import ProgrammingError

from .identifiers import FQN

connection_params = {
    "account": os.environ["SNOWFLAKE_ACCOUNT"],
    "user": os.environ["SNOWFLAKE_USER"],
    "password": os.environ["SNOWFLAKE_PASSWORD"],
}


def get_session():
    # TODO: make this snowpark-compatible
    # return Session.builder.configs(**connection_params).create()
    return snowflake.connector.connect(**connection_params)


def execute(session, sql) -> list:
    try:
        res = session.cursor(DictCursor).execute(sql).fetchall()
        return res
    except ProgrammingError as err:
        print(f"failed to execute sql, [{sql}]")
        raise err


def params_result_to_dict(params_result):
    params = {}
    for param in params_result:
        if param["type"] == "BOOLEAN":
            typed_value = param["value"] == "true"
        elif param["type"] == "NUMBER":
            typed_value = int(param["value"])
        elif param["type"] == "STRING":
            typed_value = str(param["value"]) if param["value"] else None
        params[param["key"].lower()] = typed_value
    return params


def options_result_to_list(options_result):
    return [option.strip(" ") for option in options_result.split(",")]


class Adapter:
    def __init__(self, session=None):
        self.session = session or get_session()

    def fetch_resource(self, urn):
        return getattr(self, f"fetch_{urn.resource_key}")(urn.fqn)

    def fetch_account_locator(self):
        locator = execute(self.session, "SELECT CURRENT_ACCOUNT()")[0]
        return locator

    def fetch_region(self):
        region = execute(self.session, "SELECT CURRENT_REGION()")[0]
        return region

    def fetch_columns(self, resource_type: str, fqn: FQN):
        desc_result = execute(self.session, f"DESC {resource_type} {fqn}")
        columns = []
        for col in desc_result:
            if col["kind"] != "COLUMN":
                raise Exception(f"Unexpected kind {col['kind']} in desc result")
            columns.append(
                {
                    "name": col["name"],
                    "type": col["type"],
                    "nullable": col["null?"] == "Y",
                    "default": col["default"],
                    "comment": col["comment"],
                }
            )
        return columns

    def fetch_database(self, fqn: FQN):
        show_result = execute(self.session, f"SHOW DATABASES LIKE '{fqn.name}'")

        if len(show_result) == 0:
            return {}
        if len(show_result) > 1:
            raise Exception(f"Found multiple databases matching {fqn}")
        if show_result[0]["kind"] != "STANDARD":
            return {}

        options = options_result_to_list(show_result[0]["options"])
        show_params_result = execute(self.session, f"SHOW PARAMETERS IN DATABASE {fqn.name}")
        params = params_result_to_dict(show_params_result)

        return {
            "name": show_result[0]["name"],
            "data_retention_time_in_days": int(show_result[0]["retention_time"]),
            "comment": show_result[0]["comment"] or None,
            "transient": "TRANSIENT" in options,
            "owner": show_result[0]["owner"],
            "max_data_extension_time_in_days": params["data_retention_time_in_days"],
            "default_ddl_collation": params["default_ddl_collation"],
        }

    # def fetch_role(self, fqn):
    #     role = {}
    #     with self.session.cursor(DictCursor) as cur:
    #         for show_role_response in cur.execute(f"SHOW ROLES LIKE '{fqn}'").fetchall():
    #             role.update(
    #                 {
    #                     "name": show_role_response["name"],
    #                     "owner": show_role_response["owner"],
    #                     "comment": show_role_response["comment"],
    #                 }
    #             )
    #     return role

    def fetch_schema(self, fqn):
        if fqn.database is None:
            raise Exception(f"Schema fqn must have a database {fqn}")
        try:
            show_result = execute(self.session, f"SHOW SCHEMAS LIKE '{fqn.name}' IN DATABASE {fqn.database}")
        except ProgrammingError:
            return {}

        if len(show_result) == 0:
            return {}
        if len(show_result) > 1:
            raise Exception(f"Found multiple schemas matching {fqn}")

        options = options_result_to_list(show_result[0]["options"])
        show_params_result = execute(self.session, f"SHOW PARAMETERS IN SCHEMA {fqn}")
        params = params_result_to_dict(show_params_result)

        return {
            "name": show_result[0]["name"],
            "transient": "TRANSIENT" in options,
            "owner": show_result[0]["owner"],
            "with_managed_access": "MANAGED ACCESS" in options,
            "data_retention_time_in_days": int(show_result[0]["retention_time"]),
            "max_data_extension_time_in_days": params["data_retention_time_in_days"],
            "default_ddl_collation": params["default_ddl_collation"],
            "comment": show_result[0]["comment"] or None,
        }

    def fetch_view(self, fqn):
        if fqn.schema is None:
            raise Exception(f"View fqn must have a schema {fqn}")
        try:
            show_result = execute(self.session, f"SHOW VIEWS LIKE '{fqn.name}' IN SCHEMA {fqn.database}.{fqn.schema}")
        except ProgrammingError:
            return {}

        if len(show_result) == 0:
            return {}
        if len(show_result) > 1:
            raise Exception(f"Found multiple views matching {fqn}")

        if show_result[0]["is_materialized"] == "true":
            return {}

        columns = self.fetch_columns("VIEW", fqn)

        return {
            "name": show_result[0]["name"],
            "owner": show_result[0]["owner"],
            "secure": show_result[0]["is_secure"] == "true",
            "columns": columns,
            "change_tracking": show_result[0]["change_tracking"] == "ON",
            "comment": show_result[0]["comment"] or None,
            "as_": show_result[0]["text"],
        }

        # with self.session.cursor() as cur:
        #     ) in cur.execute(f"SHOW VIEWS LIKE '{fqn}'").fetchall():
        #         if is_materialized == "true":
        #             return {}
        #         else:
        #             view.update(

        #             )
        # return view
