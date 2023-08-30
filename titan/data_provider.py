import json

from snowflake.connector.errors import ProgrammingError

from .client import get_session, execute
from .identifiers import URN, FQN

ACCESS_CONTROL_ERR = 3001
DOEST_NOT_EXIST_ERR = 2003


def _fail_if_not_granted(result, *args):
    if len(result) == 0:
        raise Exception("Failed to create grant")
    if len(result) == 1 and result[0]["status"] == "Grant not executed: Insufficient privileges.":
        raise Exception(result[0]["status"], *args)


def _filter_result(result, **kwargs):
    filtered = []
    for row in result:
        for key, value in kwargs.items():
            if row[key] != value:
                break
        else:
            filtered.append(row)
    return filtered


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


def remove_none_values(d):
    return {k: v for k, v in d.items() if v is not None}


def fetch_remote_state(provider: "DataProvider", manifest):
    state = {}
    for urn_str in manifest["_urns"]:
        urn = URN.from_str(urn_str)
        data = provider.fetch_resource(urn)
        # TODO: handle implicit and stub resources
        if urn_str in manifest and data:
            state[urn_str] = remove_none_values(data)

    return state


def update_list_for_changes_data(data):
    updates = []
    for attr, change in data.items():
        value = change[1]
        if isinstance(value, str):
            value = f"'{value}'"
        updates.append(f"{attr} = {value}")
    return updates


class DataProvider:
    def __init__(self, session=None):
        self.session = session or get_session()

    # region Resource Fetching

    def fetch_resource(self, urn):
        return getattr(self, f"fetch_{urn.resource_key}")(urn.fqn)

    def fetch_account_locator(self):
        locator = execute(self.session, "SELECT CURRENT_ACCOUNT()")[0]
        return locator

    def fetch_region(self):
        region = execute(self.session, "SELECT CURRENT_REGION()")[0]
        return region

    def fetch_account(self, fqn: FQN):
        show_result = execute(self.session, "SHOW ORGANIZATION ACCOUNTS", cacheable=True)
        accounts = _filter_result(show_result, name=fqn.name)
        if len(accounts) == 0:
            return None
        if len(accounts) > 1:
            raise Exception(f"Found multiple alerts matching {fqn}")
        data = accounts[0]
        return {
            "name": data["account_name"],
            "edition": data["edition"],
            # This column is only displayed for organizations that span multiple region groups.
            "region_group": data.get("region_group"),
            "region": data["snowflake_region"],
            "comment": data["comment"],
        }

    def fetch_alert(self, fqn: FQN):
        show_result = execute(self.session, "SHOW ALERTS", cacheable=True)
        alerts = _filter_result(show_result, name=fqn.name)
        if len(alerts) == 0:
            return None
        if len(alerts) > 1:
            raise Exception(f"Found multiple alerts matching {fqn}")
        data = alerts[0]
        return {
            "name": data["name"],
            "warehouse": data["warehouse"],
            "schedule": data["schedule"],
            "comment": data["comment"] or None,
            "condition": data["condition"],
            "then": data["action"],
            "owner": data["owner"],
        }

    def fetch_columns(self, resource_type: str, fqn: FQN):
        desc_result = execute(self.session, f"DESC {resource_type} {fqn}")
        columns = []
        for col in desc_result:
            if col["kind"] != "COLUMN":
                raise Exception(f"Unexpected kind {col['kind']} in desc result")
            columns.append(
                remove_none_values(
                    {
                        "name": col["name"],
                        "data_type": col["type"],
                        "nullable": col["null?"] == "Y",
                        "default": col["default"],
                        "comment": col["comment"] or None,
                    }
                )
            )
        return columns

    def fetch_database(self, fqn: FQN):
        show_result = execute(self.session, f"SHOW DATABASES LIKE '{fqn.name}'", cacheable=True)

        if len(show_result) == 0:
            return None
        if len(show_result) > 1:
            raise Exception(f"Found multiple databases matching {fqn}")
        if show_result[0]["kind"] != "STANDARD":
            return None

        options = options_result_to_list(show_result[0]["options"])
        show_params_result = execute(self.session, f"SHOW PARAMETERS IN DATABASE {fqn.name}")
        params = params_result_to_dict(show_params_result)

        return {
            "name": show_result[0]["name"],
            "data_retention_time_in_days": int(show_result[0]["retention_time"]),
            "comment": show_result[0]["comment"] or None,
            "transient": "TRANSIENT" in options,
            "owner": show_result[0]["owner"],
            "max_data_extension_time_in_days": params["max_data_extension_time_in_days"],
            "default_ddl_collation": params["default_ddl_collation"],
        }

    def fetch_account_grant(self, fqn: FQN):
        show_result = execute(self.session, "SHOW GRANTS ON ACCOUNT", cacheable=True)
        role_account_grants = _filter_result(show_result, grantee_name=fqn.name)

        if len(role_account_grants) == 0:
            return None

        return {
            "privs": sorted([row["privilege"] for row in role_account_grants]),
            "on": "ACCOUNT",
            "to": fqn.name,
        }

    def fetch_javascript_udf(self, fqn: FQN):
        show_result = execute(self.session, "SHOW USER FUNCTIONS IN ACCOUNT", cacheable=True)
        udfs = _filter_result(show_result, name=fqn.name)
        if len(udfs) == 0:
            return None
        if len(udfs) > 1:
            raise Exception(f"Found multiple roles matching {fqn}")

        data = udfs[0]
        inputs, output = data["arguments"].split(" RETURN ")
        desc_result = execute(self.session, f"DESC FUNCTION {inputs}", cacheable=True)
        properties = dict([(row["property"], row["value"]) for row in desc_result])

        return {
            "name": data["name"],
            "secure": data["is_secure"] == "Y",
            # "args": data["arguments"],
            "returns": output,
            "language": data["language"],
            "comment": None if data["description"] == "user-defined function" else data["description"],
            "volatility": properties["volatility"],
            "as_": properties["body"],
        }

    def fetch_priv_grant(self, fqn: FQN):
        raise NotImplementedError

    def fetch_role(self, fqn: FQN):
        show_result = execute(self.session, f"SHOW ROLES LIKE '{fqn.name}'", cacheable=True)

        if len(show_result) == 0:
            return None
        if len(show_result) > 1:
            raise Exception(f"Found multiple roles matching {fqn}")

        data = show_result[0]

        return {
            "name": data["name"],
            "comment": data["comment"] or None,
            "owner": data["owner"],
        }

    def fetch_role_grant(self, fqn: FQN):
        role, target = fqn.name.split("?")
        target_type, target_name = target.split("=")
        try:
            show_result = execute(self.session, f"SHOW GRANTS OF ROLE {role}", cacheable=True)
        except ProgrammingError as err:
            if err.errno == DOEST_NOT_EXIST_ERR:
                return None
            raise

        if len(show_result) == 0:
            return None

        for data in show_result:
            if data["granted_to"] == target_type.upper() and data["grantee_name"] == target_name:
                if data["granted_to"] == "ROLE":
                    return {"role": role, "to_role": data["grantee_name"], "owner": data["granted_by"]}
                elif data["granted_to"] == "USER":
                    return {"role": role, "to_user": data["grantee_name"], "owner": data["granted_by"]}
                else:
                    raise Exception(f"Unexpected role grant for role {fqn.name}")

        return None

    def fetch_schema(self, fqn: FQN):
        if fqn.database is None:
            raise Exception(f"Schema fqn must have a database {fqn}")
        try:
            show_result = execute(self.session, f"SHOW SCHEMAS LIKE '{fqn.name}' IN DATABASE {fqn.database}")
        except ProgrammingError:
            return None

        if len(show_result) == 0:
            return None
        if len(show_result) > 1:
            raise Exception(f"Found multiple schemas matching {fqn}")

        data = show_result[0]

        options = options_result_to_list(data["options"])
        show_params_result = execute(self.session, f"SHOW PARAMETERS IN SCHEMA {fqn}")
        params = params_result_to_dict(show_params_result)

        return {
            "name": data["name"],
            "transient": "TRANSIENT" in options,
            "owner": data["owner"],
            "with_managed_access": "MANAGED ACCESS" in options,
            "data_retention_time_in_days": int(data["retention_time"]),
            "max_data_extension_time_in_days": params["data_retention_time_in_days"],
            "default_ddl_collation": params["default_ddl_collation"],
            "comment": data["comment"] or None,
        }

    def fetch_shared_database(self, fqn: FQN):
        show_result = execute(self.session, "SELECT SYSTEM$SHOW_IMPORTED_DATABASES()", cacheable=True)
        show_result = json.loads(show_result[0]["SYSTEM$SHOW_IMPORTED_DATABASES()"])

        shares = _filter_result(show_result, name=fqn.name)

        if len(shares) == 0:
            return None
        if len(shares) > 1:
            raise Exception(f"Found multiple shares matching {fqn}")

        data = shares[0]
        return {
            "name": data["name"],
            "from_share": data["origin"],
        }

    def fetch_table(self, fqn: FQN):
        show_result = execute(self.session, "SHOW TABLES")

        tables = _filter_result(show_result, name=fqn.name, kind="TABLE")

        if len(tables) == 0:
            return None
        if len(tables) > 1:
            raise Exception(f"Found multiple tables matching {fqn}")

        columns = self.fetch_columns("TABLE", fqn)

        data = tables[0]
        return {
            "name": data["name"],
            "owner": data["owner"],
            "comment": data["comment"] or None,
            "cluster_by": data["cluster_by"] or None,
            "columns": columns,
        }

    def fetch_user(self, fqn: FQN):
        # SHOW USERS requires the MANAGE GRANTS privilege
        show_result = execute(self.session, "SHOW USERS", cacheable=True, use_role="SECURITYADMIN")

        users = _filter_result(show_result, name=fqn.name)

        if len(users) == 0:
            return None
        if len(users) > 1:
            raise Exception(f"Found multiple users matching {fqn}")

        data = users[0]

        return {
            "name": data["name"],
            "login_name": data["login_name"],
            "display_name": data["display_name"],
            "first_name": data["first_name"] or None,
            "last_name": data["last_name"] or None,
            "email": data["email"] or None,
            "mins_to_unlock": data["mins_to_unlock"] or None,
            "days_to_expiry": data["days_to_expiry"] or None,
            "comment": data["comment"] or None,
            "disabled": data["disabled"] == "true",
            "must_change_password": data["must_change_password"] == "true",
            "default_warehouse": data["default_warehouse"] or None,
            "default_namespace": data["default_namespace"] or None,
            "default_role": data["default_role"] or None,
            "default_secondary_roles": data["default_secondary_roles"] or None,
            "mins_to_bypass_mfa": data["mins_to_bypass_mfa"] or None,
            "owner": data["owner"],
        }

    def fetch_view(self, fqn: FQN):
        if fqn.schema is None:
            raise Exception(f"View fqn must have a schema {fqn}")
        try:
            show_result = execute(self.session, f"SHOW VIEWS LIKE '{fqn.name}' IN SCHEMA {fqn.database}.{fqn.schema}")
        except ProgrammingError:
            return None

        if len(show_result) == 0:
            return None
        if len(show_result) > 1:
            raise Exception(f"Found multiple views matching {fqn}")

        data = show_result[0]

        if data["is_materialized"] == "true":
            return None

        columns = self.fetch_columns("VIEW", fqn)

        return {
            "name": data["name"],
            "owner": data["owner"],
            "secure": data["is_secure"] == "true",
            "columns": columns,
            "change_tracking": data["change_tracking"] == "ON",
            "comment": data["comment"] or None,
            "as_": data["text"],
        }

    def fetch_warehouse(self, fqn: FQN):
        try:
            show_result = execute(self.session, f"SHOW WAREHOUSES LIKE '{fqn.name}'")
        except ProgrammingError:
            return None

        if len(show_result) == 0:
            return None
        if len(show_result) > 1:
            raise Exception(f"Found multiple warehouses matching {fqn}")

        data = show_result[0]

        show_params_result = execute(self.session, f"SHOW PARAMETERS FOR WAREHOUSE {fqn}")
        params = params_result_to_dict(show_params_result)

        query_accel = data.get("enable_query_acceleration")
        if query_accel:
            query_accel = query_accel == "true"

        return {
            "name": data["name"],
            "owner": data["owner"],
            "warehouse_type": data["type"],
            "warehouse_size": data["size"].upper(),
            # "max_cluster_count": data["max_cluster_count"],
            # "min_cluster_count": data["min_cluster_count"],
            # "scaling_policy": data["scaling_policy"],
            "auto_suspend": data["auto_suspend"],
            "auto_resume": data["auto_resume"] == "true",
            "comment": data["comment"] or None,
            "enable_query_acceleration": query_accel,
            "max_concurrency_level": params["max_concurrency_level"],
            "statement_queued_timeout_in_seconds": params["statement_queued_timeout_in_seconds"],
            "statement_timeout_in_seconds": params["statement_timeout_in_seconds"],
        }

    # endregion
