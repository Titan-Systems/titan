from .helpers import listify
from .enums import ParseableEnum, ResourceType
from .identifiers import URN


class Privs:
    def __init__(self, create=None, read=None, write=None, delete=None):
        self.create = listify(create)
        self.read = listify(read)
        self.write = listify(write)
        self.delete = listify(delete)


class GlobalPriv(ParseableEnum):
    ALL = "ALL"
    APPLY_AUTHENTICATION_POLICY = "APPLY AUTHENTICATION POLICY"
    APPLY_MASKING_POLICY = "APPLY MASKING POLICY"
    APPLY_PASSWORD_POLICY = "APPLY PASSWORD POLICY"
    APPLY_RESOURCE_GROUP = "APPLY RESOURCE GROUP"
    APPLY_ROW_ACCESS_POLICY = "APPLY ROW ACCESS POLICY"
    APPLY_SESSION_POLICY = "APPLY SESSION POLICY"
    APPLY_TAG = "APPLY TAG"
    ATTACH_POLICY = "ATTACH POLICY"
    AUDIT = "AUDIT"
    BIND_SERVICE_ENDPOINT = "BIND SERVICE ENDPOINT"
    CANCEL_QUERY = "CANCEL QUERY"
    CREATE_ACCOUNT = "CREATE ACCOUNT"
    CREATE_API_INTEGRATION = "CREATE API INTEGRATION"
    CREATE_APPLICATION = "CREATE APPLICATION"
    CREATE_APPLICATION_PACKAGE = "CREATE APPLICATION PACKAGE"
    CREATE_COMPUTE_POOL = "CREATE COMPUTE POOL"
    CREATE_CREDENTIAL = "CREATE CREDENTIAL"
    CREATE_DATA_EXCHANGE_LISTING = "CREATE DATA EXCHANGE LISTING"
    CREATE_DATABASE = "CREATE DATABASE"
    CREATE_FAILOVER_GROUP = "CREATE FAILOVER GROUP"
    CREATE_INTEGRATION = "CREATE INTEGRATION"
    CREATE_NETWORK_POLICY = "CREATE NETWORK POLICY"
    CREATE_REPLICATION_GROUP = "CREATE REPLICATION GROUP"
    CREATE_ROLE = "CREATE ROLE"
    CREATE_SHARE = "CREATE SHARE"
    CREATE_USER = "CREATE USER"
    CREATE_WAREHOUSE = "CREATE WAREHOUSE"
    EXECUTE_ALERT = "EXECUTE ALERT"
    EXECUTE_DATA_METRIC_FUNCTION = "EXECUTE DATA METRIC FUNCTION"
    EXECUTE_MANAGED_TASK = "EXECUTE MANAGED TASK"
    EXECUTE_TASK = "EXECUTE TASK"
    IMPORT_SHARE = "IMPORT SHARE"
    MANAGE_ACCOUNT_SUPPORT_CASES = "MANAGE ACCOUNT SUPPORT CASES"
    MANAGE_EVENT_SHARING = "MANAGE EVENT SHARING"
    MANAGE_GRANTS = "MANAGE GRANTS"
    MANAGE_USER_SUPPORT_CASES = "MANAGE USER SUPPORT CASES"
    MANAGE_WAREHOUSES = "MANAGE WAREHOUSES"
    MODIFY_LOG_LEVEL = "MODIFY LOG LEVEL"
    MODIFY_SESSION_LOG_LEVEL = "MODIFY SESSION LOG LEVEL"
    MODIFY_SESSION_TRACE_LEVEL = "MODIFY SESSION TRACE LEVEL"
    MODIFY_TRACE_LEVEL = "MODIFY TRACE LEVEL"
    MONITOR = "MONITOR"
    MONITOR_EXECUTION = "MONITOR EXECUTION"
    MONITOR_SECURITY = "MONITOR SECURITY"
    MONITOR_USAGE = "MONITOR USAGE"
    OVERRIDE_SHARE_RESTRICTIONS = "OVERRIDE SHARE RESTRICTIONS"
    PURCHASE_DATA_EXCHANGE_LISTING = "PURCHASE DATA EXCHANGE LISTING"
    RESOLVE_ALL = "RESOLVE ALL"


GLOBAL_PRIV_DEFAULT_OWNERS = {
    GlobalPriv.APPLY_MASKING_POLICY: "ACCOUNTADMIN",
    GlobalPriv.APPLY_PASSWORD_POLICY: "SECURITYADMIN",
    # GlobalPriv.APPLY_ROW_ACCESS_POLICY: "SECURITYADMIN",
    GlobalPriv.APPLY_SESSION_POLICY: "SECURITYADMIN",
    # GlobalPriv.APPLY_TAG: "ACCOUNTADMIN",
    GlobalPriv.ATTACH_POLICY: "SECURITYADMIN",
    GlobalPriv.AUDIT: "ACCOUNTADMIN",
    GlobalPriv.CREATE_ACCOUNT: "ACCOUNTADMIN",
    GlobalPriv.CREATE_DATA_EXCHANGE_LISTING: "ACCOUNTADMIN",
    GlobalPriv.CREATE_DATABASE: "SYSADMIN",
    GlobalPriv.CREATE_FAILOVER_GROUP: "ACCOUNTADMIN",
    GlobalPriv.CREATE_INTEGRATION: "ACCOUNTADMIN",
    GlobalPriv.CREATE_NETWORK_POLICY: "SECURITYADMIN",
    GlobalPriv.CREATE_REPLICATION_GROUP: "ACCOUNTADMIN",
    GlobalPriv.CREATE_ROLE: "USERADMIN",
    GlobalPriv.CREATE_SHARE: "ACCOUNTADMIN",
    GlobalPriv.CREATE_USER: "USERADMIN",
    GlobalPriv.CREATE_WAREHOUSE: "SYSADMIN",
    GlobalPriv.EXECUTE_ALERT: "ACCOUNTADMIN",
    GlobalPriv.EXECUTE_TASK: "ACCOUNTADMIN",
    GlobalPriv.IMPORT_SHARE: "ACCOUNTADMIN",
    GlobalPriv.MANAGE_ACCOUNT_SUPPORT_CASES: "ACCOUNTADMIN",
    GlobalPriv.MANAGE_GRANTS: "SECURITYADMIN",
    GlobalPriv.MANAGE_WAREHOUSES: "ACCOUNTADMIN",
    # GlobalPriv.MODIFY_LOG_LEVEL: "ACCOUNTADMIN",
    # GlobalPriv.MODIFY_SESSION_LOG_LEVEL: "ACCOUNTADMIN",
    # GlobalPriv.MODIFY_SESSION_TRACE_LEVEL: "ACCOUNTADMIN",
    # GlobalPriv.MODIFY_TRACE_LEVEL: "ACCOUNTADMIN",
    GlobalPriv.MONITOR: "ACCOUNTADMIN",
    GlobalPriv.MONITOR_EXECUTION: "ACCOUNTADMIN",
    GlobalPriv.MONITOR_SECURITY: "ACCOUNTADMIN",
    GlobalPriv.MONITOR_USAGE: "ACCOUNTADMIN",
    GlobalPriv.OVERRIDE_SHARE_RESTRICTIONS: "ACCOUNTADMIN",
    GlobalPriv.RESOLVE_ALL: "ACCOUNTADMIN",
}


class DatabasePriv(ParseableEnum):
    ALL = "ALL"
    CREATE_DATABASE_ROLE = "CREATE DATABASE ROLE"
    CREATE_SCHEMA = "CREATE SCHEMA"
    IMPORTED_PRIVILEGES = "IMPORTED PRIVILEGES"
    MODIFY = "MODIFY"
    MONITOR = "MONITOR"
    OWNERSHIP = "OWNERSHIP"
    REFERENCE_USAGE = "REFERENCE_USAGE"
    USAGE = "USAGE"


class IntegrationPriv(ParseableEnum):
    ALL = "ALL"
    USAGE = "USAGE"
    USE_ANY_ROLE = "USE_ANY_ROLE"
    OWNERSHIP = "OWNERSHIP"


class NetworkRulePriv(ParseableEnum):
    OWNERSHIP = "OWNERSHIP"


class PackagesPolicyPriv(ParseableEnum):
    OWNERSHIP = "OWNERSHIP"
    USAGE = "USAGE"


class PasswordPolicyPriv(ParseableEnum):
    OWNERSHIP = "OWNERSHIP"


class ProcedurePriv(ParseableEnum):
    ALL = "ALL"
    OWNERSHIP = "OWNERSHIP"
    USAGE = "USAGE"


class RolePriv(ParseableEnum):
    OWNERSHIP = "OWNERSHIP"
    USAGE = "USAGE"


class SchemaPriv(ParseableEnum):
    ALL = "ALL"
    ADD_SEARCH_OPTIMIZATION = "ADD SEARCH OPTIMIZATION"
    CREATE_ALERT = "CREATE ALERT"
    CREATE_EXTERNAL_TABLE = "CREATE EXTERNAL TABLE"
    CREATE_FILE_FORMAT = "CREATE FILE FORMAT"
    CREATE_FUNCTION = "CREATE FUNCTION"
    CREATE_MASKING_POLICY = "CREATE MASKING POLICY"
    CREATE_MATERIALIZED_VIEW = "CREATE MATERIALIZED VIEW"
    CREATE_NETWORK_RULE = "CREATE NETWORK RULE"
    CREATE_PACKAGES_POLICY = "CREATE PACKAGES POLICY"
    CREATE_PASSWORD_POLICY = "CREATE PASSWORD POLICY"
    CREATE_PIPE = "CREATE PIPE"
    CREATE_PROCEDURE = "CREATE PROCEDURE"
    CREATE_ROW_ACCESS_POLICY = "CREATE ROW ACCESS POLICY"
    CREATE_SECRET = "CREATE SECRET"
    CREATE_SEQUENCE = "CREATE SEQUENCE"
    CREATE_SESSION_POLICY = "CREATE SESSION POLICY"
    CREATE_SNOWFLAKE_ML_ANOMALY_DETECTION = "CREATE SNOWFLAKE.ML.ANOMALY_DETECTION"
    CREATE_SNOWFLAKE_ML_FORECAST = "CREATE SNOWFLAKE.ML.FORECAST"
    CREATE_STAGE = "CREATE STAGE"
    CREATE_STREAM = "CREATE STREAM"
    CREATE_TABLE = "CREATE TABLE"
    CREATE_TAG = "CREATE TAG"
    CREATE_TASK = "CREATE TASK"
    CREATE_VIEW = "CREATE VIEW"
    MODIFY = "MODIFY"
    MONITOR = "MONITOR"
    OWNERSHIP = "OWNERSHIP"
    USAGE = "USAGE"


class StagePriv(ParseableEnum):
    ALL = "ALL"
    OWNERSHIP = "OWNERSHIP"
    READ = "READ"
    USAGE = "USAGE"
    WRITE = "WRITE"


class TablePriv(ParseableEnum):
    ALL = "ALL"
    DELETE = "DELETE"
    INSERT = "INSERT"
    OWNERSHIP = "OWNERSHIP"
    REFERENCES = "REFERENCES"
    SELECT = "SELECT"
    TRUNCATE = "TRUNCATE"
    UPDATE = "UPDATE"


class UserPriv(ParseableEnum):
    ALL = "ALL"
    MONITOR = "MONITOR"
    OWNERSHIP = "OWNERSHIP"


class ViewPriv(ParseableEnum):
    ALL = "ALL"
    OWNERSHIP = "OWNERSHIP"
    REFERENCES = "REFERENCES"
    SELECT = "SELECT"


class WarehousePriv(ParseableEnum):
    ALL = "ALL"
    MODIFY = "MODIFY"
    MONITOR = "MONITOR"
    OPERATE = "OPERATE"
    OWNERSHIP = "OWNERSHIP"
    USAGE = "USAGE"


RESOURCE_GRANTS_MAP = {
    ResourceType.ACCOUNT: GlobalPriv,
    ResourceType.DATABASE: DatabasePriv,
    ResourceType.EXTERNAL_ACCESS_INTEGRATION: IntegrationPriv,
    ResourceType.NETWORK_RULE: NetworkRulePriv,
    ResourceType.PASSWORD_POLICY: PasswordPolicyPriv,
    ResourceType.PACKAGES_POLICY: PackagesPolicyPriv,
    ResourceType.PROCEDURE: ProcedurePriv,
    ResourceType.ROLE: RolePriv,
    ResourceType.SCHEMA: SchemaPriv,
    ResourceType.STAGE: StagePriv,
    ResourceType.TABLE: TablePriv,
    ResourceType.USER: UserPriv,
    ResourceType.VIEW: ViewPriv,
    ResourceType.WAREHOUSE: WarehousePriv,
}


def priv_for_principal(principal: URN, priv: str):
    if principal.resource_type not in RESOURCE_GRANTS_MAP:
        raise Exception(f"Unsupported resource type: {principal.resource_type}")
    return RESOURCE_GRANTS_MAP[principal.resource_type](priv)


def create_priv_for_resource_type(resource_type):
    if resource_type == "database":
        return GlobalPriv.CREATE_DATABASE
    elif resource_type == "schema":
        return DatabasePriv.CREATE_SCHEMA
    elif resource_type == "table":
        return SchemaPriv.CREATE_TABLE
    elif resource_type == "view":
        return SchemaPriv.CREATE_VIEW
    elif resource_type == "procedure":
        return SchemaPriv.CREATE_PROCEDURE
    return None


def is_ownership_priv(priv):
    return str(priv) == "OWNERSHIP"
