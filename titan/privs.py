from dataclasses import dataclass

from .enums import ParseableEnum, ResourceType


class Priv(ParseableEnum):
    pass


@dataclass
class GrantedPrivilege:
    privilege: Priv
    on: str  # probably should be FQN

    @classmethod
    def from_grant(cls, privilege: str, granted_on: str, name: str):
        resource_type = ResourceType(granted_on)
        priv_type = PRIVS_FOR_RESOURCE_TYPE[resource_type]
        if priv_type:
            priv = priv_type(privilege)
            return cls(privilege=priv, on=name)
        else:
            return cls(privilege=privilege, on=granted_on)


class AccountPriv(Priv):
    ALL = "ALL"
    APPLY_AGGREGATION_POLICY = "APPLY AGGREGATION POLICY"
    APPLY_AUTHENTICATION_POLICY = "APPLY AUTHENTICATION POLICY"
    APPLY_MASKING_POLICY = "APPLY MASKING POLICY"
    APPLY_PACKAGES_POLICY = "APPLY PACKAGES POLICY"
    APPLY_PASSWORD_POLICY = "APPLY PASSWORD POLICY"
    APPLY_PROJECTION_POLICY = "APPLY PROJECTION POLICY"
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
    CREATE_EXTERNAL_VOLUME = "CREATE EXTERNAL VOLUME"
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
    EXECUTE_MANAGED_ALERT = "EXECUTE MANAGED ALERT"
    EXECUTE_MANAGED_TASK = "EXECUTE MANAGED TASK"
    EXECUTE_TASK = "EXECUTE TASK"
    IMPORT_SHARE = "IMPORT SHARE"
    MANAGE_ACCOUNT_SUPPORT_CASES = "MANAGE ACCOUNT SUPPORT CASES"
    MANAGE_EVENT_SHARING = "MANAGE EVENT SHARING"
    MANAGE_GRANTS = "MANAGE GRANTS"
    MANAGE_USER_SUPPORT_CASES = "MANAGE USER SUPPORT CASES"
    MANAGE_WAREHOUSES = "MANAGE WAREHOUSES"
    MODIFY_LOG_LEVEL = "MODIFY LOG LEVEL"
    MODIFY_METRIC_LEVEL = "MODIFY METRIC LEVEL"
    MODIFY_SESSION_LOG_LEVEL = "MODIFY SESSION LOG LEVEL"
    MODIFY_SESSION_METRIC_LEVEL = "MODIFY SESSION METRIC LEVEL"
    MODIFY_SESSION_TRACE_LEVEL = "MODIFY SESSION TRACE LEVEL"
    MODIFY_TRACE_LEVEL = "MODIFY TRACE LEVEL"
    MONITOR = "MONITOR"
    MONITOR_EXECUTION = "MONITOR EXECUTION"
    MONITOR_SECURITY = "MONITOR SECURITY"
    MONITOR_USAGE = "MONITOR USAGE"
    OVERRIDE_SHARE_RESTRICTIONS = "OVERRIDE SHARE RESTRICTIONS"
    PURCHASE_DATA_EXCHANGE_LISTING = "PURCHASE DATA EXCHANGE LISTING"
    RESOLVE_ALL = "RESOLVE ALL"


class AlertPriv(ParseableEnum):
    ALL = "ALL"
    MONITOR = "MONITOR"
    OPERATE = "OPERATE"
    OWNERSHIP = "OWNERSHIP"


class DatabasePriv(ParseableEnum):
    ALL = "ALL"
    APPLYBUDGET = "APPLYBUDGET"
    CREATE_DATABASE_ROLE = "CREATE DATABASE ROLE"
    CREATE_SCHEMA = "CREATE SCHEMA"
    # IMPORTED_PRIVILEGES = "IMPORTED PRIVILEGES" # only granted on shared database
    MODIFY = "MODIFY"
    MONITOR = "MONITOR"
    OWNERSHIP = "OWNERSHIP"
    REFERENCE_USAGE = "REFERENCE_USAGE"  # Cannot be granted to roles, only shares
    USAGE = "USAGE"


class DatabaseRolePriv(ParseableEnum):
    OWNERSHIP = "OWNERSHIP"
    USAGE = "USAGE"


class DirectoryTablePriv(ParseableEnum):
    OWNERSHIP = "OWNERSHIP"


class EventTablePriv(ParseableEnum):
    ALL = "ALL"
    OWNERSHIP = "OWNERSHIP"
    SELECT = "SELECT"
    INSERT = "INSERT"


class ExternalVolumePriv(ParseableEnum):
    OWNERSHIP = "OWNERSHIP"
    USAGE = "USAGE"


class FailoverGroupPriv(ParseableEnum):
    ALL = "ALL"
    FAILOVER = "FAILOVER"
    MODIFY = "MODIFY"
    MONITOR = "MONITOR"
    OWNERSHIP = "OWNERSHIP"
    REPLICATE = "REPLICATE"


class FileFormatPriv(ParseableEnum):
    ALL = "ALL"
    OWNERSHIP = "OWNERSHIP"
    USAGE = "USAGE"


class FunctionPriv(ParseableEnum):
    ALL = "ALL"
    OWNERSHIP = "OWNERSHIP"
    USAGE = "USAGE"


class IntegrationPriv(ParseableEnum):
    ALL = "ALL"
    USAGE = "USAGE"
    USE_ANY_ROLE = "USE_ANY_ROLE"
    OWNERSHIP = "OWNERSHIP"


class MaterializedViewPriv(ParseableEnum):
    ALL = "ALL"
    APPLYBUDGET = "APPLYBUDGET"
    OWNERSHIP = "OWNERSHIP"
    REFERENCES = "REFERENCES"
    SELECT = "SELECT"


class NetworkPolicyPriv(ParseableEnum):
    OWNERSHIP = "OWNERSHIP"


class NetworkRulePriv(ParseableEnum):
    OWNERSHIP = "OWNERSHIP"


class NotebookPriv(ParseableEnum):
    OWNERSHIP = "OWNERSHIP"


class PackagesPolicyPriv(ParseableEnum):
    OWNERSHIP = "OWNERSHIP"
    USAGE = "USAGE"


class PasswordPolicyPriv(ParseableEnum):
    OWNERSHIP = "OWNERSHIP"


class PipePriv(ParseableEnum):
    ALL = "ALL"
    APPLYBUDGET = "APPLYBUDGET"
    MONITOR = "MONITOR"
    OPERATE = "OPERATE"
    OWNERSHIP = "OWNERSHIP"


class ProcedurePriv(ParseableEnum):
    ALL = "ALL"
    OWNERSHIP = "OWNERSHIP"
    USAGE = "USAGE"


class ReplicationGroupPriv(ParseableEnum):
    ALL = "ALL"
    MODIFY = "MODIFY"
    MONITOR = "MONITOR"
    OWNERSHIP = "OWNERSHIP"
    REPLICATE = "REPLICATE"


class RolePriv(ParseableEnum):
    OWNERSHIP = "OWNERSHIP"
    USAGE = "USAGE"


class SchemaPriv(ParseableEnum):
    ALL = "ALL"
    ADD_SEARCH_OPTIMIZATION = "ADD SEARCH OPTIMIZATION"
    APPLYBUDGET = "APPLYBUDGET"
    CREATE_AGGREGATION_POLICY = "CREATE AGGREGATION POLICY"
    CREATE_ALERT = "CREATE ALERT"
    CREATE_AUTHENTICATION_POLICY = "CREATE AUTHENTICATION POLICY"
    CREATE_CORTEX_SEARCH_SERVICE = "CREATE CORTEX SEARCH SERVICE"
    CREATE_DATASET = "CREATE DATASET"
    CREATE_DYNAMIC_TABLE = "CREATE DYNAMIC TABLE"
    CREATE_EVENT_TABLE = "CREATE EVENT TABLE"
    CREATE_EXTERNAL_TABLE = "CREATE EXTERNAL TABLE"
    CREATE_FILE_FORMAT = "CREATE FILE FORMAT"
    CREATE_FUNCTION = "CREATE FUNCTION"
    CREATE_GIT_REPOSITORY = "CREATE GIT REPOSITORY"
    CREATE_ICEBERG_TABLE = "CREATE ICEBERG TABLE"
    CREATE_IMAGE_REPOSITORY = "CREATE IMAGE REPOSITORY"
    CREATE_MASKING_POLICY = "CREATE MASKING POLICY"
    CREATE_MATERIALIZED_VIEW = "CREATE MATERIALIZED VIEW"
    CREATE_MODEL = "CREATE MODEL"
    CREATE_NETWORK_RULE = "CREATE NETWORK RULE"
    CREATE_NOTEBOOK = "CREATE NOTEBOOK"
    CREATE_PACKAGES_POLICY = "CREATE PACKAGES POLICY"
    CREATE_PASSWORD_POLICY = "CREATE PASSWORD POLICY"
    CREATE_PIPE = "CREATE PIPE"
    CREATE_PROCEDURE = "CREATE PROCEDURE"
    CREATE_PROJECTION_POLICY = "CREATE PROJECTION POLICY"
    CREATE_RESOURCE_GROUP = "CREATE RESOURCE GROUP"
    CREATE_ROW_ACCESS_POLICY = "CREATE ROW ACCESS POLICY"
    CREATE_SECRET = "CREATE SECRET"
    CREATE_SEQUENCE = "CREATE SEQUENCE"
    CREATE_SERVICE = "CREATE SERVICE"
    CREATE_SERVICE_CLASS = "CREATE SERVICE CLASS"
    CREATE_SESSION_POLICY = "CREATE SESSION POLICY"
    CREATE_SNAPSHOT = "CREATE SNAPSHOT"
    CREATE_STAGE = "CREATE STAGE"
    CREATE_STREAM = "CREATE STREAM"
    CREATE_STREAMLIT = "CREATE STREAMLIT"
    CREATE_TABLE = "CREATE TABLE"
    CREATE_TAG = "CREATE TAG"
    CREATE_TASK = "CREATE TASK"
    CREATE_TEMPORARY_TABLE = "CREATE TEMPORARY TABLE"
    CREATE_VIEW = "CREATE VIEW"
    MODIFY = "MODIFY"
    MONITOR = "MONITOR"
    OWNERSHIP = "OWNERSHIP"
    USAGE = "USAGE"


class SecretPriv(ParseableEnum):
    OWNERSHIP = "OWNERSHIP"
    READ = "READ"
    USAGE = "USAGE"


class SequencePriv(ParseableEnum):
    ALL = "ALL"
    OWNERSHIP = "OWNERSHIP"
    USAGE = "USAGE"


class StagePriv(ParseableEnum):
    ALL = "ALL"
    OWNERSHIP = "OWNERSHIP"
    READ = "READ"
    USAGE = "USAGE"
    WRITE = "WRITE"


class StreamPriv(ParseableEnum):
    ALL = "ALL"
    OWNERSHIP = "OWNERSHIP"
    SELECT = "SELECT"


class TablePriv(ParseableEnum):
    ALL = "ALL"
    APPLYBUDGET = "APPLYBUDGET"
    DELETE = "DELETE"
    EVOLVE_SCHEMA = "EVOLVE SCHEMA"
    INSERT = "INSERT"
    OWNERSHIP = "OWNERSHIP"
    REBUILD = "REBUILD"
    REFERENCES = "REFERENCES"
    SELECT = "SELECT"
    TRUNCATE = "TRUNCATE"
    UPDATE = "UPDATE"


class TagPriv(ParseableEnum):
    APPLY = "APPLY"
    OWNERSHIP = "OWNERSHIP"
    READ = "READ"


class TaskPriv(ParseableEnum):
    ALL = "ALL"
    APPLYBUDGET = "APPLYBUDGET"
    MONITOR = "MONITOR"
    OPERATE = "OPERATE"
    OWNERSHIP = "OWNERSHIP"


class UserPriv(ParseableEnum):
    ALL = "ALL"
    MONITOR = "MONITOR"
    OWNERSHIP = "OWNERSHIP"


class ViewPriv(ParseableEnum):
    ALL = "ALL"
    APPLYBUDGET = "APPLYBUDGET"
    DELETE = "DELETE"
    EVOLVE_SCHEMA = "EVOLVE SCHEMA"
    INSERT = "INSERT"
    OWNERSHIP = "OWNERSHIP"
    REBUILD = "REBUILD"
    REFERENCES = "REFERENCES"
    SELECT = "SELECT"
    TRUNCATE = "TRUNCATE"
    UPDATE = "UPDATE"


class WarehousePriv(ParseableEnum):
    ALL = "ALL"
    APPLYBUDGET = "APPLYBUDGET"
    MODIFY = "MODIFY"
    MONITOR = "MONITOR"
    OPERATE = "OPERATE"
    OWNERSHIP = "OWNERSHIP"
    USAGE = "USAGE"


PRIVS_FOR_RESOURCE_TYPE: dict[ResourceType, ParseableEnum] = {
    ResourceType.ACCOUNT: AccountPriv,
    ResourceType.AGGREGATION_POLICY: None,
    ResourceType.ALERT: AlertPriv,
    ResourceType.API_INTEGRATION: IntegrationPriv,
    ResourceType.APPLICATION_ROLE: None,
    ResourceType.AUTHENTICATION_POLICY: None,
    ResourceType.CATALOG_INTEGRATION: None,
    ResourceType.CLASS: None,
    ResourceType.COLUMN: None,
    ResourceType.COMPUTE_POOL: None,
    ResourceType.DATABASE_ROLE: DatabaseRolePriv,
    ResourceType.DATABASE: DatabasePriv,
    ResourceType.DIRECTORY_TABLE: DirectoryTablePriv,
    ResourceType.DYNAMIC_TABLE: TablePriv,
    ResourceType.EVENT_TABLE: EventTablePriv,
    ResourceType.EXTERNAL_ACCESS_INTEGRATION: IntegrationPriv,
    ResourceType.EXTERNAL_FUNCTION: FunctionPriv,
    ResourceType.EXTERNAL_VOLUME: ExternalVolumePriv,
    ResourceType.FAILOVER_GROUP: FailoverGroupPriv,
    ResourceType.FILE_FORMAT: FileFormatPriv,
    ResourceType.FUNCTION: FunctionPriv,
    ResourceType.FUTURE_GRANT: None,
    ResourceType.GIT_REPOSITORY: None,
    ResourceType.GRANT_ON_ALL: None,
    ResourceType.GRANT: None,
    ResourceType.HYBRID_TABLE: None,
    ResourceType.IMAGE_REPOSITORY: None,
    ResourceType.INTEGRATION: IntegrationPriv,
    ResourceType.MATERIALIZED_VIEW: MaterializedViewPriv,
    ResourceType.NETWORK_POLICY: NetworkPolicyPriv,
    ResourceType.NETWORK_RULE: NetworkRulePriv,
    ResourceType.NOTEBOOK: NotebookPriv,
    ResourceType.NOTIFICATION_INTEGRATION: IntegrationPriv,
    ResourceType.PACKAGES_POLICY: PackagesPolicyPriv,
    ResourceType.PASSWORD_POLICY: PasswordPolicyPriv,
    ResourceType.PIPE: PipePriv,
    ResourceType.PROCEDURE: ProcedurePriv,
    ResourceType.REPLICATION_GROUP: ReplicationGroupPriv,
    ResourceType.RESOURCE_MONITOR: None,
    ResourceType.ROLE_GRANT: None,
    ResourceType.ROLE: RolePriv,
    ResourceType.SCHEMA: SchemaPriv,
    ResourceType.SECRET: SecretPriv,
    ResourceType.SECURITY_INTEGRATION: IntegrationPriv,
    ResourceType.SEQUENCE: SequencePriv,
    ResourceType.SERVICE: None,
    ResourceType.SHARE: None,
    ResourceType.STAGE: StagePriv,
    ResourceType.STORAGE_INTEGRATION: IntegrationPriv,
    ResourceType.STREAM: StreamPriv,
    ResourceType.TABLE: TablePriv,
    ResourceType.TAG_REFERENCE: None,
    ResourceType.TAG: TagPriv,
    ResourceType.TASK: TaskPriv,
    ResourceType.USER: UserPriv,
    ResourceType.VIEW: ViewPriv,
    ResourceType.WAREHOUSE: WarehousePriv,
}


CREATE_PRIV_FOR_RESOURCE_TYPE: dict[ResourceType, ParseableEnum] = {
    ResourceType.ACCOUNT: AccountPriv.CREATE_ACCOUNT,
    ResourceType.ALERT: SchemaPriv.CREATE_ALERT,
    ResourceType.API_INTEGRATION: AccountPriv.CREATE_API_INTEGRATION,
    ResourceType.DATABASE: AccountPriv.CREATE_DATABASE,
    ResourceType.DYNAMIC_TABLE: SchemaPriv.CREATE_DYNAMIC_TABLE,
    ResourceType.EVENT_TABLE: SchemaPriv.CREATE_TABLE,
    ResourceType.EXTERNAL_ACCESS_INTEGRATION: AccountPriv.CREATE_INTEGRATION,
    ResourceType.EXTERNAL_FUNCTION: SchemaPriv.CREATE_FUNCTION,
    ResourceType.FAILOVER_GROUP: AccountPriv.CREATE_FAILOVER_GROUP,
    ResourceType.FILE_FORMAT: SchemaPriv.CREATE_FILE_FORMAT,
    ResourceType.FUNCTION: SchemaPriv.CREATE_FUNCTION,
    # ResourceType.GRANT: AccountPriv.CREATE_GRANT,
    ResourceType.MATERIALIZED_VIEW: SchemaPriv.CREATE_MATERIALIZED_VIEW,
    ResourceType.NETWORK_POLICY: AccountPriv.CREATE_NETWORK_POLICY,
    ResourceType.NETWORK_RULE: SchemaPriv.CREATE_NETWORK_RULE,
    ResourceType.PACKAGES_POLICY: SchemaPriv.CREATE_PACKAGES_POLICY,
    ResourceType.PASSWORD_POLICY: SchemaPriv.CREATE_PASSWORD_POLICY,
    ResourceType.PIPE: SchemaPriv.CREATE_PIPE,
    ResourceType.PROCEDURE: SchemaPriv.CREATE_PROCEDURE,
    # ResourceType.RESOURCE_MONITOR: AccountPriv.CREATE_RESOURCE_MONITOR, # only ACCOUNTADMIN
    ResourceType.REPLICATION_GROUP: AccountPriv.CREATE_REPLICATION_GROUP,
    ResourceType.ROLE: AccountPriv.CREATE_ROLE,
    # ResourceType.ROLE_GRANT: RolePriv.OWNERSHIP,
    ResourceType.SCHEMA: DatabasePriv.CREATE_SCHEMA,
    ResourceType.SECRET: SchemaPriv.CREATE_SECRET,
    ResourceType.SEQUENCE: SchemaPriv.CREATE_SEQUENCE,
    ResourceType.STAGE: SchemaPriv.CREATE_STAGE,
    ResourceType.STREAM: SchemaPriv.CREATE_STREAM,
    ResourceType.TABLE: SchemaPriv.CREATE_TABLE,
    ResourceType.TAG: SchemaPriv.CREATE_TAG,
    ResourceType.TASK: SchemaPriv.CREATE_TASK,
    ResourceType.USER: AccountPriv.CREATE_USER,
    ResourceType.VIEW: SchemaPriv.CREATE_VIEW,
    ResourceType.WAREHOUSE: AccountPriv.CREATE_WAREHOUSE,
}


GLOBAL_PRIV_DEFAULT_OWNERS = {
    AccountPriv.APPLY_AGGREGATION_POLICY: "ACCOUNTADMIN",
    AccountPriv.APPLY_AUTHENTICATION_POLICY: "SECURITYADMIN",
    AccountPriv.APPLY_MASKING_POLICY: "ACCOUNTADMIN",
    AccountPriv.APPLY_PACKAGES_POLICY: "SECURITYADMIN",
    AccountPriv.APPLY_PASSWORD_POLICY: "SECURITYADMIN",
    AccountPriv.APPLY_PROJECTION_POLICY: "ACCOUNTADMIN",
    AccountPriv.APPLY_RESOURCE_GROUP: "ACCOUNTADMIN",
    AccountPriv.APPLY_ROW_ACCESS_POLICY: "ACCOUNTADMIN",
    AccountPriv.APPLY_SESSION_POLICY: "SECURITYADMIN",
    AccountPriv.APPLY_TAG: "ACCOUNTADMIN",
    AccountPriv.ATTACH_POLICY: "SECURITYADMIN",
    AccountPriv.AUDIT: "ACCOUNTADMIN",
    AccountPriv.BIND_SERVICE_ENDPOINT: "SECURITYADMIN",
    AccountPriv.CANCEL_QUERY: "ACCOUNTADMIN",
    AccountPriv.CREATE_ACCOUNT: "ACCOUNTADMIN",
    AccountPriv.CREATE_API_INTEGRATION: "ACCOUNTADMIN",
    AccountPriv.CREATE_APPLICATION_PACKAGE: "ACCOUNTADMIN",
    AccountPriv.CREATE_APPLICATION: "ACCOUNTADMIN",
    AccountPriv.CREATE_COMPUTE_POOL: "SYSADMIN",
    AccountPriv.CREATE_CREDENTIAL: "ACCOUNTADMIN",
    AccountPriv.CREATE_DATA_EXCHANGE_LISTING: "ACCOUNTADMIN",
    AccountPriv.CREATE_DATABASE: "SYSADMIN",
    AccountPriv.CREATE_EXTERNAL_VOLUME: "ACCOUNTADMIN",
    AccountPriv.CREATE_FAILOVER_GROUP: "ACCOUNTADMIN",
    AccountPriv.CREATE_INTEGRATION: "ACCOUNTADMIN",
    AccountPriv.CREATE_NETWORK_POLICY: "SECURITYADMIN",
    AccountPriv.CREATE_REPLICATION_GROUP: "ACCOUNTADMIN",
    AccountPriv.CREATE_ROLE: "USERADMIN",
    AccountPriv.CREATE_SHARE: "ACCOUNTADMIN",
    AccountPriv.CREATE_USER: "USERADMIN",
    AccountPriv.CREATE_WAREHOUSE: "SYSADMIN",
    AccountPriv.EXECUTE_ALERT: "ACCOUNTADMIN",
    AccountPriv.EXECUTE_DATA_METRIC_FUNCTION: "ACCOUNTADMIN",
    AccountPriv.EXECUTE_MANAGED_ALERT: "ACCOUNTADMIN",
    AccountPriv.EXECUTE_MANAGED_TASK: "ACCOUNTADMIN",
    AccountPriv.EXECUTE_TASK: "ACCOUNTADMIN",
    AccountPriv.IMPORT_SHARE: "ACCOUNTADMIN",
    AccountPriv.MANAGE_ACCOUNT_SUPPORT_CASES: "ACCOUNTADMIN",
    AccountPriv.MANAGE_EVENT_SHARING: "ACCOUNTADMIN",
    AccountPriv.MANAGE_GRANTS: "SECURITYADMIN",
    AccountPriv.MANAGE_USER_SUPPORT_CASES: "ACCOUNTADMIN",
    AccountPriv.MANAGE_WAREHOUSES: "ACCOUNTADMIN",
    AccountPriv.MODIFY_LOG_LEVEL: "ACCOUNTADMIN",
    AccountPriv.MODIFY_METRIC_LEVEL: "ACCOUNTADMIN",
    AccountPriv.MODIFY_SESSION_LOG_LEVEL: "ACCOUNTADMIN",
    AccountPriv.MODIFY_SESSION_METRIC_LEVEL: "ACCOUNTADMIN",
    AccountPriv.MODIFY_SESSION_TRACE_LEVEL: "ACCOUNTADMIN",
    AccountPriv.MODIFY_TRACE_LEVEL: "ACCOUNTADMIN",
    AccountPriv.MONITOR_EXECUTION: "ACCOUNTADMIN",
    AccountPriv.MONITOR_SECURITY: "ACCOUNTADMIN",
    AccountPriv.MONITOR_USAGE: "ACCOUNTADMIN",
    AccountPriv.MONITOR: "ACCOUNTADMIN",
    AccountPriv.OVERRIDE_SHARE_RESTRICTIONS: "ACCOUNTADMIN",
    AccountPriv.PURCHASE_DATA_EXCHANGE_LISTING: "ACCOUNTADMIN",
    AccountPriv.RESOLVE_ALL: "ACCOUNTADMIN",
}


def is_ownership_priv(priv):
    return str(priv) == "OWNERSHIP"


def _all_privs_for_resource_type(resource_type):
    all_privs = []
    for priv in PRIVS_FOR_RESOURCE_TYPE[resource_type]:
        priv = str(priv)
        if priv != "ALL" and priv != "OWNERSHIP":
            all_privs.append(priv)
    return all_privs


def execution_role_for_priv(priv: str):
    try:
        priv = AccountPriv(priv)
    except ValueError:
        return None
    return GLOBAL_PRIV_DEFAULT_OWNERS.get(priv)
