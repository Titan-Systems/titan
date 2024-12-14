from enum import Enum, EnumMeta


def _normalize_enum_value(value: str) -> str:
    return value.strip(" ").upper()


class _Parseable(EnumMeta):
    def __getitem__(self, name):
        if isinstance(name, str):
            name = _normalize_enum_value(name)
        return super().__getitem__(name)

    def __call__(cls, value, *args, **kw):
        if isinstance(value, str):
            value = _normalize_enum_value(value)
        return super().__call__(value, *args, **kw)

    def __contains__(self, child):
        return child in self.__members__


class ParseableEnum(str, Enum, metaclass=_Parseable):
    def __str__(self):
        return str(self.value)

    @classmethod
    def synonyms(cls):
        """Override to provide a dictionary of synonyms for the enum values."""
        return {}

    @classmethod
    def _missing_(cls, val):
        synonyms = cls.synonyms()
        if val in synonyms:
            return cls.__members__[synonyms[val]]
        return super()._missing_(val)


class BlueprintScope(ParseableEnum):
    ACCOUNT = "ACCOUNT"
    DATABASE = "DATABASE"
    SCHEMA = "SCHEMA"


class RunMode(ParseableEnum):
    CREATE_OR_UPDATE = "CREATE-OR-UPDATE"
    SYNC = "SYNC"


class ResourceType(ParseableEnum):
    ACCOUNT = "ACCOUNT"
    ACCOUNT_PARAMETER = "ACCOUNT PARAMETER"
    AGGREGATION_POLICY = "AGGREGATION POLICY"
    ALERT = "ALERT"
    API_INTEGRATION = "API INTEGRATION"
    APPLICATION_ROLE = "APPLICATION ROLE"
    AUTHENTICATION_POLICY = "AUTHENTICATION POLICY"
    CATALOG_INTEGRATION = "CATALOG INTEGRATION"
    CLASS = "CLASS"
    COLUMN = "COLUMN"
    COMPUTE_POOL = "COMPUTE POOL"
    DATABASE = "DATABASE"
    DATABASE_ROLE = "DATABASE ROLE"
    DATABASE_ROLE_GRANT = "DATABASE ROLE GRANT"
    DIRECTORY_TABLE = "DIRECTORY TABLE"
    DYNAMIC_TABLE = "DYNAMIC TABLE"
    EVENT_TABLE = "EVENT TABLE"
    EXTERNAL_ACCESS_INTEGRATION = "EXTERNAL ACCESS INTEGRATION"
    EXTERNAL_FUNCTION = "EXTERNAL FUNCTION"
    EXTERNAL_TABLE = "EXTERNAL TABLE"
    EXTERNAL_VOLUME = "EXTERNAL VOLUME"
    EXTERNAL_VOLUME_STORAGE_LOCATION = "EXTERNAL VOLUME STORAGE LOCATION"
    FAILOVER_GROUP = "FAILOVER GROUP"
    FILE_FORMAT = "FILE FORMAT"
    FUNCTION = "FUNCTION"
    FUTURE_GRANT = "FUTURE GRANT"
    GIT_REPOSITORY = "GIT REPOSITORY"
    GRANT = "GRANT"
    GRANT_ON_ALL = "GRANT ON ALL"
    HYBRID_TABLE = "HYBRID TABLE"
    ICEBERG_TABLE = "ICEBERG TABLE"
    IMAGE_REPOSITORY = "IMAGE REPOSITORY"
    INTEGRATION = "INTEGRATION"
    MASKING_POLICY = "MASKING POLICY"
    MATERIALIZED_VIEW = "MATERIALIZED VIEW"
    NETWORK_POLICY = "NETWORK POLICY"
    NETWORK_RULE = "NETWORK RULE"
    NOTEBOOK = "NOTEBOOK"
    NOTIFICATION_INTEGRATION = "NOTIFICATION INTEGRATION"
    PACKAGES_POLICY = "PACKAGES POLICY"
    PASSWORD_POLICY = "PASSWORD POLICY"
    PIPE = "PIPE"
    PROCEDURE = "PROCEDURE"
    REPLICATION_GROUP = "REPLICATION GROUP"
    RESOURCE_MONITOR = "RESOURCE MONITOR"
    ROLE = "ROLE"
    ROLE_GRANT = "ROLE GRANT"
    SCANNER_PACKAGE = "SCANNER PACKAGE"
    SCHEMA = "SCHEMA"
    SECRET = "SECRET"
    SECURITY_INTEGRATION = "SECURITY INTEGRATION"
    SEQUENCE = "SEQUENCE"
    SERVICE = "SERVICE"
    SHARE = "SHARE"
    STAGE = "STAGE"
    STORAGE_INTEGRATION = "STORAGE INTEGRATION"
    STREAM = "STREAM"
    TABLE = "TABLE"
    TAG = "TAG"
    TAG_REFERENCE = "TAG REFERENCE"
    TASK = "TASK"
    USER = "USER"
    VIEW = "VIEW"
    WAREHOUSE = "WAREHOUSE"


class Scope(ParseableEnum):
    ORGANIZATION = "ORGANIZATION"
    ACCOUNT = "ACCOUNT"
    DATABASE = "DATABASE"
    SCHEMA = "SCHEMA"
    TABLE = "TABLE"


class AccountEdition(ParseableEnum):
    STANDARD = "STANDARD"
    ENTERPRISE = "ENTERPRISE"
    BUSINESS_CRITICAL = "BUSINESS_CRITICAL"

    @classmethod
    def synonyms(cls):
        """Override to provide a dictionary of synonyms for the enum values"""
        return {"BUSINESS-CRITICAL": "BUSINESS_CRITICAL"}


class AccountCloud(ParseableEnum):
    AWS = "AWS"
    GCP = "GCP"
    AZURE = "AZURE"


class DataType(ParseableEnum):
    NUMBER = "NUMBER"
    DECIMAL = "DECIMAL"
    NUMERIC = "NUMERIC"
    INT = "INT"
    INTEGER = "INTEGER"
    BIGINT = "BIGINT"
    SMALLINT = "SMALLINT"
    TINYINT = "TINYINT"
    BYTEINT = "BYTEINT"
    FLOAT = "FLOAT"
    FLOAT4 = "FLOAT4"
    FLOAT8 = "FLOAT8"
    DOUBLE = "DOUBLE"
    DOUBLE_PRECISION = "DOUBLE PRECISION"
    REAL = "REAL"
    VARCHAR = "VARCHAR"
    CHAR = "CHAR"
    CHARACTER = "CHARACTER"
    NCHAR = "NCHAR"
    STRING = "STRING"
    TEXT = "TEXT"
    NVARCHAR = "NVARCHAR"
    NVARCHAR2 = "NVARCHAR2"
    CHAR_VARYING = "CHAR VARYING"
    NCHAR_VARYING = "NCHAR VARYING"
    BINARY = "BINARY"
    VARBINARY = "VARBINARY"
    BOOLEAN = "BOOLEAN"
    DATE = "DATE"
    DATETIME = "DATETIME"
    TIME = "TIME"
    TIMESTAMP = "TIMESTAMP"
    TIMESTAMP_LTZ = "TIMESTAMP_LTZ"
    TIMESTAMP_NTZ = "TIMESTAMP_NTZ"
    TIMESTAMP_TZ = "TIMESTAMP_TZ"
    ARRAY = "ARRAY"
    OBJECT = "OBJECT"
    VARIANT = "VARIANT"
    GEOGRAPHY = "GEOGRAPHY"
    GEOMETRY = "GEOMETRY"


class Language(ParseableEnum):
    JAVA = "JAVA"
    JAVASCRIPT = "JAVASCRIPT"
    PYTHON = "PYTHON"
    SCALA = "SCALA"
    SQL = "SQL"


class NullHandling(ParseableEnum):
    CALLED_ON_NULL_INPUT = "CALLED ON NULL INPUT"
    RETURNS_NULL_ON_NULL_INPUT = "RETURNS NULL ON NULL INPUT"
    STRICT = "STRICT"


class Volatility(ParseableEnum):
    VOLATILE = "VOLATILE"
    IMMUTABLE = "IMMUTABLE"
    STABLE = "STABLE"


# https://docs.snowflake.com/developer-guide/stored-procedure/stored-procedures-rights
class ExecutionRights(ParseableEnum):
    CALLER = "CALLER"
    OWNER = "OWNER"


class SessionParameter(ParseableEnum):
    ABORT_DETACHED_QUERY = "ABORT_DETACHED_QUERY"
    AUTOCOMMIT = "AUTOCOMMIT"
    AUTOCOMMIT_API_SUPPORTED = "AUTOCOMMIT_API_SUPPORTED"
    BINARY_INPUT_FORMAT = "BINARY_INPUT_FORMAT"
    BINARY_OUTPUT_FORMAT = "BINARY_OUTPUT_FORMAT"
    CLIENT_ENABLE_CONSERVATIVE_MEMORY_USAGE = "CLIENT_ENABLE_CONSERVATIVE_MEMORY_USAGE"
    CLIENT_ENABLE_DEFAULT_OVERWRITE_IN_PUT = "CLIENT_ENABLE_DEFAULT_OVERWRITE_IN_PUT"
    CLIENT_ENABLE_LOG_INFO_STATEMENT_PARAMETERS = "CLIENT_ENABLE_LOG_INFO_STATEMENT_PARAMETERS"
    CLIENT_MEMORY_LIMIT = "CLIENT_MEMORY_LIMIT"
    CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX = "CLIENT_METADATA_REQUEST_USE_CONNECTION_CTX"
    CLIENT_METADATA_USE_SESSION_DATABASE = "CLIENT_METADATA_USE_SESSION_DATABASE"
    CLIENT_PREFETCH_THREADS = "CLIENT_PREFETCH_THREADS"
    CLIENT_RESULT_CHUNK_SIZE = "CLIENT_RESULT_CHUNK_SIZE"
    CLIENT_RESULT_COLUMN_CASE_INSENSITIVE = "CLIENT_RESULT_COLUMN_CASE_INSENSITIVE"
    CLIENT_SESSION_CLONE = "CLIENT_SESSION_CLONE"
    CLIENT_SESSION_KEEP_ALIVE = "CLIENT_SESSION_KEEP_ALIVE"
    CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY = "CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY"
    CLIENT_TIMESTAMP_TYPE_MAPPING = "CLIENT_TIMESTAMP_TYPE_MAPPING"
    CSV_TIMESTAMP_FORMAT = "CSV_TIMESTAMP_FORMAT"
    C_API_QUERY_RESULT_FORMAT = "C_API_QUERY_RESULT_FORMAT"
    DATE_INPUT_FORMAT = "DATE_INPUT_FORMAT"
    DATE_OUTPUT_FORMAT = "DATE_OUTPUT_FORMAT"
    ENABLE_CONSOLE_OUTPUT = "ENABLE_CONSOLE_OUTPUT"
    ENABLE_UNLOAD_PHYSICAL_TYPE_OPTIMIZATION = "ENABLE_UNLOAD_PHYSICAL_TYPE_OPTIMIZATION"
    ERROR_ON_NONDETERMINISTIC_MERGE = "ERROR_ON_NONDETERMINISTIC_MERGE"
    ERROR_ON_NONDETERMINISTIC_UPDATE = "ERROR_ON_NONDETERMINISTIC_UPDATE"
    GEOGRAPHY_OUTPUT_FORMAT = "GEOGRAPHY_OUTPUT_FORMAT"
    GEOMETRY_OUTPUT_FORMAT = "GEOMETRY_OUTPUT_FORMAT"
    GO_QUERY_RESULT_FORMAT = "GO_QUERY_RESULT_FORMAT"
    JDBC_FORMAT_DATE_WITH_TIMEZONE = "JDBC_FORMAT_DATE_WITH_TIMEZONE"
    JDBC_QUERY_RESULT_FORMAT = "JDBC_QUERY_RESULT_FORMAT"
    JDBC_TREAT_DECIMAL_AS_INT = "JDBC_TREAT_DECIMAL_AS_INT"
    JDBC_TREAT_TIMESTAMP_NTZ_AS_UTC = "JDBC_TREAT_TIMESTAMP_NTZ_AS_UTC"
    JDBC_USE_SESSION_TIMEZONE = "JDBC_USE_SESSION_TIMEZONE"
    JSON_INDENT = "JSON_INDENT"
    JS_TREAT_INTEGER_AS_BIGINT = "JS_TREAT_INTEGER_AS_BIGINT"
    LANGUAGE = "LANGUAGE"
    LOCK_TIMEOUT = "LOCK_TIMEOUT"
    LOG_LEVEL = "LOG_LEVEL"
    MULTI_STATEMENT_COUNT = "MULTI_STATEMENT_COUNT"
    ODBC_QUERY_RESULT_FORMAT = "ODBC_QUERY_RESULT_FORMAT"
    ODBC_SCHEMA_CACHING = "ODBC_SCHEMA_CACHING"
    ODBC_USE_CUSTOM_SQL_DATA_TYPES = "ODBC_USE_CUSTOM_SQL_DATA_TYPES"
    PREVENT_UNLOAD_TO_INTERNAL_STAGES = "PREVENT_UNLOAD_TO_INTERNAL_STAGES"
    PYTHON_CONNECTOR_QUERY_RESULT_FORMAT = "PYTHON_CONNECTOR_QUERY_RESULT_FORMAT"
    PYTHON_SNOWPARK_USE_SCOPED_TEMP_OBJECTS = "PYTHON_SNOWPARK_USE_SCOPED_TEMP_OBJECTS"
    PYTHON_SNOWPARK_USE_SQL_SIMPLIFIER = "PYTHON_SNOWPARK_USE_SQL_SIMPLIFIER"
    QA_TEST_NAME = "QA_TEST_NAME"
    QUERY_RESULT_FORMAT = "QUERY_RESULT_FORMAT"
    QUERY_TAG = "QUERY_TAG"
    QUOTED_IDENTIFIERS_IGNORE_CASE = "QUOTED_IDENTIFIERS_IGNORE_CASE"
    READ_LATEST_WRITES = "READ_LATEST_WRITES"
    ROWS_PER_RESULTSET = "ROWS_PER_RESULTSET"
    S3_STAGE_VPCE_DNS_NAME = "S3_STAGE_VPCE_DNS_NAME"
    SEARCH_PATH = "SEARCH_PATH"
    SHOW_EXTERNAL_TABLE_KIND_AS_TABLE = "SHOW_EXTERNAL_TABLE_KIND_AS_TABLE"
    SIMULATED_DATA_SHARING_CONSUMER = "SIMULATED_DATA_SHARING_CONSUMER"
    SNOWPARK_HIDE_INTERNAL_ALIAS = "SNOWPARK_HIDE_INTERNAL_ALIAS"
    SNOWPARK_LAZY_ANALYSIS = "SNOWPARK_LAZY_ANALYSIS"
    SNOWPARK_REQUEST_TIMEOUT_IN_SECONDS = "SNOWPARK_REQUEST_TIMEOUT_IN_SECONDS"
    SNOWPARK_STORED_PROC_IS_FINAL_TABLE_QUERY = "SNOWPARK_STORED_PROC_IS_FINAL_TABLE_QUERY"
    SNOWPARK_USE_SCOPED_TEMP_OBJECTS = "SNOWPARK_USE_SCOPED_TEMP_OBJECTS"
    SQL_API_NULLABLE_IN_RESULT_SET = "SQL_API_NULLABLE_IN_RESULT_SET"
    SQL_API_QUERY_RESULT_FORMAT = "SQL_API_QUERY_RESULT_FORMAT"
    STATEMENT_QUEUED_TIMEOUT_IN_SECONDS = "STATEMENT_QUEUED_TIMEOUT_IN_SECONDS"
    STATEMENT_TIMEOUT_IN_SECONDS = "STATEMENT_TIMEOUT_IN_SECONDS"
    STRICT_JSON_OUTPUT = "STRICT_JSON_OUTPUT"
    TIMESTAMP_DAY_IS_ALWAYS_24H = "TIMESTAMP_DAY_IS_ALWAYS_24H"
    TIMESTAMP_INPUT_FORMAT = "TIMESTAMP_INPUT_FORMAT"
    TIMESTAMP_LTZ_OUTPUT_FORMAT = "TIMESTAMP_LTZ_OUTPUT_FORMAT"
    TIMESTAMP_NTZ_OUTPUT_FORMAT = "TIMESTAMP_NTZ_OUTPUT_FORMAT"
    TIMESTAMP_OUTPUT_FORMAT = "TIMESTAMP_OUTPUT_FORMAT"
    TIMESTAMP_TYPE_MAPPING = "TIMESTAMP_TYPE_MAPPING"
    TIMESTAMP_TZ_OUTPUT_FORMAT = "TIMESTAMP_TZ_OUTPUT_FORMAT"
    TIMEZONE = "TIMEZONE"
    TIME_INPUT_FORMAT = "TIME_INPUT_FORMAT"
    TIME_OUTPUT_FORMAT = "TIME_OUTPUT_FORMAT"
    TRACE_LEVEL = "TRACE_LEVEL"
    TRANSACTION_ABORT_ON_ERROR = "TRANSACTION_ABORT_ON_ERROR"
    TRANSACTION_DEFAULT_ISOLATION_LEVEL = "TRANSACTION_DEFAULT_ISOLATION_LEVEL"
    TWO_DIGIT_CENTURY_START = "TWO_DIGIT_CENTURY_START"
    UI_QUERY_RESULT_FORMAT = "UI_QUERY_RESULT_FORMAT"
    UNSUPPORTED_DDL_ACTION = "UNSUPPORTED_DDL_ACTION"
    USE_CACHED_RESULT = "USE_CACHED_RESULT"
    WEEK_OF_YEAR_POLICY = "WEEK_OF_YEAR_POLICY"
    WEEK_START = "WEEK_START"


class TaskState(ParseableEnum):
    """
    Represents the scheduling state of a task.
    https://docs.snowflake.com/en/sql-reference/sql/show-tasks#output
    Snowflake provides these as lowercase but we'll uppercase them.
    """

    STARTED = "STARTED"
    SUSPENDED = "SUSPENDED"


class WarehouseSize(ParseableEnum):
    """
    Represents the size options for a warehouse.

    Available sizes:
    - XSMALL
    - SMALL
    - MEDIUM
    - LARGE
    - XLARGE
    - XXLARGE
    - XXXLARGE
    - X4LARGE
    - X5LARGE (AWS-only)
    - X6LARGE (AWS-only)
    """

    XSMALL = "XSMALL"
    SMALL = "SMALL"
    MEDIUM = "MEDIUM"
    LARGE = "LARGE"
    XLARGE = "XLARGE"
    XXLARGE = "XXLARGE"
    XXXLARGE = "XXXLARGE"
    X4LARGE = "X4LARGE"
    X5LARGE = "X5LARGE"
    X6LARGE = "X6LARGE"

    @classmethod
    def synonyms(cls):
        return {
            "X-SMALL": "XSMALL",
            "X-LARGE": "XLARGE",
            "X2LARGE": "XXLARGE",
            "2X-LARGE": "XXLARGE",
            "X3LARGE": "XXXLARGE",
            "3X-LARGE": "XXXLARGE",
            "4X-LARGE": "X4LARGE",
            "5X-LARGE": "X5LARGE",
            "6X-LARGE": "X6LARGE",
        }


class FileType(ParseableEnum):
    CSV = "CSV"
    JSON = "JSON"
    AVRO = "AVRO"
    ORC = "ORC"
    PARQUET = "PARQUET"
    XML = "XML"


class Compression(ParseableEnum):
    AUTO = "AUTO"
    GZIP = "GZIP"
    BZ2 = "BZ2"
    BROTLI = "BROTLI"
    ZSTD = "ZSTD"
    DEFLATE = "DEFLATE"
    RAW_DEFLATE = "RAW_DEFLATE"
    LZO = "LZO"
    SNAPPY = "SNAPPY"
    NONE = "NONE"


class BinaryFormat(ParseableEnum):
    HEX = "HEX"
    BASE64 = "BASE64"
    UTF8 = "UTF8"


def resource_type_is_grant(resource_type: ResourceType) -> bool:
    return resource_type in (
        ResourceType.GRANT,
        ResourceType.GRANT_ON_ALL,
        ResourceType.ROLE_GRANT,
        ResourceType.GRANT_ON_ALL,
        ResourceType.FUTURE_GRANT,
    )


class EncryptionType(ParseableEnum):
    SNOWFLAKE_FULL = "SNOWFLAKE_FULL"
    SNOWFLAKE_SSE = "SNOWFLAKE_SSE"
    AWS_CSE = "AWS_CSE"
    AWS_SSE_S3 = "AWS_SSE_S3"
    AWS_SSE_KMS = "AWS_SSE_KMS"
    GCS_SSE_KMS = "GCS_SSE_KMS"
    AZURE_CSE = "AZURE_CSE"
    NONE = "NONE"
