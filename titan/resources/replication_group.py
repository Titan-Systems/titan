from .base import Resource, AccountScoped


class ReplicationGroup(Resource, AccountScoped):
    """
    CREATE REPLICATION GROUP [ IF NOT EXISTS ] <name>
        OBJECT_TYPES = <object_type> [ , <object_type> , ... ]
        [ ALLOWED_DATABASES = <db_name> [ , <db_name> , ... ] ]
        [ ALLOWED_SHARES = <share_name> [ , <share_name> , ... ] ]
        [ ALLOWED_INTEGRATION_TYPES = <integration_type_name> [ , <integration_type_name> , ... ] ]
        ALLOWED_ACCOUNTS = <org_name>.<target_account_name> [ , <org_name>.<target_account_name> , ... ]
        [ IGNORE EDITION CHECK ]
        [ REPLICATION_SCHEDULE = '{ <num> MINUTE | USING CRON <expr> <time_zone> }' ]
    """

    resource_type = "REPLICATION GROUP"

    name: str
