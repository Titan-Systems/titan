# from .resources import Database


def create_warehouse_sql(
    name,
    or_replace=False,
    if_not_exists=False,
    warehouse_type=None,
    warehouse_size=None,
    max_cluster_count=None,
    min_cluster_count=None,
    scaling_policy=None,
    auto_suspend=None,
    auto_resume=None,
    initially_suspended=None,
    resource_monitor=None,
    comment=None,
    enable_query_acceleration=None,
    query_acceleration_max_scale_factor=None,
    max_concurrency_level=None,
    statement_queued_timeout_in_seconds=None,
    statement_timeout_in_seconds=None,
    tags=None,
):
    # CREATE [ OR REPLACE ] WAREHOUSE [ IF NOT EXISTS ] <name>
    #     [ [ WITH ] objectProperties ]
    #     [ objectParams ]

    replace = "OR REPLACE " if or_replace else ""
    exists = "IF NOT EXISTS " if if_not_exists else ""

    return f"""CREATE {replace}WAREHOUSE {exists}{name}"""


# def create_database_sql(**kwargs):
#     d = Database(**kwargs)
#     return d.create_sql()


def tidy_sql(*parts):
    if isinstance(parts[0], list):
        parts = parts[0]
    return " ".join([str(part) for part in parts if part])
