import os
import snowflake.connector
from pprint import pprint

from titan import Blueprint
from titan.resources import Database, Warehouse, Role, RoleGrant, Pipe, Table, Column, Schema, Grant, FutureGrant
from titan.blueprint import print_plan

connection_params = {
    "account": os.environ["SNOWFLAKE_ACCOUNT"],
    "user": os.environ["SNOWFLAKE_USER"],
    "password": os.environ["SNOWFLAKE_PASSWORD"],
    "role": "SYSADMIN",
    "warehouse": "TRANSFORMING",
}


def dbt():
    # Databases
    role = Role(name="DEMO_ROLE")
    sysad_grant = RoleGrant(role=role, to_role="SYSADMIN")
    # user_grant = RoleGrant(role=role, to_user="ETL")

    test_db = Database(name="TEST_TITAN", transient=False, data_retention_time_in_days=1, comment="Test Titan")

    schema = Schema(name="TEST_SCHEMA", database=test_db, transient=False, comment="Test Titan Schema")

    warehouse = Warehouse(name="FAKER_LOADER", auto_suspend=60)

    future_schema_grant = FutureGrant(priv="usage", on_future_schemas_in=test_db, to=role)
    post_grant = [future_schema_grant]

    grants = [
        Grant(priv="usage", to=role, on=warehouse),
        Grant(priv="operate", to=role, on=warehouse),
        Grant(priv="usage", to=role, on=test_db),
        # future_schema_grant,
        # x
        # Grant(priv="usage", to=role, on=schema)
    ]

    sales_table = Table(
        name="faker_data",
        schema=schema,
        columns=[
            Column(name="NAME", data_type="VARCHAR(16777216)"),
            Column(name="EMAIL", data_type="VARCHAR(16777216)"),
            Column(name="ADDRESS", data_type="VARCHAR(16777216)"),
            Column(name="ORDERED_AT_UTC", data_type="NUMBER(38,0)"),
            Column(name="EXTRACTED_AT_UTC", data_type="NUMBER(38,0)"),
            Column(name="SALES_ORDER_ID", data_type="VARCHAR(16777216)"),
        ],
        comment="Test Table",
    )

    copy_statement = f"""
    COPY INTO {test_db.name}.{schema.name}.faker_data
    FROM
    '@{test_db.name}.{schema.name}.%faker_data' FILE_FORMAT = (TYPE = 'CSV');
    """
    pipe = Pipe(
        name="TEST_TITAN_PIPE",
        as_=copy_statement,
        comment="Pipe for ingesting PARQUET data",
        schema=schema,
    )

    pipe.requires = [sales_table]

    # streams
    # dynamic tables

    return (
        role,
        sysad_grant,
        # user_grant,
        test_db,
        # *pre_grant,
        schema,
        sales_table,
        pipe,
        warehouse,
        *grants,
    )


if __name__ == "__main__":
    bp = Blueprint(name="dbt-quickstart")
    bp.add(*dbt())
    session = snowflake.connector.connect(**connection_params)
    plan = bp.plan(session)
    print_plan(plan)

    # Update Snowflake to match blueprint
    bp.apply(session, plan)
    print("Done")
