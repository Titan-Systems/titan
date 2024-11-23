import os

import pytest

from titan import data_provider
from titan import resources as res
from titan.enums import DataType

from tests.helpers import safe_fetch

pytestmark = pytest.mark.requires_snowflake

TEST_ROLE = os.environ.get("TEST_SNOWFLAKE_ROLE")
TEST_USER = os.environ.get("TEST_SNOWFLAKE_USER")


def test_fetch_table_clustered(cursor, test_db, suffix):
    cursor.execute(f"USE DATABASE {test_db}")
    cursor.execute(f"USE SCHEMA PUBLIC")

    table = res.Table(
        name=f"TABLE_{suffix}",
        database=test_db,
        schema="PUBLIC",
        owner=TEST_ROLE,
        enable_schema_evolution=True,
        cluster_by=["ID"],
        columns=[res.Column(name="ID", data_type="NUMBER(38,0)")],
    )
    cursor.execute(table.create_sql(if_not_exists=True))
    result = safe_fetch(cursor, table.urn)
    assert result is not None
    result = data_provider.remove_none_values(result)
    assert result == data_provider.remove_none_values(table.to_dict())
    cursor.execute(table.drop_sql(if_exists=True))


def test_fetch_table_simple(cursor, test_db, suffix):
    cursor.execute(f"USE DATABASE {test_db}")
    cursor.execute("USE SCHEMA PUBLIC")
    table = res.Table(
        name=f"TABLE_{suffix}",
        database=test_db,
        schema="PUBLIC",
        owner=TEST_ROLE,
        columns=[res.Column(name="ID", data_type="NUMBER(38,0)")],
    )
    cursor.execute(table.create_sql(if_not_exists=True))
    result = safe_fetch(cursor, table.urn)
    assert result is not None
    result = data_provider.remove_none_values(result)
    assert result == data_provider.remove_none_values(table.to_dict())
    cursor.execute(table.drop_sql(if_exists=True))


def test_fetch_table_transient(cursor, test_db, suffix):
    cursor.execute(f"USE DATABASE {test_db}")
    cursor.execute("USE SCHEMA PUBLIC")
    table = res.Table(
        name=f"TABLE_{suffix}",
        database=test_db,
        schema="PUBLIC",
        owner=TEST_ROLE,
        columns=[res.Column(name="ID", data_type="NUMBER(38,0)")],
        transient=True,
    )
    cursor.execute(table.create_sql(if_not_exists=True))
    result = safe_fetch(cursor, table.urn)
    assert result is not None
    result = data_provider.remove_none_values(result)
    assert result == data_provider.remove_none_values(table.to_dict())
    cursor.execute(table.drop_sql(if_exists=True))


def test_fetch_column_data_type_synonyms(cursor, test_db, suffix):
    cursor.execute(f"USE DATABASE {test_db}")
    cursor.execute("USE SCHEMA PUBLIC")

    columns = []

    # for i, data_type in enumerate(
    #     (

    #         "NUMERIC(38,0)",
    #         "NUMERIC(2,1)",
    #         "NUMERIC(7, 0)",

    #     )
    # ):
    for i, data_type in enumerate(DataType.__members__.values()):
        columns.append(res.Column(name=f"ID_{i}", data_type=str(data_type)))
    table = res.Table(
        name=f"TABLE_{suffix}",
        database=test_db,
        schema="PUBLIC",
        owner=TEST_ROLE,
        columns=columns,
    )
    cursor.execute(table.create_sql(if_not_exists=True))
    result = safe_fetch(cursor, table.urn)
    assert result is not None
    data_columns = table.to_dict()["columns"]
    result_columns = result["columns"]
    assert len(result_columns) == len(data_columns)
    for result_column, data_column in zip(result_columns, data_columns):
        assert data_column["data_type"] == result_column["data_type"]
