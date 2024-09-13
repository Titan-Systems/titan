import os

import pytest
from tests.helpers import clean_resource_data

from titan import data_provider
from titan import resources as res
from titan.client import reset_cache

pytestmark = pytest.mark.requires_snowflake

TEST_ROLE = os.environ.get("TEST_SNOWFLAKE_ROLE")
TEST_USER = os.environ.get("TEST_SNOWFLAKE_USER")


def safe_fetch(cursor, urn):
    reset_cache()
    return data_provider.fetch_resource(cursor, urn)


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
    table = res.Table(
        name=f"TABLE_{suffix}",
        database=test_db,
        schema="PUBLIC",
        owner=TEST_ROLE,
        columns=[res.Column(name="ID", data_type="INT")],
    )
    cursor.execute(table.create_sql(if_not_exists=True))
    result = safe_fetch(cursor, table.urn)
    assert result is not None
    result = clean_resource_data(res.Table.spec, result)
    assert result == clean_resource_data(res.Table.spec, table.to_dict())
