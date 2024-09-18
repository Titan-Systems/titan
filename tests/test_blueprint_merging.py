import pytest

from titan import resources as res
from titan.blueprint import _merge_pointers
from titan.resources.resource import ResourcePointer
from titan.enums import ResourceType

# from titan.identifiers import parse_URN
# from titan.privs import AccountPriv, DatabasePriv, GrantedPrivilege
# from titan.resource_name import ResourceName


def test_merge_account_scoped_pointers():
    warehouse = res.Warehouse(name="test_warehouse")
    warehouse_pointer = ResourcePointer(name="test_warehouse", resource_type=ResourceType.WAREHOUSE)
    resources = [warehouse, warehouse_pointer]
    merged = _merge_pointers(resources)
    assert len(merged) == 1
    assert merged[0] is warehouse


def test_merge_implied_database_into_actual():
    database = res.Database(name="test_database")
    schema = res.Schema(name="test_database.someschema")
    resources = [database, schema.container]
    merged = _merge_pointers(resources)
    assert len(merged) == 1
    assert merged[0] is database


def test_merge_moves_children():
    schema = res.Schema(name="someschema")
    task = res.Task(name="someschema.sometask")
    assert isinstance(task.container, ResourcePointer)
    resources = [schema, task.container]
    merged = _merge_pointers(resources)
    assert len(merged) == 1
    assert merged[0] is schema
    assert task in schema.items()
    assert task.container is schema


def test_merge_deep_tree():
    database = res.Database(name="test_database")
    schema = res.Schema(name="test_database.someschema")
    task = res.Task(name="test_database.someschema.sometask")
    account_scoped_resources = [database, schema.container]
    merged = _merge_pointers(account_scoped_resources)
    assert len(merged) == 1

    db_scoped_resources = [schema, task.container]
    merged = _merge_pointers(db_scoped_resources)
    assert len(merged) == 1
    assert merged[0] is schema
    assert task.container is schema
    assert task.container.container is database


def test_merge_throw_away_duplicates():
    database = res.Database(name="test_database")
    resources = [database, database]
    merged = _merge_pointers(resources)
    assert len(merged) == 1
    assert merged[0] is database
