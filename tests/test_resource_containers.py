import pytest

from titan import resources as res
from titan.enums import ResourceType
from titan.resources.resource import ResourceHasContainerException, WrongContainerException


def test_account_can_add_database():
    account = res.Account("SOME_ACCOUNT", "SOME_LOCATOR")
    database = res.Database("SOME_DATABASE")
    account.add(database)
    assert database in account.items(resource_type=ResourceType.DATABASE)


def test_account_cannot_add_schema():
    account = res.Account("SOME_ACCOUNT", "SOME_LOCATOR")
    schema = res.Schema("SOME_SCHEMA")
    with pytest.raises(WrongContainerException):
        account.add(schema)


def test_database_can_add_schema():
    database = res.Database("SOME_DATABASE")
    schema = res.Schema("SOME_SCHEMA")
    database.add(schema)
    assert schema in database.items(resource_type=ResourceType.SCHEMA)


def test_database_cannot_add_user():
    database = res.Database("SOME_DATABASE")
    user = res.User("SOME_USER")
    with pytest.raises(WrongContainerException):
        database.add(user)


def test_database_cannot_add_view():
    database = res.Database("SOME_DATABASE")
    view = res.View("SOME_VIEW")
    with pytest.raises(WrongContainerException):
        database.add(view)


def test_schema_can_add_view():
    schema = res.Schema("SOME_SCHEMA")
    view = res.View("SOME_VIEW")
    schema.add(view)
    assert view in schema.items(resource_type=ResourceType.VIEW)


def test_schema_cannot_add_user():
    schema = res.Schema("SOME_SCHEMA")
    user = res.User("SOME_USER")
    with pytest.raises(WrongContainerException):
        schema.add(user)


def test_remove_resource():
    database = res.Database("SOME_DATABASE")
    schema = res.Schema("SOME_SCHEMA")
    database.add(schema)
    assert schema in database.items(resource_type=ResourceType.SCHEMA)
    database.remove(schema)
    assert schema not in database.items(resource_type=ResourceType.SCHEMA)


def test_resource_already_belongs_to_container():
    schema1 = res.Schema("SOME_SCHEMA1")
    schema2 = res.Schema("SOME_SCHEMA2")
    view = res.View("SOME_VIEW")

    schema1.add(view)
    assert view in schema1.items(resource_type=ResourceType.VIEW)

    with pytest.raises(ResourceHasContainerException):
        schema2.add(view)
