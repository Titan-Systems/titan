import pytest

from titan import resources as res
from titan.blueprint import Blueprint, OrphanResourceException, _walk
from titan.enums import ResourceType
from titan.resources.resource import ResourcePointer


@pytest.fixture
def session_ctx() -> dict:
    return {
        "account": "SOMEACCT",
        "account_locator": "ABCD123",
        "role": "SYSADMIN",
        "available_roles": ["SYSADMIN", "USERADMIN"],
    }


def test_empty_blueprint(session_ctx):
    blueprint = Blueprint()
    blueprint._finalize(session_ctx)
    assert len(list(_walk(blueprint._root))) == 1


def test_account_scoped_resource_solo(session_ctx):
    role = res.Role("SOME_ROLE")
    blueprint = Blueprint(resources=[role])
    blueprint._finalize(session_ctx)
    assert len(list(_walk(blueprint._root))) == 2
    assert blueprint._root.items()[0] == role
    assert role.container == blueprint._root


def test_account_scoped_resource_with_children(session_ctx):
    database = res.Database("SOME_DATABASE")
    schema = res.Schema("SOME_SCHEMA")
    database.add(schema)
    blueprint = Blueprint(resources=[database])
    blueprint._finalize(session_ctx)
    assert len(list(_walk(blueprint._root))) == 4
    assert blueprint._root.items()[0] == database
    assert database.container == blueprint._root
    assert schema.container == database


def test_database_scoped_resource_solo(session_ctx):
    schema = res.Schema("SOME_SCHEMA")
    blueprint = Blueprint(resources=[schema])

    assert session_ctx.get("database") is None
    with pytest.raises(OrphanResourceException):
        blueprint._finalize(session_ctx)


def test_database_scoped_resource_solo_with_active_database_session():
    session_ctx = {
        "account": "SOMEACCT",
        "account_locator": "ABCD123",
        "database": "SOME_DATABASE",
        "role": "SYSADMIN",
        "available_roles": ["SYSADMIN", "USERADMIN"],
    }
    schema = res.Schema("SOME_SCHEMA")
    blueprint = Blueprint(resources=[schema])
    blueprint._finalize(session_ctx)

    assert session_ctx.get("database") == "SOME_DATABASE"
    assert len(list(_walk(blueprint._root))) == 4
    assert len(blueprint._root.items()) == 1
    root_database = blueprint._root.items()[0]
    assert isinstance(root_database, ResourcePointer)
    assert root_database.resource_type == ResourceType.DATABASE
    assert root_database.name == "SOME_DATABASE"
    assert len(root_database.items()) == 2
    assert root_database.items()[0].name == "PUBLIC"
    assert root_database.items()[1] == schema
    assert root_database == schema.container


def test_database_scoped_resource_with_attached_parent(session_ctx):
    database = res.Database("SOME_DATABASE")
    schema = res.Schema("SOME_SCHEMA")
    database.add(schema)
    blueprint = Blueprint(resources=[schema])

    blueprint._finalize(session_ctx)
    assert len(list(_walk(blueprint._root))) == 4
    assert blueprint._root.items()[0] == database
    assert database.container == blueprint._root
    assert schema.container == database


def test_database_scoped_resource_with_referenced_parent(session_ctx):
    schema = res.Schema("SOME_SCHEMA", database="SOME_DATABASE")
    blueprint = Blueprint(resources=[schema])

    blueprint._finalize(session_ctx)
    assert len(list(_walk(blueprint._root))) == 4
    assert blueprint._root.items()[0].name == "SOME_DATABASE"
    assert blueprint._root.items()[0].find(name="SOME_SCHEMA", resource_type=ResourceType.SCHEMA) == schema
    assert schema.container == blueprint._root.items()[0]


def test_database_scoped_resource_with_inline_referenced_parent(session_ctx):
    schema = res.Schema("SOME_DATABASE.SOME_SCHEMA")
    blueprint = Blueprint(resources=[schema])

    blueprint._finalize(session_ctx)
    assert len(list(_walk(blueprint._root))) == 4
    assert blueprint._root.items()[0].name == "SOME_DATABASE"
    assert blueprint._root.items()[0].find(name="SOME_SCHEMA", resource_type=ResourceType.SCHEMA) == schema
    assert schema.container == blueprint._root.items()[0]


def test_resource_merging(session_ctx):
    db = res.Database("SOME_DATABASE")
    schema = res.Schema("SOME_DATABASE.SOME_SCHEMA")

    assert schema.container is not None
    assert isinstance(schema.container, ResourcePointer)
    assert schema.container.name == "SOME_DATABASE"
    assert schema.container.resource_type == ResourceType.DATABASE

    blueprint = Blueprint(resources=[db, schema])

    assert session_ctx.get("database") is None
    blueprint._finalize(session_ctx)
    resources = list(_walk(blueprint._root))
    assert len(resources) == 4
    assert schema.container is not None
    assert isinstance(schema.container, res.Database)
    assert schema.container.name == "SOME_DATABASE"


def test_public_schema_raises_error():
    db = res.Database("SOME_DATABASE")
    with pytest.raises(ValueError):
        res.Schema("PUBLIC", database=db, comment="This is a test")
