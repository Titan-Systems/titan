from titan.enums import ResourceType
from titan.privs import _all_privs_for_resource_type
from titan.identifiers import URN
from titan.resource_name import ResourceName
from titan.resources import FutureGrant, Grant, ImageRepository, Role, RoleGrant, Schema, User, Warehouse
from titan.resources.resource import ResourcePointer


def test_grant_global_priv():
    grant = Grant(priv="CREATE WAREHOUSE", on="ACCOUNT", to="somerole")
    assert grant.priv == "CREATE WAREHOUSE"
    assert grant.on == "ACCOUNT"
    assert grant.to.name == "somerole"
    assert str(URN.from_resource(grant)) == "urn:::grant/SOMEROLE?priv=CREATE WAREHOUSE&on=account/ACCOUNT"
    assert grant.create_sql() == "GRANT CREATE WAREHOUSE ON ACCOUNT TO SOMEROLE"


def test_grant_account_obj_priv_with_resource():
    wh = Warehouse(name="somewh")
    grant = Grant(priv="MODIFY", on=wh, to="somerole")
    assert grant.priv == "MODIFY"
    assert grant.on == "SOMEWH"
    assert grant.on_type == ResourceType.WAREHOUSE
    assert grant.to.name == "SOMEROLE"
    assert str(URN.from_resource(grant)) == "urn:::grant/SOMEROLE?priv=MODIFY&on=warehouse/SOMEWH"


def test_grant_account_obj_priv_with_kwarg():
    grant = Grant(priv="MODIFY", on_warehouse="somewh", to="somerole")
    assert grant.priv == "MODIFY"
    assert grant.on == "SOMEWH"
    assert grant.on_type == ResourceType.WAREHOUSE
    assert grant.to.name == "SOMEROLE"
    assert str(URN.from_resource(grant)) == "urn:::grant/SOMEROLE?priv=MODIFY&on=warehouse/SOMEWH"


def test_grant_schema_priv_with_resource():
    sch = Schema(name="someschema")
    grant = Grant(priv="CREATE VIEW", on=sch, to="somerole")
    assert grant.priv == "CREATE VIEW"
    assert grant.on == "SOMESCHEMA"
    assert grant.on_type == ResourceType.SCHEMA
    assert grant.to.name == "SOMEROLE"
    assert str(URN.from_resource(grant)) == "urn:::grant/SOMEROLE?priv=CREATE VIEW&on=schema/SOMESCHEMA"


def test_grant_schema_priv_with_kwarg():
    grant = Grant(priv="CREATE VIEW", on_schema="someschema", to="somerole")
    assert grant.priv == "CREATE VIEW"
    assert grant.on == "SOMESCHEMA"
    assert grant.on_type == ResourceType.SCHEMA
    assert grant.to.name == "SOMEROLE"
    assert str(URN.from_resource(grant)) == "urn:::grant/SOMEROLE?priv=CREATE VIEW&on=schema/SOMESCHEMA"


def test_grant_all():
    grant = Grant(priv="ALL", on_warehouse="somewh", to="somerole")
    assert grant.priv == "ALL"
    assert grant.on == "SOMEWH"
    assert grant.on_type == ResourceType.WAREHOUSE
    assert grant.to.name == "SOMEROLE"
    assert grant._data._privs == _all_privs_for_resource_type(ResourceType.WAREHOUSE)
    assert str(URN.from_resource(grant)) == "urn:::grant/SOMEROLE?priv=ALL&on=warehouse/SOMEWH"


def test_grant_future_schemas_priv():
    grant = FutureGrant(priv="CREATE VIEW", on_future_schemas_in_database="somedb", to="somerole")
    assert grant.priv == "CREATE VIEW"
    assert grant.on_type == ResourceType.SCHEMA
    assert grant.in_type == ResourceType.DATABASE
    assert grant.in_name == "SOMEDB"
    assert grant.to.name == "SOMEROLE"
    assert str(URN.from_resource(grant)) == "urn:::future_grant/SOMEROLE?priv=CREATE VIEW&on=database/SOMEDB.<SCHEMA>"


def test_role_grant_to_user_with_kwargs():
    grant = RoleGrant(role="somerole", to_user="someuser")
    assert grant.role.name == "somerole"
    assert grant._data.to_user is not None
    assert grant.to.name == "someuser"
    assert str(URN.from_resource(grant)) == "urn:::role_grant/SOMEROLE?user=SOMEUSER"


def test_role_grant_to_user_with_resource():
    grant = RoleGrant(role="somerole", to=User(name="someuser"))
    assert grant.role.name == "somerole"
    assert grant._data.to_user is not None
    assert grant.to.name == "someuser"
    assert str(URN.from_resource(grant)) == "urn:::role_grant/SOMEROLE?user=SOMEUSER"


def test_role_grant_to_role_with_kwargs():
    grant = RoleGrant(role="somerole", to_role="someotherrole")
    assert grant.role.name == "somerole"
    assert grant._data.to_role is not None
    assert grant.to.name == "someotherrole"
    assert str(URN.from_resource(grant)) == "urn:::role_grant/SOMEROLE?role=SOMEOTHERROLE"


def test_role_grant_to_role_with_resource():
    grant = RoleGrant(role="somerole", to=Role(name="someotherrole"))
    assert grant.role.name == "somerole"
    assert grant._data.to_role is not None
    assert grant.to.name == "someotherrole"
    assert str(URN.from_resource(grant)) == "urn:::role_grant/SOMEROLE?role=SOMEOTHERROLE"


# def test_grant_all_schemas_priv():
#     grant = Grant(priv="CREATE VIEW", on_all_schemas_in_database="somedb", to="somerole")
#     assert grant.priv == "CREATE VIEW"
#     assert grant.on == "database somedb"
#     assert grant.on_all == "SCHEMAS"
#     assert grant.to == "somerole"


def test_grant_refs():
    grant = Grant.from_sql("GRANT READ ON IMAGE REPOSITORY some_repo TO ROLE titan_app_admin")
    repo = ImageRepository(
        name="titan_app_image_repo",
        owner="titan_app_admin",
        database="titan_app_db",
        schema="titan_app",
    )
    assert ResourceName(str(repo.fqn)) == ResourceName("titan_app_db.titan_app.titan_app_image_repo")
    assert grant.priv == "READ"
    assert ResourcePointer("some_repo", ResourceType.IMAGE_REPOSITORY) in grant.refs


def test_grant_priv_is_serialized_uppercase():
    grant = Grant(priv="usage", on_warehouse="somewh", to="somerole")
    assert grant.priv == "USAGE"
