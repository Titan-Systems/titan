from titan.enums import ResourceType
from titan.identifiers import URN
from titan.resources import FutureGrant, Grant, Role, RoleGrant, Schema, User, Warehouse


def test_grant_global_priv():
    grant = Grant(priv="CREATE WAREHOUSE", on="ACCOUNT", to="somerole")
    assert grant.priv == "CREATE WAREHOUSE"
    assert grant.on == "ACCOUNT"
    assert grant.to.name == "somerole"
    assert str(URN.from_resource(grant)) == "urn:::grant/somerole?priv=CREATE WAREHOUSE&on=ACCOUNT"
    assert grant.create_sql() == "GRANT CREATE WAREHOUSE ON ACCOUNT TO somerole"


def test_grant_account_obj_priv_with_resource():
    wh = Warehouse(name="somewh")
    grant = Grant(priv="MODIFY", on=wh, to="somerole")
    assert grant.priv == "MODIFY"
    assert grant.on == "SOMEWH"
    assert grant.on_type == ResourceType.WAREHOUSE
    assert grant.to.name == "SOMEROLE"
    assert str(URN.from_resource(grant)) == "urn:::grant/somerole?priv=MODIFY&on=warehouse/somewh"


def test_grant_account_obj_priv_with_kwarg():
    grant = Grant(priv="MODIFY", on_warehouse="somewh", to="somerole")
    assert grant.priv == "MODIFY"
    assert grant.on == "SOMEWH"
    assert grant.on_type == ResourceType.WAREHOUSE
    assert grant.to.name == "SOMEROLE"
    assert str(URN.from_resource(grant)) == "urn:::grant/somerole?priv=MODIFY&on=warehouse/somewh"


def test_grant_schema_priv_with_resource():
    sch = Schema(name="someschema")
    grant = Grant(priv="CREATE VIEW", on=sch, to="somerole")
    assert grant.priv == "CREATE VIEW"
    assert grant.on == "SOMESCHEMA"
    assert grant.on_type == ResourceType.SCHEMA
    assert grant.to.name == "SOMEROLE"
    assert str(URN.from_resource(grant)) == "urn:::grant/somerole?priv=CREATE VIEW&on=schema/someschema"


def test_grant_schema_priv_with_kwarg():
    grant = Grant(priv="CREATE VIEW", on_schema="someschema", to="somerole")
    assert grant.priv == "CREATE VIEW"
    assert grant.on == "SOMESCHEMA"
    assert grant.on_type == ResourceType.SCHEMA
    assert grant.to.name == "SOMEROLE"
    assert str(URN.from_resource(grant)) == "urn:::grant/somerole?priv=CREATE VIEW&on=schema/someschema"


def test_grant_future_schemas_priv():
    grant = FutureGrant(priv="CREATE VIEW", on_future_schemas_in_database="somedb", to="somerole")
    assert grant.priv == "CREATE VIEW"
    assert grant.on_type == ResourceType.SCHEMA
    assert grant.in_type == ResourceType.DATABASE
    assert grant.in_name == "SOMEDB"
    assert grant.to.name == "SOMEROLE"
    assert str(URN.from_resource(grant)) == "urn:::future_grant/somerole?priv=CREATE VIEW&on=database/somedb.<SCHEMA>"


def test_role_grant_to_user_with_kwargs():
    grant = RoleGrant(role="somerole", to_user="someuser")
    assert grant.role.name == "somerole"
    assert grant._data.to_user is not None
    assert grant.to.name == "someuser"
    assert str(URN.from_resource(grant)) == "urn:::role_grant/somerole?user=someuser"


def test_role_grant_to_user_with_resource():
    grant = RoleGrant(role="somerole", to=User(name="someuser"))
    assert grant.role.name == "somerole"
    assert grant._data.to_user is not None
    assert grant.to.name == "someuser"
    assert str(URN.from_resource(grant)) == "urn:::role_grant/somerole?user=someuser"


def test_role_grant_to_role_with_kwargs():
    grant = RoleGrant(role="somerole", to_role="someotherrole")
    assert grant.role.name == "somerole"
    assert grant._data.to_role is not None
    assert grant.to.name == "someotherrole"
    assert str(URN.from_resource(grant)) == "urn:::role_grant/somerole?role=someotherrole"


def test_role_grant_to_role_with_resource():
    grant = RoleGrant(role="somerole", to=Role(name="someotherrole"))
    assert grant.role.name == "somerole"
    assert grant._data.to_role is not None
    assert grant.to.name == "someotherrole"
    assert str(URN.from_resource(grant)) == "urn:::role_grant/somerole?role=someotherrole"


# def test_grant_all_schemas_priv():
#     grant = Grant(priv="CREATE VIEW", on_all_schemas_in_database="somedb", to="somerole")
#     assert grant.priv == "CREATE VIEW"
#     assert grant.on == "database somedb"
#     assert grant.on_all == "SCHEMAS"
#     assert grant.to == "somerole"
