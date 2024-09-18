import pytest

from titan import resources as res
from titan.enums import ResourceType
from titan.privs import all_privs_for_resource_type
from titan.identifiers import URN
from titan.resource_name import ResourceName
from titan.resources.resource import ResourcePointer


def test_grant_global_priv():
    grant = res.Grant(priv="CREATE WAREHOUSE", on="ACCOUNT", to="somerole")
    assert grant.priv == "CREATE WAREHOUSE"
    assert grant.on == "ACCOUNT"
    assert grant.to.name == "somerole"
    assert str(URN.from_resource(grant)) == "urn:::grant/SOMEROLE?priv=CREATE WAREHOUSE&on=account/ACCOUNT"
    assert grant.create_sql() == "GRANT CREATE WAREHOUSE ON ACCOUNT TO SOMEROLE"


def test_grant_account_obj_priv_with_resource():
    wh = res.Warehouse(name="somewh")
    grant = res.Grant(priv="MODIFY", on=wh, to="somerole")
    assert grant.priv == "MODIFY"
    assert grant.on == "SOMEWH"
    assert grant.on_type == ResourceType.WAREHOUSE
    assert grant.to.name == "SOMEROLE"
    assert str(URN.from_resource(grant)) == "urn:::grant/SOMEROLE?priv=MODIFY&on=warehouse/SOMEWH"


def test_grant_account_obj_priv_with_kwarg():
    grant = res.Grant(priv="MODIFY", on_warehouse="somewh", to="somerole")
    assert grant.priv == "MODIFY"
    assert grant.on == "SOMEWH"
    assert grant.on_type == ResourceType.WAREHOUSE
    assert grant.to.name == "SOMEROLE"
    assert str(URN.from_resource(grant)) == "urn:::grant/SOMEROLE?priv=MODIFY&on=warehouse/SOMEWH"


def test_grant_schema_priv_with_resource():
    sch = res.Schema(name="someschema")
    grant = res.Grant(priv="CREATE VIEW", on=sch, to="somerole")
    assert grant.priv == "CREATE VIEW"
    assert grant.on == "SOMESCHEMA"
    assert grant.on_type == ResourceType.SCHEMA
    assert grant.to.name == "SOMEROLE"
    assert str(URN.from_resource(grant)) == "urn:::grant/SOMEROLE?priv=CREATE VIEW&on=schema/SOMESCHEMA"


def test_grant_schema_priv_with_kwarg():
    grant = res.Grant(priv="CREATE VIEW", on_schema="someschema", to="somerole")
    assert grant.priv == "CREATE VIEW"
    assert grant.on == "SOMESCHEMA"
    assert grant.on_type == ResourceType.SCHEMA
    assert grant.to.name == "SOMEROLE"
    assert str(URN.from_resource(grant)) == "urn:::grant/SOMEROLE?priv=CREATE VIEW&on=schema/SOMESCHEMA"


def test_grant_all():
    grant = res.Grant(priv="ALL", on_warehouse="somewh", to="somerole")
    assert grant.priv == "ALL"
    assert grant.on == "SOMEWH"
    assert grant.on_type == ResourceType.WAREHOUSE
    assert grant.to.name == "SOMEROLE"
    assert grant._data._privs == all_privs_for_resource_type(ResourceType.WAREHOUSE)
    assert str(URN.from_resource(grant)) == "urn:::grant/SOMEROLE?priv=ALL&on=warehouse/SOMEWH"


def test_future_grant_schemas_priv():
    grant = res.FutureGrant(priv="CREATE VIEW", on_future_schemas_in_database="somedb", to="somerole")
    assert grant.priv == "CREATE VIEW"
    assert grant.on_type == ResourceType.SCHEMA
    assert grant.in_type == ResourceType.DATABASE
    assert grant.in_name == "SOMEDB"
    assert grant.to.name == "SOMEROLE"
    assert str(URN.from_resource(grant)) == "urn:::future_grant/SOMEROLE?priv=CREATE VIEW&on=database/SOMEDB.<SCHEMA>"


def test_future_grant_anonymous_target():
    grant = res.FutureGrant(priv="SELECT", on_future_tables_in_schema="someschema", to="somerole")
    assert grant.priv == "SELECT"
    assert grant.on_type == ResourceType.TABLE
    assert grant.in_type == ResourceType.SCHEMA
    assert grant.in_name == "SOMESCHEMA"
    assert grant.to.name == "SOMEROLE"
    assert str(URN.from_resource(grant)) == "urn:::future_grant/SOMEROLE?priv=SELECT&on=schema/SOMESCHEMA.<TABLE>"


def test_future_grant_anonymous_nested_target():
    grant = res.FutureGrant(priv="SELECT", on_future_tables_in_schema="somedb.someschema", to="somerole")
    assert grant.priv == "SELECT"
    assert grant.on_type == ResourceType.TABLE
    assert grant.in_type == ResourceType.SCHEMA
    assert grant.in_name == "somedb.SOMESCHEMA"
    assert grant.to.name == "SOMEROLE"
    assert (
        str(URN.from_resource(grant)) == "urn:::future_grant/SOMEROLE?priv=SELECT&on=schema/SOMEDB.SOMESCHEMA.<TABLE>"
    )


def test_future_grant_referenced_inferred_target():
    schema = res.Schema(name="somedb.someschema")
    grant = res.FutureGrant(priv="SELECT", on_future_tables_in=schema, to="somerole")
    assert grant.priv == "SELECT"
    assert grant.on_type == ResourceType.TABLE
    assert grant.in_type == ResourceType.SCHEMA
    assert grant.in_name == "somedb.SOMESCHEMA"
    assert grant.to.name == "SOMEROLE"
    assert (
        str(URN.from_resource(grant)) == "urn:::future_grant/SOMEROLE?priv=SELECT&on=schema/SOMEDB.SOMESCHEMA.<TABLE>"
    )


def test_role_grant_to_user_with_kwargs():
    grant = res.RoleGrant(role="somerole", to_user="someuser")
    assert grant.role.name == "somerole"
    assert grant._data.to_user is not None
    assert grant.to.name == "someuser"
    assert str(URN.from_resource(grant)) == "urn:::role_grant/SOMEROLE?user=SOMEUSER"


def test_role_grant_to_user_with_resource():
    grant = res.RoleGrant(role="somerole", to=res.User(name="someuser"))
    assert grant.role.name == "somerole"
    assert grant._data.to_user is not None
    assert grant.to.name == "someuser"
    assert str(URN.from_resource(grant)) == "urn:::role_grant/SOMEROLE?user=SOMEUSER"


def test_role_grant_to_role_with_kwargs():
    grant = res.RoleGrant(role="somerole", to_role="someotherrole")
    assert grant.role.name == "somerole"
    assert grant._data.to_role is not None
    assert grant.to.name == "someotherrole"
    assert str(URN.from_resource(grant)) == "urn:::role_grant/SOMEROLE?role=SOMEOTHERROLE"


def test_role_grant_to_role_with_resource():
    grant = res.RoleGrant(role="somerole", to=res.Role(name="someotherrole"))
    assert grant.role.name == "somerole"
    assert grant._data.to_role is not None
    assert grant.to.name == "someotherrole"
    assert str(URN.from_resource(grant)) == "urn:::role_grant/SOMEROLE?role=SOMEOTHERROLE"


def test_grant_redirect_to_all():
    with pytest.raises(ValueError):
        res.Grant(priv="CREATE VIEW", on_all_schemas_in_database="somedb", to="somerole")


def test_grant_on_all():
    grant = res.GrantOnAll(priv="CREATE VIEW", on_all_schemas_in_database="somedb", to="somerole")
    assert grant._data.priv == "CREATE VIEW"
    assert grant._data.on_type == ResourceType.SCHEMA
    assert grant._data.in_type == ResourceType.DATABASE
    assert grant._data.in_name == "somedb"
    assert grant._data.to.name == "somerole"


def test_grant_refs():
    grant = res.Grant.from_sql("GRANT READ ON IMAGE REPOSITORY some_repo TO ROLE titan_app_admin")
    repo = res.ImageRepository(
        name="titan_app_image_repo",
        owner="titan_app_admin",
        database="titan_app_db",
        schema="titan_app",
    )
    assert ResourceName(str(repo.fqn)) == ResourceName("titan_app_db.titan_app.titan_app_image_repo")
    assert grant.priv == "READ"
    assert ResourcePointer("some_repo", ResourceType.IMAGE_REPOSITORY) in grant.refs


def test_grant_priv_is_serialized_uppercase():
    grant = res.Grant(priv="usage", on_warehouse="somewh", to="somerole")
    assert grant.priv == "USAGE"


def test_grant_on_accepts_resource_name():
    wh = res.Warehouse(name="somewh")
    assert isinstance(wh.name, ResourceName)
    grant = res.Grant(priv="usage", on_warehouse=wh.name, to="somerole")
    assert grant.on == "SOMEWH"
    assert grant.on_type == ResourceType.WAREHOUSE
