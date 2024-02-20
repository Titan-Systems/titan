from titan.enums import ResourceType
from titan.resources import Database, Grant, Role, Schema, Warehouse

"""
    GRANT {  { globalPrivileges         | ALL [ PRIVILEGES ] } ON ACCOUNT
        | { accountObjectPrivileges  | ALL [ PRIVILEGES ] } ON { USER | RESOURCE MONITOR | WAREHOUSE | DATABASE | INTEGRATION | FAILOVER GROUP | REPLICATION GROUP } <object_name>
        | { schemaPrivileges         | ALL [ PRIVILEGES ] } ON { SCHEMA <schema_name> | ALL SCHEMAS IN DATABASE <db_name> }
        | { schemaPrivileges         | ALL [ PRIVILEGES ] } ON { FUTURE SCHEMAS IN DATABASE <db_name> }
        | { schemaObjectPrivileges   | ALL [ PRIVILEGES ] } ON { <object_type> <object_name> | ALL <object_type_plural> IN { DATABASE <db_name> | SCHEMA <schema_name> } }
        | { schemaObjectPrivileges   | ALL [ PRIVILEGES ] } ON FUTURE <object_type_plural> IN { DATABASE <db_name> | SCHEMA <schema_name> }
        }
    TO [ ROLE ] <role_name> [ WITH GRANT OPTION ]

"""


# def test_grant_global_priv():
#     grant = Grant(priv="ALL", on="ACCOUNT", to="somerole")
#     assert grant.priv == "ALL"
#     assert grant.on == "ACCOUNT"
#     assert grant.to.name == "somerole"


def test_grant_account_obj_priv_with_resource():
    wh = Warehouse(name="somewh")
    grant = Grant(priv="MODIFY", on=wh, to="somerole")
    assert grant.priv == "MODIFY"
    assert grant.on == "SOMEWH"
    assert grant.on_type == ResourceType.WAREHOUSE
    assert grant.to.name == "SOMEROLE"


def test_grant_account_obj_priv_with_kwarg():
    grant = Grant(priv="MODIFY", on_warehouse="somewh", to="somerole")
    assert grant.priv == "MODIFY"
    assert grant.on == "SOMEWH"
    assert grant.on_type == ResourceType.WAREHOUSE
    assert grant.to.name == "SOMEROLE"


def test_grant_schema_priv_with_resource():
    sch = Schema(name="someschema")
    grant = Grant(priv="CREATE VIEW", on=sch, to="somerole")
    assert grant.priv == "CREATE VIEW"
    assert grant.on == "SOMESCHEMA"
    assert grant.on_type == ResourceType.SCHEMA
    assert grant.to.name == "SOMEROLE"


def test_grant_schema_priv_with_kwarg():
    grant = Grant(priv="CREATE VIEW", on_schema="someschema", to="somerole")
    assert grant.priv == "CREATE VIEW"
    assert grant.on == "SOMESCHEMA"
    assert grant.on_type == ResourceType.SCHEMA
    assert grant.to.name == "SOMEROLE"


# def test_grant_future_schemas_priv():
#     grant = Grant(priv="CREATE VIEW", on_future_schemas_in_database="somedb", to="somerole")
#     assert grant.priv == "CREATE VIEW"
#     assert grant.on == "database somedb"
#     assert grant.on_future == "SCHEMAS"
#     assert grant.to.name == "somerole"


# def test_grant_all_schemas_priv():
#     grant = Grant(priv="CREATE VIEW", on_all_schemas_in_database="somedb", to="somerole")
#     assert grant.priv == "CREATE VIEW"
#     assert grant.on == "database somedb"
#     assert grant.on_all == "SCHEMAS"
#     assert grant.to == "somerole"
