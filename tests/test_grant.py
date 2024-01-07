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


def test_grant_global_priv():
    grant = Grant(priv="ALL", on="ACCOUNT", to="somerole")
    assert grant.priv == "ALL"
    assert grant.on == "ACCOUNT"
    assert grant.to == Role(name="somerole", stub=True)


def test_grant_account_obj_priv_with_resource():
    grant = Grant(priv="MODIFY", on=Warehouse(name="somewh"), to="somerole")
    assert grant.priv == "MODIFY"
    assert grant.on == Warehouse(name="somewh")
    assert grant.to == Role(name="somerole", stub=True)


def test_grant_account_obj_priv_with_kwarg():
    grant = Grant(priv="MODIFY", on_warehouse="somewh", to="somerole")
    assert grant.priv == "MODIFY"
    assert grant.on == "warehouse somewh"
    assert grant.to == Role(name="somerole", stub=True)


def test_grant_schema_priv_with_resource():
    grant = Grant(priv="CREATE VIEW", on=Schema(name="someschema"), to="somerole")
    assert grant.priv == "CREATE VIEW"
    assert grant.on == Schema(name="someschema")
    assert grant.to == Role(name="somerole", stub=True)


def test_grant_schema_priv_with_kwarg():
    grant = Grant(priv="CREATE VIEW", on_schema="someschema", to="somerole")
    assert grant.priv == "CREATE VIEW"
    assert grant.on == "schema someschema"
    assert grant.to == Role(name="somerole", stub=True)


# def test_grant_future_schemas_priv():
#     grant = Grant(priv="CREATE VIEW", on_future_schemas_in_database="somedb", to="somerole")
#     assert grant.priv == "CREATE VIEW"
#     assert grant.on == Database(name="somedb", stub=True)
#     assert grant.on_future == "SCHEMAS"
#     assert grant.to == Role(name="somerole", stub=True)


# def test_grant_all_schemas_priv():
#     grant = Grant(priv="CREATE VIEW", on_all_schemas_in_database="somedb", to="somerole")
#     assert grant.priv == "CREATE VIEW"
#     assert grant.on == Database(name="somedb", stub=True)
#     assert grant.on_all == "SCHEMAS"
#     assert grant.to == Role(name="somerole", stub=True)
