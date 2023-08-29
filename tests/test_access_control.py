from titan.access_control import ACL, SuperPriv
from titan.privs import DatabasePriv, GlobalPriv, SchemaPriv, TablePriv
from titan.resources import Database, OwnershipGrant, PrivGrant, Role, Schema, Table


def test_create():
    db = Database(name="somedb")
    sch = Schema(name="someschema")
    tbl = Table(name="sometable", columns=["id INT"])
    role = Role(name="somerole")
    acl = ACL(privs=[SuperPriv.CREATE], roles=[role], resources=[db, sch, tbl])
    grants = acl.grants()
    assert grants == [
        PrivGrant(privs=[GlobalPriv.CREATE_DATABASE], on=db, to=role),
        PrivGrant(privs=[DatabasePriv.CREATE_SCHEMA], on=sch, to=role),
        PrivGrant(privs=[SchemaPriv.CREATE_TABLE], on=tbl, to=role),
    ]


def test_delete():
    db = Database(name="somedb")
    role = Role(name="somerole")
    acl = ACL(privs=[SuperPriv.DELETE], roles=[role], resources=[db])
    grants = acl.grants()
    assert len(grants) == 1
    assert OwnershipGrant(on=db, to=role) in grants


def test_read():
    tbl = Table(name="sometbl", columns=["id INT"])
    role = Role(name="somerole")
    acl = ACL(privs=[SuperPriv.READ], roles=[role], resources=[tbl])
    grants = acl.grants()
    assert len(grants) == 1
    assert PrivGrant(privs=TablePriv.SELECT, on=tbl, to=role) in grants


def test_write():
    tbl = Table(name="sometbl", columns=["id INT"])
    role = Role(name="somerole")
    acl = ACL(privs=[SuperPriv.WRITE], roles=[role], resources=[tbl])
    grants = acl.grants()
    assert len(grants) == 4
    assert PrivGrant(privs=TablePriv.INSERT, on=tbl, to=role) in grants
    assert PrivGrant(privs=TablePriv.UPDATE, on=tbl, to=role) in grants
    assert PrivGrant(privs=TablePriv.DELETE, on=tbl, to=role) in grants
    assert PrivGrant(privs=TablePriv.TRUNCATE, on=tbl, to=role) in grants
