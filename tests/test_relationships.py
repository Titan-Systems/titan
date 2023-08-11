import unittest

from titan import Account, Database, DatabaseRole, Schema, Table, User, View, Warehouse


class TestRelationships(unittest.TestCase):
    def validate_parent_child_relationships(self, parent, children):
        for child in children:
            self.assertIn(child, parent.children)
            self.assertEqual(parent, child.parent)

    def test_relationships_account_scoped(self):
        parent = Account(name="XYZ123")
        # child1 = Database(name="db", account=parent)
        # child2 = Warehouse(name="wh", parent=parent)
        child = User(name="usr")
        parent.children.add(child)
        self.validate_parent_child_relationships(parent, [child])

    def test_relationships_database_scoped(self):
        parent = Database(name="db")
        child1 = Schema(name="sch")
        child2 = DatabaseRole(name="role")
        parent.children.add(child1, child2)
        self.validate_parent_child_relationships(parent, [child1, child2])

    def test_relationships_schema_scoped(self):
        parent = Schema(name="sch")
        tbl = Table(name="tbl")
        vw = View(name="view", as_="SELECT 1")
        parent.children.add(tbl, vw)
        self.validate_parent_child_relationships(parent, [tbl, vw])

    def test_relationships_via_constructor(self):
        parent = Account(name="XYZ123")
        child1 = Database(name="db", account=parent)
        child2 = Warehouse(name="wh", parent=parent)
        self.validate_parent_child_relationships(parent, [child1, child2])
