from titan import resources as res
from titan import var
from titan.var import VarString


def test_blueprint_vars_comparison_with_system_names():
    database = res.Database(name=var.database_name)
    assert isinstance(database.name, VarString)

    schema = res.Schema(name=var.schema_name)
    assert isinstance(schema.name, VarString)


def test_vars_in_owner():
    schema = res.Schema(name="schema", owner="role_{{ var.role_name }}")
    assert isinstance(schema._data.owner, VarString)


def test_vars_database_role():
    role = res.DatabaseRole(name="role_{{ var.role_name }}", database="db_{{ var.db_name }}")
    assert isinstance(role._data.name, VarString)
    assert isinstance(role._data.database, VarString)
