from titan import resources as res
from titan import var
from titan.var import VarString


def test_blueprint_vars_comparison_with_system_names():
    database = res.Database(name=var.database_name)
    assert isinstance(database.name, VarString)

    schema = res.Schema(name=var.schema_name)
    assert isinstance(schema.name, VarString)
