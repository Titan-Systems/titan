import titan

from tabulate import tabulate


def check_resource_coverage():
    table = []
    classes = sorted(titan.Resource.classes.items())
    for resource_key, resource_cls in classes:
        if resource_cls == titan.Resource:
            continue
        from_sql = hasattr(resource_cls, "from_sql")
        create_sql = hasattr(resource_cls, "create_sql")
        fetch = hasattr(titan.DataProvider, f"fetch_{resource_key}")
        create = hasattr(titan.DataProvider, f"create_{resource_key}")
        update = hasattr(titan.DataProvider, f"update_{resource_key}")
        drop = hasattr(titan.DataProvider, f"drop_{resource_key}")
        table.append(
            [
                resource_cls.__name__,
                "✔" if from_sql else "-",
                "✔" if create_sql else "-",
                "✔" if fetch else "-",
                "✔" if create else "-",
                "✔" if update else "-",
                "✔" if drop else "-",
            ]
        )

    print(tabulate(table, headers=["Resource Name", "from SQL", "to SQL", "fetch", "create", "update", "drop"]))


if __name__ == "__main__":
    check_resource_coverage()
