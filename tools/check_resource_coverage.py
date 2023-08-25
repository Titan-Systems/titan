import titan

from tabulate import tabulate, SEPARATING_LINE

CRITICAL = [
    "database",
    "schema",
    "table",
    "view",
    "user",
    "role",
    "warehouse",
]


def check_resource_coverage():
    critical = []
    all_others = []
    classes = sorted(titan.Resource.classes.items())
    for resource_key, resource_cls in classes:
        if resource_cls == titan.Resource:
            continue
        from_sql = hasattr(resource_cls, "from_sql")
        create_sql = hasattr(resource_cls, "create_sql")
        lifecycle = resource_cls.lifecycle_privs is not None
        fetch = hasattr(titan.DataProvider, f"fetch_{resource_key}")
        create = hasattr(titan.DataProvider, f"create_{resource_key}")
        update = hasattr(titan.DataProvider, f"update_{resource_key}")
        drop = hasattr(titan.DataProvider, f"drop_{resource_key}")

        if resource_key in CRITICAL:
            add_to = critical
        else:
            add_to = all_others

        add_to.append(
            [
                resource_cls.__name__,
                "✔" if from_sql else "-",
                "✔" if create_sql else "-",
                "✔" if lifecycle else "-",
                "✔" if fetch else "-",
                "✔" if create else "-",
                "✔" if update else "-",
                "✔" if drop else "-",
            ]
        )

    headers = [
        "Resource Name",
        "from SQL",
        "to SQL",
        "lifecycle",
        "fetch",
        "create",
        "update",
        "drop",
    ]

    print(tabulate(critical + [SEPARATING_LINE] + all_others, headers=headers))


if __name__ == "__main__":
    check_resource_coverage()
