import titan

from tabulate import tabulate, SEPARATING_LINE

CRITICAL = [
    "database",
    "schema",
    "shared_database",
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
        lifecycle = None
        if resource_cls.lifecycle_privs:
            create = len(resource_cls.lifecycle_privs.create) > 0
            read = len(resource_cls.lifecycle_privs.read) > 0
            write = len(resource_cls.lifecycle_privs.write) > 0
            delete = len(resource_cls.lifecycle_privs.delete) > 0
            lifecycle = "".join(
                ["C" if create else "-", "R" if read else "-", "W" if write else "-", "D" if delete else "-"]
            )
        fetch = hasattr(titan.DataProvider, f"fetch_{resource_key}")

        if resource_key in CRITICAL:
            add_to = critical
        else:
            add_to = all_others

        add_to.append(
            [
                resource_cls.__name__,
                "✔" if from_sql else "-",
                "✔" if create_sql else "-",
                lifecycle if lifecycle else "----",
                "✔" if fetch else "-",
            ]
        )

    headers = [
        "Resource Name",
        "from SQL",
        "to SQL",
        "lifecycle",
        "fetch",
    ]

    print(tabulate(critical + [SEPARATING_LINE] + all_others, headers=headers))


if __name__ == "__main__":
    check_resource_coverage()
