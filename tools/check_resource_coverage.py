import titan
import titan.data_provider

from titan import Resource
from titan.identifiers import resource_label_for_type

import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


from tests.helpers import get_json_fixtures, get_sql_fixtures

# from titan import resources as res

from tabulate import tabulate, SEPARATING_LINE

CRITICAL = [
    "account",
    "database",
    "schema",
    "shared_database",
    "table",
    "view",
    "user",
    "role",
    "warehouse",
]

JSON_FIXTURES = dict(list(get_json_fixtures()))
SQL_FIXTURES = set([resource_cls for resource_cls, _, _ in get_sql_fixtures()])


def check_resource_coverage():

    for x, y, z in get_sql_fixtures():
        print(">>>", x, y, z)

    resources = []
    polymorphic_resources = set()
    for resource_type, classes in Resource.__types__.items():
        if len(classes) > 1:
            polymorphic_resources.update(classes)
        for cls in classes:
            # print(resource_type, "->", cls.__name__)
            resources.append(cls)

    def _resource_sort(resource: Resource):
        return (
            str(resource.scope.__class__),
            str(resource.resource_type),
            resource.__name__,
        )

    sorted_resources = sorted(resources, key=_resource_sort)

    headers = {
        "name": "Resource Name",
        "json": "json",
        "sql": "sql",
        "fetch": "fetch",
        "stable": "stable",
    }
    audits = []

    current_scope = None
    current_data_type = None
    for resource in sorted_resources:
        if resource.scope.__class__ != current_scope:
            # print(">>>", resource.scope.__class__)
            current_scope = resource.scope.__class__
            audits.append({"name": f"**{resource.scope.__class__.__name__}**"})

        if resource.resource_type != current_data_type:
            current_data_type = resource.resource_type
            if resource in polymorphic_resources:
                audits.append({"name": str(resource.resource_type).title()})

        resource_label = resource_label_for_type(resource.resource_type)

        name = f"↳ {resource.__name__}" if resource in polymorphic_resources else resource.__name__
        has_json = resource in JSON_FIXTURES
        has_sql = resource in SQL_FIXTURES
        has_fetch = hasattr(titan.data_provider, f"fetch_{resource_label}")
        is_stable = all([has_json, has_sql, has_fetch])

        audit = {
            "name": name,
            "json": "✔" if has_json else "-",
            "sql": "✔" if has_sql else "-",
            "fetch": "✔" if has_fetch else "-",
            "stable": "✔" if is_stable else "-",
        }
        audits.append(audit)

    print(tabulate(audits, headers=headers))


def _check_resource_coverage():
    critical = []
    all_others = []
    classes = sorted(titan.Resource.classes.items())
    for resource_key, resource_cls in classes:
        if resource_cls == titan.Resource:
            continue
        resource_type = getattr(resource_cls, "resource_type", "--")
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
        fetch = hasattr(titan.data_provider, f"fetch_{resource_key}")

        if resource_key in CRITICAL:
            add_to = critical
        else:
            add_to = all_others

        add_to.append(
            [
                resource_cls.__name__,
                resource_type,
                "✔" if from_sql else "-",
                "✔" if create_sql else "-",
                lifecycle if lifecycle else "----",
                "✔" if fetch else "-",
            ]
        )

    headers = [
        "Resource Name",
        "type",
        "< SQL",
        "> SQL",
        "privs",
        "fetch",
    ]

    print(tabulate(critical + [SEPARATING_LINE] + all_others, headers=headers))


if __name__ == "__main__":
    check_resource_coverage()
