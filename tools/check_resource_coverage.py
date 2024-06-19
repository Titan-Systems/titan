import sys
import os
import re

import titan
import titan.data_provider

from titan import Resource
from titan.identifiers import resource_label_for_type

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


from tests.helpers import get_json_fixtures, get_sql_fixtures

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


# Get the path to the parent directory's docs folder
docs_path = os.path.join(os.path.dirname(__file__), "..", "docs", "resources")

# List all files in the docs directory that end with .md
DOCS = [f[:-3] for f in os.listdir(docs_path) if f.endswith(".md")]


def camelcase_to_snakecase(name: str) -> str:
    pattern = re.compile(r"(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")
    name = pattern.sub("_", name).lower()
    return name


def check_resource_coverage():

    print(DOCS)

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
        "docs": "docs",
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
        has_docs = camelcase_to_snakecase(resource.__name__) in DOCS
        is_stable = all([has_json, has_sql, has_fetch, has_docs])

        print(resource_label)

        audit = {
            "name": name,
            "json": "✔" if has_json else "-",
            "sql": "✔" if has_sql else "-",
            "fetch": "✔" if has_fetch else "-",
            "docs": "✔" if has_docs else "-",
            "stable": "✅" if is_stable else "-",
        }
        audits.append(audit)

    print(tabulate(audits, headers=headers, tablefmt="rounded_grid"))


if __name__ == "__main__":
    check_resource_coverage()
