import ast
import re
import os

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOCS_ROOT = os.path.join(REPO_ROOT, "docs")
RESOURCES_ROOT = os.path.join(REPO_ROOT, "titan", "resources")

doc_template = """\
---
description: >-
  {description}
---

# {resource_title}

## Example

### Python

{python_example}

### YAML

{yaml_example}

## Fields

{args}

* `name` (required) - Identifier for the virtual warehouse; must be unique for your account.
* `owner` (string or [Role](role.md)) - The role that owns this resource
* `warehouse_type` (string or [WarehouseType](warehouse.md#warehousetype)

## Enums

### WarehouseType

* STANDARD
* SNOWPARK-OPTIMIZED

"""


def parse_resource_docstring(docstring):
    sections = {
        "Description": "",
        "Args": "",
        "Python": "",
        "Yaml": "",
    }

    if not docstring:
        return sections

    def _strip_leading_spaces(text):
        lines = text.split("\n")
        return "\n".join(line[4:] for line in lines).strip()

    # Normalize line breaks
    docstring = docstring.strip()

    # Split the docstring into sections
    parts = re.split(r"\n\s*\n", docstring)

    # Identify sections

    for part in parts:
        if part.startswith("Description:"):
            current_section = "Description"
            part = part[11:].strip()
        elif part.startswith("Args:"):
            current_section = "Args"
            part = part[5:].strip()
        elif part.startswith("Python:"):
            current_section = "Python"
            part = part[7:].strip()
        elif part.startswith("Yaml:"):
            current_section = "Yaml"
            part = part[5:].strip()

        sections[current_section] += _strip_leading_spaces(part)

    return sections


def get_resource_docstring(resource_type: str):
    resource_file = os.path.join(RESOURCES_ROOT, f"{resource_type}.py")
    with open(resource_file, "r") as f:
        tree = ast.parse(f.read(), filename=resource_file)

    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            if node.name == resource_type.capitalize():
                return ast.get_docstring(node)


def generate_resource_doc(resource_type: str):

    resource_title = resource_type.replace("_", " ").title()
    resource_docstring = get_resource_docstring(resource_type)
    parsed = parse_resource_docstring(resource_docstring)
    with open(os.path.join(DOCS_ROOT, "resources", f"{resource_type}.md"), "w") as f:
        f.write(
            doc_template.format(
                resource_title=resource_title,
                description=parsed["Description"],
                args=parsed["Args"],
                python_example=parsed["Python"],
                yaml_example=parsed["Yaml"],
            )
        )


def main():
    generate_resource_doc("user")


if __name__ == "__main__":
    main()
