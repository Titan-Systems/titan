import ast
import re
import os
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOCS_ROOT = os.path.join(REPO_ROOT, "docs")
RESOURCES_ROOT = os.path.join(REPO_ROOT, "titan", "resources")

doc_template = """\
---
description: >-
  
---

# {class_name}

[Snowflake Documentation]({snowflake_docs})

{description}

## Examples

### Python

{python_example}

### YAML

{yaml_example}

## Fields

{fields}

"""

field_template = """\
* `{field_name}` ({field_type}{is_required}) - {field_description}
"""

summary_template = """\
# Table of contents

* [Overview](README.md)

## Resources

{resources}
"""


class EmptyDocstringError(Exception):
    pass


class LegacyDocstringError(Exception):
    pass


def parse_resource_docstring(docstring):
    sections = {
        "Description": "",
        "Snowflake Docs": "",
        "Fields": "",
        "Python": "",
        "Yaml": "",
    }

    if not docstring:
        raise EmptyDocstringError()
    if docstring.strip().startswith("CREATE"):
        raise LegacyDocstringError()
    if docstring.strip().startswith("GRANT"):
        raise LegacyDocstringError()

    # Identify sections
    current_section = None
    for line in docstring.split("\n"):
        if line.startswith("Description:"):
            current_section = "Description"
            continue
        elif line.startswith("Snowflake Docs:"):
            current_section = "Snowflake Docs"
            continue
        elif line.startswith("Fields:"):
            current_section = "Fields"
            continue
        elif line.startswith("Python:"):
            current_section = "Python"
            continue
        elif line.startswith("Yaml:"):
            current_section = "Yaml"
            continue

        line = line[4:]
        if line.strip() == "":
            continue

        if current_section:
            if current_section == "Snowflake Docs":
                sections[current_section] += line
            else:
                sections[current_section] += line + "\n"
        else:
            raise ValueError(f"Unknown section: {line}")

    return sections


def parse_field_docstring(field_docstring):
    pattern = re.compile(
        r"""
        ^                           # Start of the string
        (?P<field_name>\w+)         # Capture field name: one or more word characters
        \s+                         # One or more whitespace characters
        \(                          # Literal opening parenthesis
        (?P<field_type>[^,]+?)      # Capture field type: any characters except comma, non-greedy
        (?:                         # Start of non-capturing group for optional 'required'
            ,\s+                    # Comma followed by one or more whitespace characters
            (?P<is_required>\w+)    # Capture requirement status: word characters (e.g., 'required')
        )?                          # End of non-capturing group, make it optional
        \)                          # Literal closing parenthesis
        :\s+                        # Colon followed by one or more whitespace characters
        (?P<field_description>.+)   # Capture field description: any characters until the end
        $                           # End of the string
        """,
        re.VERBOSE,
    )
    match = pattern.match(field_docstring)
    if match:
        return match.groupdict()
    else:
        raise ValueError(f"Failed to parse field docstring: {field_docstring}")


def enrich_fields(fields):
    new_fields = []
    for field in fields:

        # Add a comma if the field is required
        if field["is_required"]:
            field["is_required"] = ", required"
        else:
            field["is_required"] = ""

        # Link resources named in field_type
        field_type = field["field_type"]
        words = field_type.split()
        linked_words = []
        for word in words:
            if word[0].isupper():
                docs_page = camelcase_to_snakecase(word)
                linked_word = f"[{word}]({docs_page}.md)"
                linked_words.append(linked_word)
            else:
                linked_words.append(word)
        field["field_type"] = " ".join(linked_words)
        new_fields.append(field)
    return new_fields


def get_resource_classes_for_file(resource_file: str) -> dict[str, str]:
    with open(resource_file, "r") as f:
        tree = ast.parse(f.read(), filename=resource_file)

    resource_classes = {}
    for node in tree.body:
        if isinstance(node, ast.ClassDef):
            if "Resource" in [base.id for base in node.bases]:
                docstring = ast.get_docstring(node)
                resource_classes[node.name] = docstring

    return resource_classes


def camelcase_to_snakecase(name: str) -> str:
    pattern = re.compile(r"(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")
    name = pattern.sub("_", name).lower()
    return name


def generate_resource_doc(resource_class_name: str, resource_docstring: str):

    doc_file_name = camelcase_to_snakecase(resource_class_name)
    parsed = parse_resource_docstring(resource_docstring)

    fields = parsed["Fields"].split("\n")
    fields = [parse_field_docstring(field) for field in fields if field]
    fields = enrich_fields(fields)
    fields_md = "".join([field_template.format(**field) for field in fields])
    # print(fields_md)
    # return

    with open(os.path.join(DOCS_ROOT, "resources", f"{doc_file_name}.md"), "w") as f:
        f.write(
            doc_template.format(
                class_name=resource_class_name,
                description=parsed["Description"],
                snowflake_docs=parsed["Snowflake Docs"],
                fields=fields_md,
                python_example=parsed["Python"],
                yaml_example=parsed["Yaml"],
            )
        )


def generate_summary(generated_docs):
    with open(os.path.join(DOCS_ROOT, "SUMMARY.md"), "w") as f:
        resources = ""
        for class_name in generated_docs:
            resources += f"* [{class_name}](resources/{camelcase_to_snakecase(class_name)}.md)\n"
        output = summary_template.format(resources=resources)
        f.write(output)


def main(resource_file: str):
    generated_docs = []
    for file in os.listdir(RESOURCES_ROOT):
        if file.endswith(".py"):
            if file == "resource.py":
                continue
            if resource_file and file != f"{resource_file}.py":
                continue
            classes = get_resource_classes_for_file(os.path.join(RESOURCES_ROOT, file))
            for class_name, class_docstring in classes.items():
                try:
                    generate_resource_doc(class_name, class_docstring)
                    generated_docs.append(class_name)
                except EmptyDocstringError:
                    print(f"[{file}] » {class_name} has no docstring")
                except LegacyDocstringError:
                    print(f"[{file}] » {class_name} has a legacy docstring")
                except ValueError as e:
                    print(f"[{file}] » {class_name} has an invalid docstring: {e}")
                # except Exception as e:
                #     print(f"[{file}] » Error generating {class_name}")
    if resource_file is None:
        generate_summary(sorted(generated_docs))


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else None)
