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

[Snowflake Documentation]({snowflake_docs})

## Example

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


def parse_resource_docstring(docstring):
    sections = {
        "Description": "",
        "Snowflake Docs": "",
        "Fields": "",
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
    current_section = None
    for part in parts:
        if part.startswith("Description:"):
            current_section = "Description"
            part = part[11:].strip()
        elif part.startswith("Snowflake Docs:"):
            current_section = "Snowflake Docs"
            part = part[13:].strip()
        elif part.startswith("Fields:"):
            current_section = "Fields"
            part = part[6:].strip()
        elif part.startswith("Python:"):
            current_section = "Python"
            part = part[7:].strip()
        elif part.startswith("Yaml:"):
            current_section = "Yaml"
            part = part[5:].strip()

        if current_section:
            sections[current_section] += _strip_leading_spaces(part)
        else:
            raise ValueError(f"Unknown section: {part}")

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
                docs_page = re.sub(r"(?<!^)(?=[A-Z])", "_", word).lower()
                linked_word = f"[{word}]({docs_page}.md)"
                linked_words.append(linked_word)
            else:
                linked_words.append(word)
        field["field_type"] = " ".join(linked_words)
        new_fields.append(field)
    return new_fields


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

    fields = parsed["Fields"].split("\n")
    fields = [parse_field_docstring(field) for field in fields]
    fields = enrich_fields(fields)
    fields_md = "".join([field_template.format(**field) for field in fields])
    # print(fields_md)
    # return

    with open(os.path.join(DOCS_ROOT, "resources", f"{resource_type}.md"), "w") as f:
        f.write(
            doc_template.format(
                resource_title=resource_title,
                description=parsed["Description"],
                snowflake_docs=parsed["Snowflake Docs"],
                fields=fields_md,
                python_example=parsed["Python"],
                yaml_example=parsed["Yaml"],
            )
        )


def main():
    for res in ["user", "role", "warehouse"]:
        try:
            generate_resource_doc(res)
        except Exception as e:
            print(f"Error generating {res} doc: {e}")


if __name__ == "__main__":
    main()
