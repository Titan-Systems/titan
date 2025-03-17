---
description: >-
  
---

# Streamlit

[Snowflake Documentation](https://docs.snowflake.com/en/sql-reference/sql/create-streamlit)

Represents a Streamlit app in Snowflake, which is a schema-scoped resource for creating interactive applications using Python code.

## Examples

### Python

```python
# Creating a Streamlit app from a stage
streamlit_stage = Streamlit(
    name="my_db.my_schema.my_streamlit",
    from_="@my_stage",
    main_file="app.py",
    title="My Streamlit App",
    query_warehouse="my_warehouse",
    comment="A sample Streamlit app from a stage",
    tags={"project": "demo"}
)

# Creating a Streamlit app from a Git repository
streamlit_repo = Streamlit(
    name="my_streamlit",
    from_="https://github.com/user/repo.git",
    version="main",
    main_file="app.py",
    title="Repo Streamlit App",
    owner="SYSADMIN"
)
```

### YAML

```yaml
streamlits:
  - name: my_db.my_schema.my_streamlit
    from: "@my_stage"
    main_file: "app.py"
    title: "My Streamlit App"
    query_warehouse: "my_warehouse"
    comment: "A sample Streamlit app from a stage"
    owner: SYSADMIN
    tags:
      project: demo
  - name: my_streamlit
    from: "https://github.com/user/repo.git"
    version: "main"
    main_file: "app.py"
    title: "Repo Streamlit App"
```

## Fields

* `name` (string, required) - The name of the Streamlit app. Can be a fully qualified name (e.g., "database.schema.app_name").
* `from_` (string, required) - The source of the Streamlit app. This can be either a stage (e.g., '@mystage') or a repository URL (e.g., 'https://github.com/user/repo.git').
* `version` (string) - The version or branch of the repository to use. Only applicable if from_ is a repository URL.
* `main_file` (string) - The name of the main Python file for the Streamlit app (e.g., 'app.py').
* `title` (string) - The display title of the Streamlit app.
* `query_warehouse` (string) - The name of the warehouse to use for queries in the app.
* `comment` (string) - A comment or description for the Streamlit app.
* `owner` (string or Role) - The role that owns the Streamlit app. Defaults to "SYSADMIN".
* `tags` (dict) - A dictionary of tags to associate with the Streamlit app.