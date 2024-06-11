import os
import sys


# Set repo_root to the parent directory that this file lives in
REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def main(resource_name):
    resource_path = os.path.join(REPO_ROOT, "titan", "resources", f"{resource_name}.py")
    sql_fixture_path = os.path.join(REPO_ROOT, "tests", "fixtures", "sql", f"{resource_name}.sql")
    json_fixture_path = os.path.join(REPO_ROOT, "tests", "fixtures", "json", f"{resource_name}.json")

    # Create the resource file
    with open(resource_path, "w") as f:
        f.write("# This is the resource file for " + resource_name)

    # Create the SQL fixture file
    with open(sql_fixture_path, "w") as f:
        f.write("-- SQL fixture for " + resource_name)

    # Create the JSON fixture file
    with open(json_fixture_path, "w") as f:
        f.write("")


if __name__ == "__main__":
    main(sys.argv[1])
