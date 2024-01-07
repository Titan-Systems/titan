import yaml


from github import Github, InputGitTreeElement

from .identifiers import ResourceLocator, FQN
from .search import crawl_resources

CREATE_MODE = "100644"


def _git_path_for_resource(resource):
    if resource["resource_key"] == "database":
        return f"{resource['resource_key']}:{resource['name']}.yaml"
    elif resource["resource_key"] == "schema":
        return f"{resource['database']}/{resource['resource_key']}:{resource['name']}.yaml"
    else:
        return "UNKNOWN"


def export(session, repo: str, path: str, locator_str: str, access_token: str):
    """
    Export resources from a Snowflake account to a GitHub repository.

        repo (str): The name of the repository to import from. Ex: "teej/titan"
    """
    locator = ResourceLocator.from_str(locator_str)

    gh = Github(access_token)
    repo = gh.get_repo(repo)

    changes = []
    for resource in crawl_resources(session, locator):
        resource_path = _git_path_for_resource(resource)
        changes.append({"path": path + "/" + resource_path, "content": yaml.dump(resource)})

    # Create a tree element for each file change
    tree_elements = []
    for file_change in changes:
        blob = repo.create_git_blob(file_change["content"], "utf-8")
        tree_elements.append(InputGitTreeElement(path=file_change["path"], mode=CREATE_MODE, type="blob", sha=blob.sha))

    # Create a tree
    tree = repo.create_git_tree(tree_elements)

    # Get the current commit (head commit of the branch)
    parent = repo.get_git_ref("heads/main").object.sha
    parent_commit = repo.get_git_commit(parent)

    # Create a new commit
    new_commit = repo.create_git_commit("commit msg", tree, [parent_commit])

    # Update the reference to point to the new commit
    repo.get_git_ref("heads/main").edit(new_commit.sha)
