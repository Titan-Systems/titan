import requests


def git_import(sp_session, path: str):
    """
    Imports resources from a git repository.
    """
    sql = [
        f"IMPORT INTO SCHEMA {sp_session.get_current_schema()} FROM GIT URL='{git_url}'",
        f"BRANCH='{branch}'" if branch else "",
    ]
    if not dry_run:
        _execute(sp_session, sql)
    return {"sql": sql}
