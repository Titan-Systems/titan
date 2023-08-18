_refs = []
track_ref = _refs.append


def capture_refs():
    global _refs
    refs = list(_refs)
    _refs.clear()
    return refs


def raise_if_hanging_refs():
    global _refs
    if _refs:
        raise Exception(f"Hanging refs: {_refs}")


class SQL:
    def __init__(self, sql: str):
        self.refs = capture_refs()
        self.sql = sql
