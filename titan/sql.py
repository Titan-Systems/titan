_refs = set()

add_ref = _refs.add


def capture_refs():
    global _refs
    refs = list(_refs)
    _refs.clear()
    return refs


class SQL:
    def __init__(self, sql: str) -> None:
        self.refs = capture_refs()
        self._sql = sql

    def __str__(self):
        return self._sql
