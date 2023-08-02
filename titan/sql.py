_refs = []
add_ref = _refs.append


def capture_refs():
    global _refs
    refs = list(_refs)
    _refs.clear()
    return refs


def SQL(sql: str):
    capture_refs()
    return sql
