_refs = []  # set()


add_ref = _refs.append
# def add_ref(ref):
#     _refs.add(ref.fully_qualified_name)


def capture_refs():
    global _refs
    refs = list(_refs)
    _refs.clear()
    return refs


def SQL(sql: str):
    capture_refs()
    return sql
    # return SQL(sql


# class SQL:
#     def __init__(self, sql: str) -> None:
#         self.refs = capture_refs()
#         self._sql = sql

#     def __str__(self):
#         return self._sql
