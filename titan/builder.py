class SQL:
    def __init__(self, *parts, _use_role=None):
        self.parts = parts
        self.use_role = _use_role

    def __str__(self):
        return tidy_sql(*self.parts)


def tidy_sql(*parts):
    if isinstance(parts[0], list):
        parts = parts[0]
    return " ".join([str(part) for part in parts if part])
