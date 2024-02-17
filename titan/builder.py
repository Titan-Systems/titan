def tidy_sql(*parts):
    if isinstance(parts[0], list):
        parts = parts[0]
    return " ".join([str(part) for part in parts if part != "" and part is not None])
