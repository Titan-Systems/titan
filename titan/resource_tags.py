class ResourceTags:
    def __init__(self, tags: dict[str, str]):
        self.tags = {}
        if isinstance(tags, ResourceTags):
            tags = tags.to_dict()
        for key, value in tags.items():
            self[key] = value

    def __setitem__(self, key, value):
        if not isinstance(key, str) or not isinstance(value, str):
            raise ValueError("Keys and values must be strings.")
        if len(key) > 256:
            raise ValueError("Keys cannot be longer than 256 characters.")
        if len(self.tags) >= 50:
            raise ValueError("Cannot have more than 50 key-value pairs.")
        self.tags[key] = value

    def __getitem__(self, key):
        return self.tags[key]

    def __hash__(self):
        return hash(frozenset(self.tags.items()))

    def to_dict(self):
        return self.tags.copy()

    def tag_names(self):
        return self.tags.keys()

    def items(self):
        return self.tags.items()

    def keys(self):
        return self.tags.keys()

    def values(self):
        return self.tags.values()
