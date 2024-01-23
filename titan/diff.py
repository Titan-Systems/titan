from enum import Enum


class DiffAction(Enum):
    ADD = "add"
    REMOVE = "remove"
    CHANGE = "change"


def dict_delta(original, new):
    original_keys = set(original.keys())
    new_keys = set(new.keys())

    delta = {}

    for key in original_keys - new_keys:
        delta[key] = None

    for key in original_keys & new_keys:
        if original[key] != new[key]:
            delta[key] = new[key]

    for key in new_keys - original_keys:
        delta[key] = new[key]

    return delta


def diff(original, new):
    original_keys = set(original.keys())
    new_keys = set(new.keys())

    # Resources in remote state but not in the manifest should be removed
    for key in original_keys - new_keys:
        yield DiffAction.REMOVE, key, original[key]

    # Resources in the manifest but not in remote state should be added
    for key in new_keys - original_keys:
        if isinstance(new[key], dict) and new[key].get("_stub", False):
            raise Exception(f"Stubbed resource doesn't exist or isn't visible in session: {key}")
        elif isinstance(new[key], list):
            for item in new[key]:
                yield DiffAction.ADD, key, item
        else:
            yield DiffAction.ADD, key, new[key]

    for key in original_keys & new_keys:
        if isinstance(original[key], dict):
            # We don't diff stubbed resources
            if new[key].get("_stub", False):
                continue
            delta = dict_delta(original[key], new[key])
            for attr, value in delta.items():
                yield DiffAction.CHANGE, key, {attr: value}
        elif isinstance(original[key], list):
            for item in original[key]:
                if item not in new[key]:
                    yield DiffAction.REMOVE, key, item
            for item in new[key]:
                if item not in original[key]:
                    yield DiffAction.ADD, key, item
