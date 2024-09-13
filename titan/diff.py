from enum import Enum

from .resource_name import ResourceName, attribute_is_resource_name


class Action(Enum):
    CREATE = "create"
    DROP = "drop"
    UPDATE = "update"
    TRANSFER = "transfer"


def eq(lhs, rhs, key):
    if lhs is None or rhs is None:
        return lhs == rhs

    if attribute_is_resource_name(key):
        return ResourceName(lhs) == ResourceName(rhs)
    elif key == "args":
        # Ignore arg defaults
        def _scrub_defaults(args):
            new_args = []
            for arg in args:
                new_arg = arg.copy()
                new_arg.pop("default", None)
                new_args.append(new_arg)
            return new_args

        lhs_copy = _scrub_defaults(lhs)
        rhs_copy = _scrub_defaults(rhs)
        return lhs_copy == rhs_copy
    elif key == "columns":
        if len(lhs) != len(rhs):
            return False
        for i, lhs_col in enumerate(lhs):
            rhs_col = rhs[i]
            for col_key, lhs_value in lhs_col.items():
                if col_key not in rhs_col:
                    # raise Exception(f"Column {col_key} not found in rhs {rhs}")
                    continue
                rhs_value = rhs_col[col_key]
                if not eq(lhs_value, rhs_value, col_key):
                    return False
        return True
    else:
        return lhs == rhs


def dict_delta(original, new):
    original_keys = set(original.keys())
    new_keys = set(new.keys()) - {"_pointer", "_implicit"}

    delta = {}

    for key in original_keys - new_keys:
        delta[key] = None

    for key in original_keys & new_keys:
        if not eq(original[key], new[key], key):
            delta[key] = new[key]

    for key in new_keys - original_keys:
        delta[key] = new[key]

    if "_implicit" in delta:
        raise Exception(f"Unexpected implicit resource {delta}")
    return delta


def diff(original, new):
    original_keys = set(original.keys())
    new_keys = set(new.keys())

    # Resources in remote state but not in the manifest should be removed
    for key in original_keys - new_keys:
        yield Action.DROP, key, original[key]

    # Resources in the manifest but not in remote state should be added
    for key in new_keys - original_keys:
        if new[key].get("_pointer", False):
            raise Exception(f"Blueprint has pointer to resource that doesn't exist or isn't visible in session: {key}")

        # We don't create implicit resources
        if new[key].get("_implicit", False):
            continue

        yield Action.CREATE, key, new[key]

    # Resources in both should be compared
    for key in original_keys & new_keys:
        if not isinstance(original[key], dict):
            raise RuntimeError(f"Unexpected type for resource {key}: {type(original[key])}")

        # We don't diff resource pointers
        if new[key].get("_pointer", False):
            continue

        delta = dict_delta(original[key], new[key])
        owner_attr = delta.pop("owner", None)

        for attr, value in delta.items():
            yield Action.UPDATE, key, {attr: value}

        # Force the transfer to happen after all other attribute changes
        if owner_attr:
            yield Action.TRANSFER, key, {"owner": owner_attr}
