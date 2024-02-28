from enum import Enum


class DiffAction(Enum):
    ADD = "add"
    REMOVE = "remove"
    CHANGE = "change"


def eq(lhs, rhs, key):
    if key != "args":
        return lhs == rhs
    else:
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


def dict_delta(original, new):
    original_keys = set(original.keys())
    new_keys = set(new.keys())

    delta = {}

    for key in original_keys - new_keys:
        delta[key] = None

    for key in original_keys & new_keys:
        if not eq(original[key], new[key], key):
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
        if isinstance(new[key], dict) and new[key].get("_pointer", False):
            raise Exception(f"Blueprint has pointer to resource that doesn't exist or isn't visible in session: {key}")
        elif isinstance(new[key], list):
            for item in new[key]:
                yield DiffAction.ADD, key, item
        else:
            yield DiffAction.ADD, key, new[key]

    # Resources in both should be comparedx
    for key in original_keys & new_keys:
        if isinstance(original[key], dict):
            # We don't diff resource pointers
            if new[key].get("_pointer", False):
                continue

            delta = dict_delta(original[key], new[key])

            for attr, value in delta.items():
                yield DiffAction.CHANGE, key, {attr: {'new_value':new[key][attr],'old_value':original[key][attr]}}

        elif isinstance(original[key], list):

            for item in original[key]:
                if item not in new[key]:
                    yield DiffAction.REMOVE, key, item

            for item in new[key]:
                if item not in original[key]:
                    yield DiffAction.ADD, key, item
