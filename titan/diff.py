from enum import Enum
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List

from .resource_name import ResourceName


class DiffAction(Enum):
    ADD = "add"
    REMOVE = "remove"
    CHANGE = "change"

@dataclass(unsafe_hash=True)
class DictDiff:
    """
    Represents a change between two dictionaries.
    action: The type of change (add, remove, change)
    key: The key in the dictionary that changed
    old_value: The old value (or None if the change is an add)
    new_value: The new value (or None if the change is a remove)
    """
    action: DiffAction
    key: str
    old_value: Any = None
    new_value: Any = None


def eq(lhs, rhs, key):
    special_cases = {"args", "name"}

    if key not in special_cases:
        return lhs == rhs
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
    elif key == "name":
        return ResourceName(lhs) == ResourceName(rhs)


def dict_delta(original:Dict, new:Dict) -> Dict:
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


def diff(original:Dict[str,Dict], new:Dict[str,Dict]) -> Iterable[DictDiff]:
    """
    Generates a list of differences between the remote state ("original") and the manifest ("new").
    Both are dictionaries keyed on the resource URN.
    """
    original_keys = set(original.keys())
    new_keys = set(new.keys())

    # Resources in remote state but not in the manifest should be removed
    for key in original_keys - new_keys:
        yield DictDiff(action=DiffAction.REMOVE, key=key, old_value=original[key])

    # Resources in the manifest but not in remote state should be added
    for key in new_keys - original_keys:
        if isinstance(new[key], dict) and new[key].get("_pointer", False):
            raise Exception(f"Blueprint has pointer to resource that doesn't exist or isn't visible in session: {key}")
        elif isinstance(new[key], list):
            for item in new[key]:
                yield DictDiff(action=DiffAction.ADD, key=key, new_value=item)
        else:
            yield DictDiff(action=DiffAction.ADD, key=key, new_value=new[key])

    # Resources in both should be comparedx
    for key in original_keys & new_keys:
        if isinstance(original[key], dict):
            # We don't diff resource pointers
            if new[key].get("_pointer", False):
                continue

            delta = dict_delta(original[key], new[key])

            for attr, value in delta.items():
                yield DictDiff(action=DiffAction.CHANGE, key=key, old_value=original[key][attr], new_value=new[key][attr])

        elif isinstance(original[key], list):

            for item in original[key]:
                if item not in new[key]:
                    yield DictDiff(action=DiffAction.REMOVE, key=key, old_value=item)

            for item in new[key]:
                if item not in original[key]:
                    yield DictDiff(action=DiffAction.ADD, key=key, new_value=item)

