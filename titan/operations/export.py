from collections import OrderedDict
from inflection import pluralize

from titan.data_provider import list_resource, fetch_resource, fetch_account_locator
from titan.enums import ResourceType
from titan.identifiers import FQN, URN, resource_label_for_type
from titan.operations.connector import connect


def export_resources(include: list[ResourceType] = None, exclude: list[ResourceType] = None) -> dict[str, list]:
    session = connect()
    resource_types = [resource_type for resource_type in ResourceType if resource_type in include]
    return {resource_type: export_resource(session, resource_type) for resource_type in resource_types}


def export_all_resources() -> dict[str, list]:
    session = connect()
    config = {}
    for resource_type in ResourceType:
        try:
            config.update(export_resource(session, resource_type))
        except AttributeError:
            continue
    return config


def export_resource(session, resource_type: ResourceType) -> dict[str, list]:
    resource_label = resource_label_for_type(resource_type)
    resource_names = list_resource(session, resource_label)
    if len(resource_names) == 0:
        return {}
    resources = []
    for fqn in resource_names:
        resource = fetch_resource(session, URN(resource_type, fqn, account_locator=""))
        # Sort dict based on key name
        resource = {k: resource[k] for k in sorted(resource)}
        # Put name field at the top of the dict
        name = {}
        if "name" in resource:
            name = {"name": resource.pop("name")}
        if "_privs" in resource:
            resource.pop("_privs")
        resources.append({**name, **resource})
    return {pluralize(resource_label): resources}
