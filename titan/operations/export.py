from inflection import pluralize

from titan.data_provider import fetch_resource, list_resource
from titan.enums import ResourceType
from titan.identifiers import URN, resource_label_for_type
from titan.operations.connector import connect
from titan.resources.grant import grant_yaml


def export_resources(include: list[ResourceType] = None, exclude: list[ResourceType] = None) -> dict[str, list]:
    session = connect()
    config = {}
    for resource_type in ResourceType:
        if include and resource_type not in include:
            continue
        if exclude and resource_type in exclude:
            continue
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
        resources.append(_format_resource_config(resource, resource_type))
    return {pluralize(resource_label): resources}


def _format_resource_config(resource: dict, resource_type: ResourceType) -> dict:
    if resource_type == ResourceType.GRANT:
        return grant_yaml(resource)
    # Sort dict based on key name
    resource = {k: resource[k] for k in sorted(resource)}
    # Put name field at the top of the dict
    first_field = {}
    if "name" in resource:
        first_field = {"name": resource.pop("name")}
    return {**first_field, **resource}
