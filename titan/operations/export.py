from inflection import pluralize

from titan.data_provider import list_resource, fetch_resource, fetch_account_locator
from titan.enums import ResourceType
from titan.identifiers import FQN, URN, resource_label_for_type
from titan.operations.connector import connect


def export_resources(resource_type: ResourceType) -> dict[str, list]:
    session = connect()
    resource_label = resource_label_for_type(resource_type)
    resource_names = list_resource(session, resource_label)
    account_locator = fetch_account_locator(session)
    resource_urns = [URN(resource_type, FQN(name), account_locator) for name in resource_names]
    resources = [fetch_resource(session, urn) for urn in resource_urns]
    return {pluralize(resource_label): resources}
