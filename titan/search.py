from . import data_provider as dp
from .identifiers import ResourceLocator, FQN, URN


def crawl_resources(session, locator: ResourceLocator):
    resources = []

    if locator.star:
        pass
    else:
        urn = URN.from_locator(locator)
        resources.append(dp.fetch_resource(session, urn))

    return resources

    # Start with root resource
    if locator.resource_key == "account":
        # resources.append(dp.fetch_account(session, FQN(database="TITAN")))
        pass
    elif locator.resource_key == "database":
        resources.append(dp.fetch_database(session, FQN(database="TITAN")))

    return [
        {
            "resource_key": "database",
            "comment": None,
            "data_retention_time_in_days": 1,
            "default_ddl_collation": None,
            "max_data_extension_time_in_days": 14,
            "name": "TITAN",
            "owner": "ACCOUNTADMIN",
            "transient": None,
        },
        dp.fetch_schema(session, FQN(database="TITAN", name="SPROCS")) | {"resource_key": "schema"},
    ]
