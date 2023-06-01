from titan.share import Share


# NOTE: Catalog should probably crawl a share and add all its tables and views into the catalog
class Catalog:
    def __init__(self, session):
        self._catalog = {}
        self._session = session

    def __contains__(self, resource):
        resource_cls = type(resource)
        if resource_cls not in self._catalog:
            self._catalog[resource_cls] = resource_cls.show(self._session)
        resource_identifier = resource.name
        if resource_cls is Share:
            resource_identifier = resource.listing
        return resource_identifier in self._catalog[resource_cls]
