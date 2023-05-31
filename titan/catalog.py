from titan.share import Share


# NOTE: Catalog should probably crawl a share and add all its tables and views into the catalog
class Catalog:
    def __init__(self, session):
        self._catalog = {}
        self._session = session

    def __contains__(self, entity):
        entity_cls = type(entity)
        if entity_cls not in self._catalog:
            self._catalog[entity_cls] = entity_cls.show(self._session)
        entity_identifier = entity.name
        if entity_cls is Share:
            entity_identifier = entity.listing
        return entity_identifier in self._catalog[entity_cls]
