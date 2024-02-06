from titan.resources.resource import Resource, ResourcePointer


def coerce_from_str(cls: Resource) -> callable:
    def _coerce(name_or_resource):
        if isinstance(name_or_resource, str):
            return ResourcePointer(name=name_or_resource, resource_type=cls.resource_type)
        else:
            return name_or_resource

    return _coerce
