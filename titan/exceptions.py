class MissingVarException(Exception):
    pass


class DuplicateResourceException(Exception):
    pass


class MissingResourceException(Exception):
    pass


class MissingPrivilegeException(Exception):
    pass


class MarkedForReplacementException(Exception):
    pass


class NonConformingPlanException(Exception):
    pass


class ResourceInsertionException(Exception):
    pass


class OrphanResourceException(Exception):
    pass


class InvalidOwnerException(Exception):
    pass


class InvalidResourceException(Exception):
    pass
