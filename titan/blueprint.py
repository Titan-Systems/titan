class Blueprint:
    def __init__(self, name=None) -> None:
        self.name = name

    def plan(self, session):
        pass

    def apply(self, session):
        pass


"""

@titan.blueprint
def prod():
    admin_db = titan.Database("ADMIN")
    shared_db = titan.Database("SHARED")
    # setup_base(base_db)
    provisioner = titan.Role("provisioner")
    provisioner.grant(client_database.required_permissions)
    return {}

@titan.blueprint(policies=[titan.Policy(...), titan_standard_policy])
def prod():
    provisioner = titan.Role("provisioner")
    provisioner.grant(client_database.required_permissions)
    return (
        titan.Database("ADMIN"),
        titan.Database("SHARED"),
        provisioner,
    )

Ideas
 - blueprints have parameters to let them be stamped out
 - blueprints have exports (why?)

Stuck
 - How can we write a blueprint that doesnt result in a bunch of hanging python objects?
   Explicit is probably better than implicit



"""
