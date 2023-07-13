# _ = titan.Policy(
#     acls=[
#         titan.ACL(
#             privs=["READ"],
#             roles=[f"role:ANALYTICS", titan.Role.all["DATAENG"]],
#             resources=[f"tag:group=analytics"],
#         )
#     ],
#     roles={
#         f"role:CLIENT_{client_id}": [client_admin.username],
#     },
# )


# What are all the objects that user X can access?

from .grant import Grant


class ACL:
    superprivs = {"READ": {}, "WRITE": {}}

    def __init__(self, privs, roles, resources):
        self.privs = privs
        self.roles = roles
        self.resources = resources

    def grants(self):
        """Return a list of grants that this ACL represents."""
        return [
            Grant(priv=priv, role=role, resource=resource)
            for priv in self.privs
            for role in self.roles
            for resource in self.resources
        ]
