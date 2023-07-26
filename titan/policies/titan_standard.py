from typing import Callable, Union

from titan.policy import Policy, PolicyPack, OwnershipPolicy, EnforcementLevel  # , resource_types

# from titan.user import User
# from titan.role import Role


# @resource_types(User, Role) Union[User, Role]
def users_and_roles_owned_by_useradmin(user_or_role, report_violation: Callable):
    if user_or_role.owner != "USERADMIN":
        report_violation("All users must be owned by USERADMIN role")


titan_standard = PolicyPack(
    name="titan-standard",
    policies=[
        Policy(
            name="useradmin-owns-all-users-and-roles",
            description="All users and roles must be owned by USERADMIN",
            enforcement_level=EnforcementLevel.MANDATORY,
            validate=users_and_roles_owned_by_useradmin,
        )
    ],
)
