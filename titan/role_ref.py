from typing import Union, TYPE_CHECKING

if TYPE_CHECKING:
    from titan.resources.role import Role, DatabaseRole

RoleRef = Union["Role", "DatabaseRole", str]
