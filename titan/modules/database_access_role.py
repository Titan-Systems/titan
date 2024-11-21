from .base import ResourceModule

from titan.resources import Grant, Role


class DatabaseAccessRole(ResourceModule):
    inputs = {
        "database": str,
        "schema": str,
        "name": str,
        "read_suffix": str,  # "read"
        "write_suffix": str,  # "write"
    }

    outputs = {
        "read_role": str,
        "write_role": str,
    }

    def __init__(self, inputs: dict, outputs: dict):
        self.resources = []

        database = ResourcePoin
        read_role = Role(name="db_{}_read")
        write_role = Role(name="db_{}_write")

        # USAGE grants
        self.resources.append(Grant(priv="USAGE", on_))

        # self.resources.append()
