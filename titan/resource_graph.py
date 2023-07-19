from contextlib import contextmanager
from queue import Queue
from typing import List, Type, Optional, Set

from .resource import Resource


class ResourceGraph:
    """
    The ResourceGraph is a DAG that manages dependent relationships between Titan resources.

    For example: a table, like most resources, must have a schema and database. The resource graph
    represents this as
    [table] -needs-> [schema] -needs-> [database]

    ```
    @app.table()
    def one_off():
        return "create table STAGING.ONE_OFF (...)"
    ````

    ```
    @app.table(schema="STAGING")
    def one_off():
        return "create table ONE_OFF (...)"
    ````

    ```
    with app.schema("STAGING") as schema:
        titan.table("create table ONE_OFF (...)")
    ```

    """

    def __init__(self):
        self._members: Set[Resource] = set()

        self._ref_listener = None

    def __len__(self):
        return len(self._members)

    @property
    def all(self) -> List[Resource]:
        return list(self._members)

        # While func is executed, the __format__ function for one or more resources may be called
        # we need to find a way to bubble that up so that this View resource is dependent on those
        # other resources

    def add(self, *resources: Resource):
        """

        There are many ways that resource relationships can be captured, both explicitly and implicitly.


        # Direct approach
        app = titan.App(database="RAW")
        t = titan.Table(name="foo", columns=[...])
        app.add(t)

        # Function Decorator
        app = titan.App(database="RAW")
        @app.table()
        def t():
            return "CREATE TABLE foo (...)"

        # Ridealong
        d = titan.Database(name="RAW")
        t = titan.Table(name="foo", columns=[...])
        d.add(t)
        app.add(d)

        # Interpolated
        @app.view()
        def v(t):
            return "CREATE VIEW bar AS SELECT * FROM {t}"


        # Session context
        app.from_sql(`
            CREATE DATABASE RAW;
            CREATE TABLE foo (...);
        `)

        """

        for resource in resources:
            if resource is None:
                raise TypeError

            if resource in self._members:
                return

            if resource.stub:
                return

            if resource.graph:
                raise Exception(f"Resource {repr(resource)} has existing graph pointer")

            self._members.add(resource)
            resource.graph = self

            # if self._ref_listener:
            #     for ref in self._ref_listener:
            #         self.add_dependency(resource, ref)

            # Some resources in Snowflake come with extra resources attached. For example, a Table comes
            # with a special Stage (called a Table Stage) that is created and destroyed atomically with
            # the Table.
            for dep in resource.connections:
                self.add(dep)

    def notify(self, resource):
        """
        An resource has just notified us that it is being interpolated. Add it to a list of items to
        tack on as dependencies
        """
        # self.pending_refs.append(resource)
        pass

    def sorted(self):
        # Kahn's algorithm

        # Compute in-degree (# of inbound edges) for each node
        in_degrees = dict([(node, len(node.required_by)) for node in self._members])
        neighbors = dict([(node, node.connections.copy()) for node in self._members])
        # Put all nodes with 0 in-degree in a queue
        queue: Queue = Queue()
        for node, in_degree in in_degrees.items():
            if in_degree == 0:
                queue.put(node)

        # Create an empty node list
        nodes = []

        while not queue.empty():
            node = queue.get()
            nodes.append(node)

            # For each of node's outgoing edges
            empty_neighbors = set()
            for neighbor in neighbors[node]:
                if neighbor.stub:
                    nodes.append(neighbor)
                    continue
                in_degrees[neighbor] -= 1
                if in_degrees[neighbor] == 0:
                    queue.put(neighbor)
                    empty_neighbors.add(neighbor)

            # Remove edges to empty neighbors
            neighbors[node].difference_update(empty_neighbors)
        nodes.reverse()
        return nodes

    class ReferenceListener:
        # This is a dumb name
        pass
        # def __init__(self):
        #     self.

    @contextmanager
    def capture_refs(self):
        # This is a context manager that yields a special context that will auto link any references

        if self._ref_listener:
            raise Exception("Only one reference listener can be active at a time")

        self._ref_listener = set()
        yield self
        self._ref_listener = None

    def resource_referenced(self, resource):
        if self._ref_listener is not None:
            self._ref_listener.add(resource)
