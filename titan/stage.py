import re

from sqlglot import exp

from .resource import SchemaLevelResource


class Stage(SchemaLevelResource):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.hooks = {"on_file_added": None}

    @classmethod
    def from_expression(cls, expression: exp.Create):
        """
        (COMMAND
            this: CREATE,
            expression:  STAGE "DATA_APPS_DEMO"."DEMO"."DEMOCITIBIKEDATA"
                         ENCRYPTION=(TYPE='AWS_SSE_S3')
                         URL = 's3://demo-citibike-data/')
        """

        sql = expression.args["expression"]

        identifier = re.compile(r'STAGE\s+(["\.A-Z_]+)', re.IGNORECASE)
        match = re.search(identifier, sql)

        if match is None:
            raise Exception
        name = match.group(1).split(".")[-1]
        # props = cls.parse_props(sql[match.end() :])
        props = {}

        return cls(name=name, **props)

    # @property
    # def on_file_added(self):
    #     return self.hooks["on_file_added"]

    # @on_file_added.setter
    # def on_file_added(self, hook):
    #     # TODO: This needs to be refactored to be wrapped in a Sproc resource and for dependencies to be implicitly tracked
    #     self.hooks["on_file_added"] = on_file_added_factory("ZIPPED_TRIPS", hook)
    #     self.state["on_file_added:last_checked"] = State(key="last_checked", value="'1900-01-01'::DATETIME")
    #     # print("on_file_added")
    #     # print(inspect.getsource(self.hooks["on_file_added"]))

    # def create(self, session):
    #     super().create(session)

    #     for statefunc in self.state.values():
    #         statefunc.create(session)

    #     if self.hooks["on_file_added"]:
    #         session.sql("CREATE STAGE IF NOT EXISTS sprocs").collect()
    #         session.add_packages("snowflake-snowpark-python")
    #         session.sproc.register(
    #             self.hooks["on_file_added"],
    #             name=f"ZIPPED_TRIPS_hook_on_file_added",
    #             replace=True,
    #             is_permanent=True,
    #             stage_location="@sprocs",
    #             execute_as="caller",
    #         )
