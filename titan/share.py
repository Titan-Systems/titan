from .resource import Resource, ResourceDB, Namespace
from .props import Props, StringProp
from .schema import Schema


class Share(Resource):
    """
    CREATE DATABASE
        IDENTIFIER('SNOWPARK_FOR_PYTHON__HANDSONLAB__WEATHER_DATA')
    FROM SHARE
        IDENTIFIER('WEATHERSOURCE.SNOWFLAKE_MANAGED$PUBLIC_GCP_US_CENTRAL1."WEATHERSOURCE_SNOWFLAKE_SNOWPARK_TILE_SNOWFLAKE_SECURE_SHARE_1651768630709"');
    """

    # resource_type = "DATABASE"
    namespace = Namespace.ACCOUNT
    props = Props(
        from_share=StringProp("from share"),
    )

    name: str
    from_share: str

    _schemas: ResourceDB

    def model_post_init(self, ctx):
        super().model_post_init(ctx)
        self._schemas = ResourceDB(Schema)

    @property
    def schemas(self):
        return self._schemas

    # self.listing = listing
    # self.accept_terms = accept_terms
    # self.database_share: str = 'WEATHERSOURCE.SNOWFLAKE_MANAGED$PUBLIC_GCP_US_CENTRAL1."WEATHERSOURCE_SNOWFLAKE_SNOWPARK_TILE_SNOWFLAKE_SECURE_SHARE_1651768630709"'
    # This is a bug, Shares need to have a model of all the entities that will be created
    # self.implicit_schema: Schema = Schema(name="ONPOINT_ID", database=self, implicit=True)

    # SHOW OBJECTS IN DATABASE WEATHER_NYC

    # def create(self, session):
    #     # Punting for now. Not sure if this is better represented as a dependency in the resource graph
    #     if self.accept_terms:
    #         session.sql(f"CALL SYSTEM$ACCEPT_LEGAL_TERMS('DATA_EXCHANGE_LISTING', '{self.listing}');").collect()
    #     session.sql(
    #         f"""
    #         CREATE DATABASE {self.name}
    #         FROM SHARE {self.database_share}
    #         """
    #     ).collect()

    # def table(self, tablename):
    #     table = Table(name=tablename, database=self, schema=self.implicit_schema, implicit=True)

    #     # TODO: there needs to be a way for share to bring its ridealongs
    #     if self.graph:
    #         self.graph.add(table)

    #     return table

    # @classmethod
    # def show(cls, session):
    #     return [row.listing_global_name for row in session.sql("SHOW SHARES").collect()]
