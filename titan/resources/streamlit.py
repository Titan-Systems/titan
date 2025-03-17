from dataclasses import dataclass
from typing import Optional

from ..enums import ResourceType
from ..props import FromProp, IdentifierProp, Props, StringProp
from ..resource_name import ResourceName
from ..role_ref import RoleRef
from ..scope import SchemaScope
from .resource import NamedResource, Resource, ResourceSpec
from .tag import TaggableResource


@dataclass(unsafe_hash=True)
class _Streamlit(ResourceSpec):
    """
    Specification for a Streamlit resource, defining its data structure.
    """

    name: ResourceName
    from_: str
    version: Optional[str] = None
    main_file: Optional[str] = None
    title: Optional[str] = None
    query_warehouse: Optional[str] = None
    comment: Optional[str] = None
    owner: RoleRef = "SYSADMIN"

    def __post_init__(self):
        super().__post_init__()
        if self.from_.startswith("@"):
            if self.version is not None:
                raise ValueError("Version should not be set when the source is a stage")


class Streamlit(NamedResource, TaggableResource, Resource):
    """
    Description:
        Represents a Streamlit app in Snowflake, which is a schema-scoped resource for creating
        interactive applications.

    Snowflake Docs:
        https://docs.snowflake.com/en/sql-reference/sql/create-streamlit

    Fields:
        name (string, required): The name of the Streamlit app. Can be fully qualified (e.g., "db.schema.app").
        from_ (string, required): The source of the Streamlit app, either a stage (e.g., '@mystage') or a
            repository URL (e.g., 'https://github.com/user/repo.git').
        version (string): The version of the repository, applicable only if from_ is a repository URL.
        main_file (string): The main Python file for the Streamlit app (e.g., 'app.py').
        title (string): The display title of the Streamlit app.
        query_warehouse (string): The warehouse used for queries in the app.
        comment (string): A descriptive comment about the Streamlit app.
        owner (string or Role): The role that owns the Streamlit app. Defaults to "SYSADMIN".
        tags (dict): A dictionary of key-value pairs for tagging the Streamlit app.

    Python Example:
        # Creating a Streamlit app from a stage
        streamlit_stage = Streamlit(
            name="my_db.my_schema.my_streamlit",
            from_="@my_stage",
            main_file="app.py",
            title="My Streamlit App",
            query_warehouse="my_warehouse",
            comment="A sample Streamlit app from a stage",
            tags={"project": "demo"}
        )

        # Creating a Streamlit app from a Git repository
        streamlit_repo = Streamlit(
            name="my_streamlit",
            from_="https://github.com/user/repo.git",
            version="main",
            main_file="app.py",
            title="Repo Streamlit App",
            owner="SYSADMIN"
        )
    YAML Example:
        streamlits:
        - name: my_db.my_schema.my_streamlit
            from: "@my_stage"
            main_file: "app.py"
            title: "My Streamlit App"
            query_warehouse: "my_warehouse"
            comment: "A sample Streamlit app from a stage"
            owner: SYSADMIN
            tags:
            project: demo
        - name: my_streamlit
            from: "https://github.com/user/repo.git"
            version: "main"
            main_file: "app.py"
            title: "Repo Streamlit App"
    """

    resource_type = ResourceType.STREAMLIT
    props = Props(
        from_=FromProp(),
        version=StringProp("VERSION"),
        main_file=StringProp("MAIN_FILE"),
        title=StringProp("TITLE"),
        query_warehouse=IdentifierProp("QUERY_WAREHOUSE"),
        comment=StringProp("COMMENT"),
    )
    scope = SchemaScope()
    spec = _Streamlit

    def init(
        self,
        name: str,
        from_: str,
        version: Optional[str] = None,
        main_file: Optional[str] = None,
        title: Optional[str] = None,
        query_warehouse: Optional[str] = None,
        comment: Optional[str] = None,
        owner: str = "SYSADMIN",
        tags: dict[str, str] = None,
        **kwargs,
    ):
        super().__init__(name, **kwargs)
        self._data: _Streamlit = _Streamlit(
            name=self.name,
            from_=from_,
            version=version,
            main_file=main_file,
            title=title,
            query_warehouse=query_warehouse,
            comment=comment,
            owner=owner,
        )
        self.set_tags(tags)
