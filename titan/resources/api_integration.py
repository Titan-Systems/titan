from ..resource import Resource, AccountScoped
from ..parseable_enum import ParseableEnum
from ..props import Props, EnumProp, StringProp, BoolProp


class ApiProvider(ParseableEnum):
    AWS_API_GATEWAY = "AWS_API_GATEWAY"
    AWS_PRIVATE_API_GATEWAY = "AWS_PRIVATE_API_GATEWAY"
    AWS_GOV_API_GATEWAY = "AWS_GOV_API_GATEWAY"
    AWS_GOV_PRIVATE_API_GATEWAY = "AWS_GOV_PRIVATE_API_GATEWAY"


class APIIntegration(Resource, AccountScoped):
    """
    CREATE [ OR REPLACE ] API INTEGRATION [ IF NOT EXISTS ] <integration_name>
        API_PROVIDER = { aws_api_gateway | aws_private_api_gateway | aws_gov_api_gateway | aws_gov_private_api_gateway }
        API_AWS_ROLE_ARN = '<iam_role>'
        [ API_KEY = '<api_key>' ]
        API_ALLOWED_PREFIXES = ('<...>')
        [ API_BLOCKED_PREFIXES = ('<...>') ]
        ENABLED = { TRUE | FALSE }
        [ COMMENT = '<string_literal>' ]
        ;
    """

    resource_type = "API INTEGRATION"
    props = Props(
        api_provider=EnumProp("api_provider", ApiProvider),
        api_aws_role_arn=StringProp("api_aws_role_arn"),
        api_key=StringProp("api_key"),
        api_allowed_prefixes=StringProp("api_allowed_prefixes"),
        api_blocked_prefixes=StringProp("api_blocked_prefixes"),
        enabled=BoolProp("enabled"),
        comment=StringProp("comment"),
    )

    name: str
    api_provider: str
    api_aws_role_arn: str
    api_key: str = None
    api_allowed_prefixes: str
    api_blocked_prefixes: str = None
    enabled: bool
    comment: str = None
