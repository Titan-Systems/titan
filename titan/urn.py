"""
    Titan Universal Resource Naming (URN)

    Org »

    Acct » Database » Schema » Table

    https://uj63311.us-central1.gcp.snowflakecomputing.com

    ```
    urn header
    |
    |  namespace
    |   |
    |   |   service
    _|_ _|_ _|_________
    urn:aws:mediatailor:${Region}:${Account}:${ResourceType}/${ResourceName}
    ```

    [account]
    ???
    urn:sf:us-central1.gcp::account/UJ63311

    [table]
    urn:sf:us-central1.gcp:UJ63311:table/PROD.AGGS.MYTABLE

    [user]
    urn:sf:us-central1.gcp:UJ63311:user/teej

    [db]
    urn:sf:::database/PROD
"""

from pydantic import BaseModel

header = "urn"
namespace = "sf"


# def URN(region=None, account=None, resource_type=None, resource_name=None) -> str:
# return "/".join(
#     [
#         ":".join(
#             [
#                 header,
#                 namespace,
#                 region or "",
#                 account or "",
#                 resource_type or "",
#             ]
#         ),
#         resource_name or "",
#     ]
# )


class URN(BaseModel):
    region: str = None
    account: str = None
    resource_type: str = None
    name: str = None

    def __str__(self) -> str:
        return "/".join(
            [
                ":".join(
                    [
                        header,
                        namespace,
                        self.region or "",
                        self.account or "",
                        self.resource_type or "",
                    ]
                ),
                self.name or "",
            ]
        )

    def __hash__(self) -> int:
        return hash(str(self))

    def __repr__(self) -> str:
        return str(self)


def parse_urn(urn_string):
    remainder, name = urn_string.split("/")
    _, _, region, account, resource_type = remainder.split(":")
    return URN(
        region=region,
        account=account,
        resource_type=resource_type,
        name=name,
    )
