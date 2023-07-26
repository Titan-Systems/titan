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


class URN(BaseModel):
    region: str = None
    account: str = None
    resource_key: str = None
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
                        self.resource_key or "",
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
    _, _, region, account, resource_key = remainder.split(":")
    return URN(
        region=region,
        account=account,
        resource_key=resource_key,
        name=name,
    )
