from titan.parse import parse_region


def test_parse_region():
    assert parse_region("AWS_US_WEST_2") == {"cloud": "AWS", "cloud_region": "US_WEST_2"}
    assert parse_region("PUBLIC.AWS_US_WEST_2") == {
        "region_group": "PUBLIC",
        "cloud": "AWS",
        "cloud_region": "US_WEST_2",
    }
    assert parse_region("AZURE_WESTUS2") == {"cloud": "AZURE", "cloud_region": "WESTUS2"}
    assert parse_region("GCP_EUROPE_WEST4") == {"cloud": "GCP", "cloud_region": "EUROPE_WEST4"}

    assert parse_region("AWS_US_GOV_WEST_1_FHPLUS") == {"cloud": "AWS", "cloud_region": "US_GOV_WEST_1_FHPLUS"}
    assert parse_region("AWS_US_GOV_WEST_1_DOD") == {"cloud": "AWS", "cloud_region": "US_GOV_WEST_1_DOD"}
    assert parse_region("AWS_AP_SOUTHEAST_1") == {"cloud": "AWS", "cloud_region": "AP_SOUTHEAST_1"}
    assert parse_region("AWS_EU_CENTRAL_1") == {"cloud": "AWS", "cloud_region": "EU_CENTRAL_1"}

    assert parse_region("AZURE_CANADACENTRAL") == {"cloud": "AZURE", "cloud_region": "CANADACENTRAL"}
    assert parse_region("AZURE_NORTHEUROPE") == {"cloud": "AZURE", "cloud_region": "NORTHEUROPE"}
    assert parse_region("AZURE_SWITZERLANDNORTH") == {"cloud": "AZURE", "cloud_region": "SWITZERLANDNORTH"}
    assert parse_region("AZURE_USGOVVIRGINIA") == {"cloud": "AZURE", "cloud_region": "USGOVVIRGINIA"}

    assert parse_region("GCP_US_CENTRAL1") == {"cloud": "GCP", "cloud_region": "US_CENTRAL1"}
    assert parse_region("GCP_EUROPE_WEST2") == {"cloud": "GCP", "cloud_region": "EUROPE_WEST2"}
    assert parse_region("GCP_EUROPE_WEST3") == {"cloud": "GCP", "cloud_region": "EUROPE_WEST3"}

    assert parse_region("PUBLIC.AWS_EU_CENTRAL_1") == {
        "region_group": "PUBLIC",
        "cloud": "AWS",
        "cloud_region": "EU_CENTRAL_1",
    }
    assert parse_region("PUBLIC.AZURE_WESTEUROPE") == {
        "region_group": "PUBLIC",
        "cloud": "AZURE",
        "cloud_region": "WESTEUROPE",
    }
    assert parse_region("PUBLIC.GCP_US_CENTRAL1") == {
        "region_group": "PUBLIC",
        "cloud": "GCP",
        "cloud_region": "US_CENTRAL1",
    }

    assert parse_region("SOME_VALUE.GCP_US_CENTRAL1") == {
        "region_group": "SOME_VALUE",
        "cloud": "GCP",
        "cloud_region": "US_CENTRAL1",
    }
