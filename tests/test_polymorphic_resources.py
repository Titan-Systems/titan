from titan import resources
from titan.enums import ResourceType
from tests.helpers import get_json_fixture


def test_internal_stage():
    data = get_json_fixture("internal_stage")
    data["resource_type"] = ResourceType.STAGE
    res = resources.Resource.from_dict(data)
    assert isinstance(res, resources.InternalStage)


def test_external_stage():
    data = get_json_fixture("external_stage")
    data["resource_type"] = ResourceType.STAGE
    res = resources.Resource.from_dict(data)
    assert isinstance(res, resources.ExternalStage)


def test_table_stream():
    data = get_json_fixture("table_stream")
    data["resource_type"] = ResourceType.STREAM
    res = resources.Resource.from_dict(data)
    assert isinstance(res, resources.TableStream)


# def test_external_table_stream():
#     data = get_json_fixture("external_table_stream")
#     data["resource_type"] = ResourceType.STREAM
#     res = resources.Resource.from_dict(data)
#     assert isinstance(res, resources.ExternalTableStream)


def test_stage_stream():
    data = get_json_fixture("stage_stream")
    data["resource_type"] = ResourceType.STREAM
    res = resources.Resource.from_dict(data)
    assert isinstance(res, resources.StageStream)


def test_view_stream():
    data = get_json_fixture("view_stream")
    data["resource_type"] = ResourceType.STREAM
    res = resources.Resource.from_dict(data)
    assert isinstance(res, resources.ViewStream)
