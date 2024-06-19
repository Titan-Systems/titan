import logging
import pytest

from titan import resources, Resource
from titan.enums import ResourceType
from tests.helpers import get_json_fixture, camelcase_to_snakecase


logger = logging.getLogger("titan")


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


def enumerate_polymorphic_resources():
    resources = []
    for resource_type, class_list in Resource.__types__.items():
        if len(class_list) > 1:
            for class_ in class_list:
                resources.append((resource_type, class_))
    return resources


POLYMORPHIC_RESOURCES = enumerate_polymorphic_resources()


@pytest.fixture(
    params=POLYMORPHIC_RESOURCES,
    ids=[f"{resource_type}:{class_.__name__}" for resource_type, class_ in POLYMORPHIC_RESOURCES],
    scope="function",
)
def polymorphic_resource(request):
    resource_type, class_list = request.param
    yield resource_type, class_list


def test_polymorphic_resources(polymorphic_resource):
    resource_type, class_ = polymorphic_resource

    resource_name = camelcase_to_snakecase(class_.__name__)
    try:
        data = get_json_fixture(resource_name)
    except FileNotFoundError:
        pytest.fail(f"No JSON fixture for {resource_name}")
    except ValueError:
        pytest.fail(f"Missing or malformed JSON fixture for {resource_name}")
    assert Resource.resolve_resource_cls(resource_type, data) == class_, f"{resource_name} -> {class_.__name__}"
