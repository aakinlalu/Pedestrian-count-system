import asyncio
import tempfile

import pytest

from pedestrian_count_system import __version__
from pedestrian_count_system.api_call import APIUrl

dir_name = tempfile.mkdtemp()


@pytest.fixture
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    # loop.close()


apiCall = APIUrl(
    "https://data.melbourne.vic.gov.au/resource/b2ak-trbp.json",
    "ml-artifact-store",
    dir_name,
)


def test_fetch_url_payload(event_loop):
    data = event_loop.run_until_complete(apiCall.fetch_url_payload(2021, "May"))
    assert type(data) == list


def test_write_json_to_local(event_loop):
    assert (
        event_loop.run_until_complete(apiCall.write_json_to_local(2021, "May")) is None
    )


def test_version():
    assert __version__ == "0.1.0"
