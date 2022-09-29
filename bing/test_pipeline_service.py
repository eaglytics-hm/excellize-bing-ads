import logging

logging.basicConfig(level=logging.INFO)
logging.getLogger("suds.client").setLevel(logging.DEBUG)
logging.getLogger("suds.client").addHandler(logging.StreamHandler())


import pytest

from bing import pipeline, pipeline_service

TIMEFRAME = [
    ("auto", (None, None)),
    ("manual", ("2022-01-01", "2022-09-29")),
]


@pytest.mark.parametrize(
    "_pipeline",
    pipeline.pipelines.values(),
    ids=pipeline.pipelines.keys(),
)
@pytest.mark.parametrize(
    "account_id",
    ["141581395"],
)
@pytest.mark.parametrize(
    "timeframe",
    [i[1] for i in TIMEFRAME],
    ids=[i[0] for i in TIMEFRAME],
)
def test_pipeline_service(_pipeline, account_id, timeframe):
    res = pipeline_service.pipeline_service(_pipeline, account_id, *timeframe)
    assert res
