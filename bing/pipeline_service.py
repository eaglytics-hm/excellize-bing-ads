from typing import Optional
from datetime import datetime, date, timedelta

from compose import compose

from db import bigquery
from bing.pipeline import interface
from bing import repo

DATE_FORMAT = "%Y-%m-%d"


def pipeline_service(
    pipeline: interface.Pipeline,
    account_id: str,
    start: Optional[str],
    end: Optional[str],
):
    auth_data = repo.get_auth_data()

    _end = datetime.strptime(end, DATE_FORMAT).date() if end else date.today()
    _start = (
        datetime.strptime(start, DATE_FORMAT).date()
        if start
        else date.today() - timedelta(days=30)
    )

    reporting_service = repo.get_reporting_service(auth_data)

    return compose(
        lambda x: {"output_rows": x},
        bigquery.load(f"p_{pipeline.name}__{account_id}", pipeline.schema),
        pipeline.transform_fn,
        lambda report: [i for i in report.report_records],
        repo.get_report(auth_data),
        pipeline.build_fn(reporting_service),
    )(account_id, (_start, _end))
