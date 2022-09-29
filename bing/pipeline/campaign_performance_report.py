from datetime import date

from bingads.service_client import ServiceClient

from bing.pipeline.interface import Pipeline


def build_report(reporting_service: ServiceClient):
    def _build(account_id: str, timeframe: tuple[date, date]):
        def _build_time():
            def _build_custom_date_range(value: date):
                dt = reporting_service.factory.create("Date")
                dt.Day = value.day
                dt.Month = value.month
                dt.Year = value.year
                return dt

            start, end = timeframe

            time = reporting_service.factory.create("ReportTime")
            time.CustomDateRangeStart = _build_custom_date_range(start)
            time.CustomDateRangeEnd = _build_custom_date_range(end)
            time.ReportTimeZone = "PacificTimeUSCanadaTijuana"

            return time

        def _build_scope():
            scope = reporting_service.factory.create(
                "AccountThroughCampaignReportScope"
            )
            scope.AccountIds = {"long": [account_id]}
            scope.Campaigns = None
            return scope

        def _build_columns():
            report_columns = reporting_service.factory.create(
                "ArrayOfCampaignPerformanceReportColumn"
            )
            report_columns.CampaignPerformanceReportColumn.append(
                [
                    "AccountName",
                    "AccountId",
                    "TimePeriod",
                    "CampaignId",
                    "CampaignName",
                    "Impressions",
                    "Clicks",
                    "Conversions",
                    "Spend",
                ]
            )
            return report_columns

        report_request = reporting_service.factory.create(
            "CampaignPerformanceReportRequest"
        )
        report_request.Aggregation = "Daily"
        report_request.ExcludeColumnHeaders = False
        report_request.ExcludeReportFooter = False
        report_request.ExcludeReportHeader = False
        report_request.Format = "Csv"
        report_request.ReturnOnlyCompleteData = False
        report_request.ReportName = "CampaignPerformanceReport"
        report_request.Time = _build_time()
        report_request.Scope = _build_scope()
        report_request.Columns = _build_columns()

        return report_request

    return _build


pipeline = Pipeline(
    "CampaignPerformanceReport",
    build_report,
    lambda rows: [
        {
            "AccountName": row.value("AccountName"),
            "AccountId": row.int_value("AccountId"),
            "TimePeriod": row.value("TimePeriod"),
            "CampaignId": row.int_value("CampaignId"),
            "CampaignName": row.value("CampaignName"),
            "Impressions": row.int_value("Impressions"),
            "Clicks": row.int_value("Clicks"),
            "Conversions": row.int_value("Conversions"),
            "Spend": row.float_value("Spend"),
        }
        for row in rows
    ],
    [
        {"name": "AccountName", "type": "STRING"},
        {"name": "AccountId", "type": "INTEGER"},
        {"name": "TimePeriod", "type": "STRING"},
        {"name": "CampaignId", "type": "INTEGER"},
        {"name": "CampaignName", "type": "STRING"},
        {"name": "Impressions", "type": "INTEGER"},
        {"name": "Clicks", "type": "INTEGER"},
        {"name": "Conversions", "type": "INTEGER"},
        {"name": "Spend", "type": "FLOAT"},
    ],
)
