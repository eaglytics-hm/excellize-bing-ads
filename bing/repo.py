from typing import Any
import os
import logging

logging.basicConfig(level=logging.INFO)
logging.getLogger("suds.client").setLevel(logging.DEBUG)
logging.getLogger("suds.transport.http").setLevel(logging.DEBUG)
logging.getLogger("suds.client").addHandler(logging.StreamHandler())


from bingads.authorization import (
    AuthorizationData,
    OAuthDesktopMobileAuthCodeGrant,
)
from bingads.service_client import ServiceClient
from bingads.v13.reporting.reporting_download_parameters import (
    ReportingDownloadParameters,
)
from bingads.v13.reporting.reporting_service_manager import ReportingServiceManager

ENVIRONMENT = "production"


def get_auth_data() -> AuthorizationData:
    auth_data = AuthorizationData(developer_token=os.getenv("BING_DEVELOPER_TOKEN"))
    auth = OAuthDesktopMobileAuthCodeGrant(
        client_id=os.getenv("BING_CLIENT_ID"),
        env=ENVIRONMENT,
    )

    auth.state = "bld@bingads_amp"
    auth.client_secret = os.getenv("BING_CLIENT_SECRET")

    auth_data.authentication = auth

    auth_data.authentication.request_oauth_tokens_by_refresh_token(
        os.getenv("BING_REFRESH_TOKEN")
    )
    return auth_data


def get_reporting_service(auth_data: AuthorizationData) -> ServiceClient:
    return ServiceClient(
        service="ReportingService",
        version=13,
        authorization_data=auth_data,
        environment=ENVIRONMENT,
    )


def get_report(auth_data: AuthorizationData):
    def _get(report_request: Any):
        reporting_download_parameters = ReportingDownloadParameters(
            report_request=report_request,
            timeout_in_milliseconds=9 * 60 * 1000,
        )
        reporting_service_manager = ReportingServiceManager(
            authorization_data=auth_data,
            poll_interval_in_milliseconds=5 * 1000,
            environment=ENVIRONMENT,
        )
        report_container = reporting_service_manager.download_report(
            reporting_download_parameters
        )
        return report_container

    return _get
