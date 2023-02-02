"""Stream type classes for tap-indeedsponsoredjobs."""

import csv
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Union

import pendulum
import requests
from memoization import cached
from requests import PreparedRequest
from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.authenticators import OAuthAuthenticator
from singer_sdk.pagination import BaseAPIPaginator, SimpleHeaderPaginator

from tap_indeedsponsoredjobs.auth import IndeedSponsoredJobsAuthenticator
from tap_indeedsponsoredjobs.client import IndeedSponsoredJobsStream

SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")


class Employers(IndeedSponsoredJobsStream):
    """List of all employers we have access to"""

    name = "employers"
    path = "/appinfo"
    primary_keys = ["id"]
    replication_key = None
    records_jsonpath = "$.['employers'][*]"  # Or override `parse_response`.
    url_base = "https://secure.indeed.com/v2/api"

    @cached
    def authenticator(self) -> IndeedSponsoredJobsAuthenticator:
        """Return a new authenticator object."""
        return IndeedSponsoredJobsAuthenticator.create_multiemployerauth_for_stream(
            self
        )

    # Optionally, you may also use `schema_filepath` in place of `schema`:
    # schema_filepath = SCHEMAS_DIR / "users.json"
    schema = th.PropertiesList(
        th.Property("name", th.StringType),
        th.Property("id", th.StringType),
        th.Property(
            "name",
            th.StringType,
        ),
    ).to_dict()

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "_sdc_employer_id": record["id"],
        }


class EmployerStatsReport(IndeedSponsoredJobsStream):
    """Employer Stats Report.

    These reports are CSVs generated in daily increments.
    The benefit over this stream vs the Campaign Performance Stats stream is that this stream includes extra information in the report such as Job information.

    Line Number is the line number in the CSV returned line 1 = headers
    """

    name = "employer_stats_report"
    path = "/v1/stats"
    primary_keys = ["_sdc_employer_id", "_sdc_start_date", "_sdc_line_number"]
    replication_key = "_sdc_start_date"
    is_sorted = True
    parent_stream_type = Employers
    records_jsonpath = "$.[*]"
    # We assumed version 6
    schema = th.PropertiesList(
        th.Property("Campaign ID", th.StringType),
        th.Property("Advertisement", th.StringType),
        th.Property("Job", th.StringType),
        th.Property("Job Reference Number", th.StringType),
        th.Property("Platform", th.StringType),
        th.Property("Clicks", th.StringType),
        th.Property("Impressions", th.StringType),
        th.Property("Conversions", th.StringType),
        th.Property("Indeed Apply Conversions", th.StringType),
        th.Property("Spend (currency)", th.StringType),
        th.Property("Organic Clicks", th.StringType),
        th.Property("Organic Impressions", th.StringType),
        th.Property("Apply Starts", th.StringType),
        th.Property("Organic Apply Starts", th.StringType),
        th.Property("_sdc_employer_id", th.StringType),
        th.Property("_sdc_start_date", th.DateType),
        th.Property("_sdc_end_date", th.DateType),
        th.Property("_sdc_employer_id", th.StringType),
        th.Property("_sdc_campaign_id", th.StringType),
    ).to_dict()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        start_date = pendulum.parse(self.get_starting_replication_key_value(context))
        end_date = start_date.add(days=1)
        params: dict = {}
        params["startDate"] = start_date.format("YYYY-MM-DD")
        params["endDate"] = end_date.format("YYYY-MM-DD")
        params["v"] = "6"  # Report version 6

        return params

    def get_records(self, context):
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        # Have Docusign generate a report
        report_pointer_record = None
        for record in self.request_records(context):
            if report_pointer_record is None:
                report_pointer_record = record
            else:
                raise Exception(
                    "More than one record for a report is returned. This should not happen."
                )

        # Get data from th genearted report
        # Update URL
        self.path = report_pointer_record["data"]["location"]

        for record in self.request_records(context):
            record["_sdc_line_number"] = record.line_num
            record["_sdc_employer_id"] = context["_sdc_employer_id"]
            record["_sdc_start_date"] = self.get_url_params(context)["startDate"]
            record["_sdc_end_date"] = self.get_url_params(context)["endDate"]
            yield record

        # Set path back for next request
        self.path = "/v1/stats"

    def get_new_paginator(self) -> BaseAPIPaginator:
        """No Paginator needed

        Returns:
            A paginator instance.
        """
        return SimpleHeaderPaginator("NOOP_Paginator")

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: A raw `requests.Response`_ object.

        Yields:
            One item for every item found in the response.

        .. _requests.Response:
            https://requests.readthedocs.io/en/latest/api/#requests.Response
        """

        if self.path == "/v1/stats":
            yield response.json()
        else:
            breakpoint()
            csv_data = csv.DictReader(response.text.splitlines())
            for record in csv_data:
                yield record


class Campaigns(IndeedSponsoredJobsStream):
    """Campaigns per Employer"""

    name = "campaigns"
    path = "/v1/campaigns"
    primary_keys = ["Id"]
    records_jsonpath = "$.['data']['Campaigns'][*]"
    replication_key = None
    parent_stream_type = Employers
    schema = th.PropertiesList(
        th.Property("Name", th.StringType),
        th.Property("Id", th.StringType),
        th.Property("Status", th.StringType),
        th.Property("_sdc_employer_id", th.StringType),
    ).to_dict()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        # if next_page_token:
        #    params["page"] = next_page_token
        # if self.replication_key:
        #    params["sort"] = "asc"
        #    params["order_by"] = self.replication_key
        params["perPage"] = 1000000000
        params["status"] = "Active"
        return params

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""
        return {
            "_sdc_employer_id": context["_sdc_employer_id"],
            "_sdc_campaign_id": record["Id"],
        }


class CampaignPerformanceStats(IndeedSponsoredJobsStream):
    """Campaign Performance per Campaign"""

    name = "campaign_performance_stats"
    path = "/v1/campaigns/{_sdc_campaign_id}/stats"
    primary_keys = ["Id", "Date"]
    records_jsonpath = "$.['data']['entries'][*]"
    replication_key = "Date"
    is_sorted = False
    parent_stream_type = Campaigns
    schema = th.PropertiesList(
        th.Property("Name", th.StringType),
        th.Property("Id", th.StringType),
        th.Property("Date", th.DateType),
        th.Property("Clicks", th.IntegerType),
        th.Property("Impressions", th.IntegerType),
        th.Property("Conversions", th.IntegerType),
        th.Property("CurrencyCode", th.StringType),
        th.Property("Cost", th.NumberType),
        th.Property("OrganicClicks", th.IntegerType),
        th.Property("OrganicImpressions", th.IntegerType),
        th.Property("Applystarts", th.IntegerType),
        th.Property("OrganicApplystarts", th.IntegerType),
        th.Property("_sdc_employer_id", th.StringType),
        th.Property("_sdc_campaign_id", th.StringType),
    ).to_dict()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        # TODO call super
        # if next_page_token:
        #    params["page"] = next_page_token
        # if self.replication_key:
        #    params["sort"] = "asc"
        #    params["order_by"] = self.replication_key
        params["perPage"] = 1000000000
        # params["startDate"]=self.config["start_date"]
        params["startDate"] = self.get_starting_replication_key_value(context)

        return params


class CampaignBudget(IndeedSponsoredJobsStream):
    """Campaign Budget per Campaign"""

    name = "campaign_budget"
    path = "/v1/campaigns/{_sdc_campaign_id}/budget"
    primary_keys = ["_sdc_campaign_id"]
    records_jsonpath = "$.['data']"
    replication_key = None
    parent_stream_type = Campaigns
    schema = th.PropertiesList(
        th.Property("budgetMonthlyLimit", th.NumberType),
        th.Property("_sdc_employer_id", th.StringType),
        th.Property("_sdc_campaign_id", th.StringType),
    ).to_dict()


class CampaignInfo(IndeedSponsoredJobsStream):
    """Campaign Info per Campaign"""

    name = "campaign_info"
    path = "/v1/campaigns/{_sdc_campaign_id}"
    primary_keys = ["Id"]
    records_jsonpath = "$.['data']"
    replication_key = None
    parent_stream_type = Campaigns
    schema = th.PropertiesList(
        th.Property("Name", th.StringType),
        th.Property("Id", th.StringType),
        th.Property("Type", th.StringType),
        th.Property("Status", th.StringType),
        th.Property("CurrencyCode", th.StringType),
        th.Property("TrackingToken", th.StringType),
        th.Property(
            "Objective",
            th.ObjectType(
                th.Property("description", th.StringType),
                th.Property("objectiveType", th.StringType),
            ),
        ),
        th.Property(
            "NonSpendingReasons",
            th.ArrayType(
                th.ObjectType(
                    th.Property("type", th.StringType),
                    th.Property("description", th.StringType),
                )
            ),
        ),
        th.Property("SpendingChannels", th.ArrayType(th.StringType)),
        th.Property("_sdc_employer_id", th.StringType),
        th.Property("_sdc_campaign_id", th.StringType),
    ).to_dict()
