"""Stream type classes for tap-indeedsponsoredjobs."""

from __future__ import annotations

import csv
import datetime
from pathlib import Path
import threading
from typing import Any, Dict, Iterable, List, Optional, Union

import pendulum
import requests
from memoization import cached
from requests import PreparedRequest
from singer_sdk import typing as th  # JSON Schema typing helpers
from singer_sdk.authenticators import OAuthAuthenticator
from singer_sdk.pagination import BaseAPIPaginator, SimpleHeaderPaginator
from singer_sdk.helpers.jsonpath import extract_jsonpath

from tap_indeedsponsoredjobs.auth import IndeedSponsoredJobsAuthenticator
from tap_indeedsponsoredjobs.client import (
    IndeedSponsoredJobsStream,
    ScopeNotWorkingForEmployerID,
)

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
        th.Property("_sdc_start_date", th.DateTimeType),
        th.Property("_sdc_end_date", th.DateTimeType),
        th.Property("_sdc_line_number", th.CustomType({"type": ["string", "null"]})),
    ).to_dict()

    def get_url_params(
        self,
        context: Optional[dict],
        next_page_token,
    ) -> Dict[str, Any]:
        params: dict = {}
        params["startDate"] = self.start_date_format
        params["endDate"] = self.end_date_format
        params["v"] = "6"  # Report version 6
        return params
    
    @property
    def start_date(self) -> pendulum.DateTime:
        return self._start_date

    @start_date.setter
    def start_date(self, value):
        self._start_date = value

    @property
    def start_date_format(self) -> str:
        return self.start_date.format("YYYY-MM-DD")

    @property
    def end_date_format(self) -> str:
        return self.start_date.add(days=1).format("YYYY-MM-DD")

    def get_records(self, context: dict | None) -> Iterable[dict[str, Any]]:
        """Return a generator of record-type dictionary objects.

        To get all reports we must iterate over each day individually according to the Indeed API

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        try:
            starting_timestamp: datetime.datetime | None =  \
                self.get_starting_timestamp(context)
            if starting_timestamp is None:
                raise Exception("No start date provided. Please provide a start date.")
            initial_start_date: pendulum.DateTime = \
                pendulum.instance(starting_timestamp)
            # Set instance var start date for the get_url_params function
            self.start_date = initial_start_date

            while self.start_date.date() != pendulum.today().date():
                yield from self.get_single_report(context)
                self.start_date = self.start_date.add(days=1)
                self.logger.info(
                    f"We have { pendulum.today().diff(self.start_date).in_days() } day(s) left. Fetching {self.start_date_format} to {self.end_date_format} next."
                )
        except ScopeNotWorkingForEmployerID as e:
            self.logger.warning(e)

    def get_single_report(self, context):
        """Get a single report for a given date range.

        Args:
            context: Context dictionary.
            start_date: The start date for the report.
            end_date: The end date for the report.

        Returns:
            Nothing
        """
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
            record["_sdc_employer_id"] = context["_sdc_employer_id"]
            record["_sdc_start_date"] = self.get_url_params(context, None)["startDate"]
            record["_sdc_end_date"] = self.get_url_params(context, None)["endDate"]
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
            csv_data = csv.DictReader(response.text.splitlines())
            has_data = False
            for record in csv_data:
                has_data = True
                record["_sdc_line_number"] = str(csv_data.line_num)
                yield record
            if has_data is False:
                yield {"_sdc_line_number": None}



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
        campaign_status = self.config["campaign_status"].upper()
        assert campaign_status in ["ACTIVE", "PAUSED", "DELETED"]
        params["status"] = campaign_status
        return params

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Return a context dictionary for child streams."""

        # The name of each threaded child stream and its associated url, for use in the
        # manage_threads() function. Name is required so that manage_threads() can store
        # data in a way that allows child streams to differentiate which data belongs to
        # them. Adding url base and record["Id"] could be done inside manage_threads(),
        # but it seemed cleaner to do it all here.
        endpoints = {
            "campaign_budget": f"{self.url_base}/v1/campaigns/{record['Id']}/budget",
            "campaign_info": f"{self.url_base}/v1/campaigns/{record['Id']}",
            "campaign_property": f"{self.url_base}/v1/campaigns/{record['Id']}/properties",
            "campaign_job_detail": f"{self.url_base}/v1/campaigns/{record['Id']}/jobDetails",
        }

        threaded_data = self.manage_threads(endpoints=endpoints, context=context)

        return {
            "_sdc_employer_id": context["_sdc_employer_id"],
            "_sdc_campaign_id": record["Id"],
            "_threaded_data": threaded_data,
        }
    
    def manage_threads(self, endpoints: dict[str, str], context) -> dict[str,dict]:
        """Manages a series of threads determined by a dict of endpoints.

        Args:
            endpoints: A dictionary of endpoints. Each will have its own thread.
        """
        threads: list[threading.Thread] = []
        results = {}

        # Start each thread, keeping track of them in an array.
        for name, endpoint in endpoints.items():
            new_thread = threading.Thread(group=None, target=self.thread_stream, args=[name,endpoint,results,context])
            threads.append(new_thread)
            new_thread.start()

        # Wait for the completion of all threads before continuing.
        for thread in threads:
            thread.join()

        return results

    def thread_stream(self, name, endpoint, results, context) -> None:
        """Hits an endpoint and adds the response to the results dictionary.

        Args:
            name: The key to use in the results dictionary when adding data.
            endpoint: The endpoint (full URL) to get data from.
            results: A dictionary where returned data should be added.
            context: Relevant context for the stream.
        """

        results[name] = []

        paginator = self.get_new_paginator()
        decorated_request = self.request_decorator(self._request)

        while not paginator.finished:

            # This code cuts out the prepare_request() intermediary and goes straight to
            # build_prepared_request() because prepare_request() relies on certain
            # instance variable being set in ways that paralell execution can't
            # guarantee.
            prepared_request = self.build_prepared_request(
                method="GET",
                url=endpoint,
                params=super().get_url_params(context, paginator.current_value),
                headers=self.http_headers,
                json=None,
                context=context,
            )

            resp = decorated_request(prepared_request, context)
            results[name].append(resp.json())
            paginator.advance(resp)


class CampaignPerformanceStats(IndeedSponsoredJobsStream):
    """Campaign Performance per Campaign. Note we limit the data set to be one year old."""

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
        params["perPage"] = 1000000000

        start_date = self.get_starting_replication_key_value(context)
        start_date = pendulum.parse(start_date)

        days_from_today = pendulum.today().diff(start_date).in_days()
        if days_from_today > 365:
            start_date = pendulum.today().subtract(days=365)
            self.logger.info(
                f"We have {days_from_today} day(s) left. "
                "The campaign stats endpoint only allows "
                "a maximum of 365 days of data. "
                f"We are modifiying the start_date to be {start_date}"
            )

        params["startDate"] = start_date.format("YYYY-MM-DD")
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

    def request_records(self, context: dict | None) -> Iterable[dict]:
        for input in context["_threaded_data"][self.name]:
            yield from extract_jsonpath(self.records_jsonpath, input=input)


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

    def request_records(self, context: dict | None) -> Iterable[dict]:
        for input in context["_threaded_data"][self.name]:
            yield from extract_jsonpath(self.records_jsonpath, input=input)


class CampaignProperties(IndeedSponsoredJobsStream):
    """Campaign Properties per Campaign"""

    name = "campaign_property"
    path = "/v1/campaigns/{_sdc_campaign_id}/properties"
    primary_keys = ["_sdc_campaign_id"]
    records_jsonpath = "$.['data']"
    replication_key = None
    parent_stream_type = Campaigns
    schema = th.PropertiesList(
        th.Property("dateCreated", th.DateType),
        th.Property("firstSpendDate", th.DateType),
        th.Property("lastSpendDate", th.DateType),
        th.Property("_sdc_campaign_id", th.StringType),
        th.Property("_sdc_employer_id", th.StringType),
    ).to_dict()

    def request_records(self, context: dict | None) -> Iterable[dict]:
        for input in context["_threaded_data"][self.name]:
            yield from extract_jsonpath(self.records_jsonpath, input=input)


class CampaignJobDetails(IndeedSponsoredJobsStream):
    """Job Details per Campaign"""

    name = "campaign_job_detail"
    path = "/v1/campaigns/{_sdc_campaign_id}/jobDetails"
    primary_keys = ["_sdc_campaign_id", "jobKey", "_sdc_employer_id", "refNum"]
    records_jsonpath = "$.['data']['entries'][*]"
    replication_key = None
    parent_stream_type = Campaigns
    schema = th.PropertiesList(
        th.Property("jobKey", th.StringType),
        th.Property("refNum", th.StringType),
        th.Property("title", th.StringType),
        th.Property("location", th.StringType),
        th.Property("_sdc_campaign_id", th.StringType),
        th.Property("_sdc_employer_id", th.StringType),
    ).to_dict()

    def request_records(self, context: dict | None) -> Iterable[dict]:
        for input in context["_threaded_data"][self.name]:
            yield from extract_jsonpath(self.records_jsonpath, input=input)
