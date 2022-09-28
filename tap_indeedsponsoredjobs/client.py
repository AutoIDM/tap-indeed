"""REST client handling, including IndeedSponsoredJobsStream base class."""

from __future__ import annotations
import requests
import json
import logging
import cloudscraper
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable
from urllib.parse import urlparse
from collections import OrderedDict

from memoization import cached

from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream
from singer_sdk.pagination import BaseHATEOASPaginator, first, BaseAPIPaginator

from tap_indeedsponsoredjobs.auth import IndeedSponsoredJobsAuthenticator
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError


SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

class ScopeNotWorkingForEmployerID(Exception):
    """Raised if a target receives RECORD messages prior to a SCHEMA message."""

class HATEOASPaginator(BaseHATEOASPaginator):
    def get_next_url(self, response):
        """Override this method to extract a HATEOAS link from the response.
        Args:
            response: API response object.
        """
        try: 
            retval = first(
                    extract_jsonpath("$['meta']['links'][?(@['rel']=='next')]['href']", response.json())
                )
            raise Exception("Pagination not implemented yet, but we require pagination here. With perpage being so high this is surprising!")
            return retval
        except StopIteration:
            return None

class IndeedSponsoredJobsStream(RESTStream):
    """IndeedSponsoredJobs stream class."""

    url_base = "https://apis.indeed.com/ads"
    _LOG_REQUEST_METRICS: bool = True
    # Disabled by default for safety:
    _LOG_REQUEST_METRIC_URLS: bool = True

    # OR use a dynamic url_base:
    # @property
    # def url_base(self) -> str:
    #     """Return the API URL root, configurable via tap settings."""
    #     return self.config["api_url"]

    #records_jsonpath = "$[*]"  # Or override `parse_response`.
    #next_page_token_jsonpath = "$['meta']['links'][?(@['rel']=='next')]['href']"
    
    def __init__(self, *args, **kwargs) -> None:
        """Initialize the REST stream.

        Args:
            tap: Singer Tap this stream belongs to.
            schema: JSON schema for records in this stream.
            name: Name of this stream.
            path: URL path for this entity stream.
        """
        super().__init__(*args, **kwargs)
        self.requests_session = cloudscraper.create_scraper(sess=self.requests_session)

    @property
    def requests_session(self) -> requests.Session:
        """Get requests session.

        Returns:
            The `requests.Session`_ object for HTTP requests.

        .. _requests.Session:
            https://requests.readthedocs.io/en/latest/api/#request-sessions
        """
        if not self._requests_session:
            self._requests_session = cloudscraper.create_scraper()
        return self._requests_session
    
    @requests_session.setter
    def requests_session(self, session):
        """Get requests session.

        Returns:
            The `requests.Session`_ object for HTTP requests.

        .. _requests.Session:
            https://requests.readthedocs.io/en/latest/api/#request-sessions
        """
        self._requests_session = session

    def get_new_paginator(self) -> BaseAPIPaginator:
        """Get a fresh paginator for this API endpoint.

        Returns:
            A paginator instance.
        """
        return HATEOASPaginator()
    def prepare_request(
        self, context, next_page_token
    ) -> requests.PreparedRequest:
        """Prepare a request object for this stream.

        If partitioning is supported, the `context` object will contain the partition
        definitions. Pagination information can be parsed from `next_page_token` if
        `next_page_token` is not None.

        Args:
            context: Stream partition or context dictionary.
            next_page_token: Token, page number or any request argument to request the
                next page of data.

        Returns:
            Build a request with the stream's URL, path, query parameters,
            HTTP headers and authenticator.
        """
        http_method = self.rest_method
        url: str = self.get_url(context)
        params: dict = self.get_url_params(context, next_page_token)
        request_data = self.prepare_request_payload(context, next_page_token)
        headers = self.http_headers

        return self.build_prepared_request(
            method=http_method,
            url=url,
            params=params,
            headers=headers,
            json=request_data,
            context=context,
        )

    def build_prepared_request(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> requests.PreparedRequest:
        """Build a generic but authenticated request.

        Uses the authenticator instance to mutate the request with authentication.

        Args:
            *args: Arguments to pass to `requests.Request`_.
            **kwargs: Keyword arguments to pass to `requests.Request`_.

        Returns:
            A `requests.PreparedRequest`_ object.

        .. _requests.PreparedRequest:
            https://requests.readthedocs.io/en/latest/api/#requests.PreparedRequest
        .. _requests.Request:
            https://requests.readthedocs.io/en/latest/api/#requests.Request
        """
        context = kwargs.pop("context") #Hack as we need a different authenticator based on context
        request = requests.Request(*args, **kwargs)

        if context and context["_sdc_employer_id"]:
            authenticator = self.authenticator(employerid=context["_sdc_employer_id"])
        else:
            authenticator = self.authenticator()
        authenticator.authenticate_request(request)

        return self.requests_session.prepare_request(request)

    @cached
    def authenticator(self, employerid):
        """Return a new authenticator object."""
        return IndeedSponsoredJobsAuthenticator.create_singleemployerauth_for_stream(self, employerid)

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        headers["User-Agent"] = self.config.get("user_agent", "AutoIDM") # If set to python-requests/2.28.1 you will 403 so I chose AutoIDM by default
        headers["Accept"]="application/json"
        return headers

    def get_next_page_token(
            self, response: requests.Response, previous_token: Optional[Any]
            ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        # TODO: If pagination is required, return a token which can be used to get the
        #       next page. If this is the final page, return "None" to end the
        #       pagination loop.
        if self.next_page_token_jsonpath:
            all_matches = extract_jsonpath(
                    self.next_page_token_jsonpath, response.json()
                    )
            first_match = next(iter(all_matches), None)
            next_page_token = first_match
        else:
            next_page_token = response.headers.get("X-Next-Page", None)

        return next_page_token

    def get_url_params(
            self, context: Optional[dict], next_page_token: Optional[Any]
            ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        #if next_page_token:
        #    params["page"] = next_page_token
        #if self.replication_key:
        #    params["sort"] = "asc"
        #    params["order_by"] = self.replication_key
        params["perPage"]=1000000000
        return params

    def prepare_request_payload(
            self, context: Optional[dict], next_page_token: Optional[Any]
            ) -> Optional[dict]:
        """Prepare the data payload for the REST API request.

        By default, no payload will be sent (return None).
        """
        # TODO: Delete this method if no payload is required. (Most REST APIs.)
        return None

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records."""
        # TODO: Parse response body and return a set of records.
        yield from extract_jsonpath(self.records_jsonpath, input=response.json())

    def post_process(self, row: dict, context: Optional[dict]) -> dict:
        """As needed, append or transform raw data to match expected structure."""
        # TODO: Delete this method if not needed.
        return row

    def validate_response(self, response: requests.Response) -> None:
        """Validate HTTP response.

        Checks for error status codes and wether they are fatal or retriable.

        In case an error is deemed transient and can be safely retried, then this
        method should raise an :class:`singer_sdk.exceptions.RetriableAPIError`.
        By default this applies to 5xx error codes, along with values set in:
        :attr:`~singer_sdk.RESTStream.extra_retry_statuses`

        In case an error is unrecoverable raises a
        :class:`singer_sdk.exceptions.FatalAPIError`. By default, this applies to
        4xx errors, excluding values found in:
        :attr:`~singer_sdk.RESTStream.extra_retry_statuses`

        Tap developers are encouraged to override this method if their APIs use HTTP
        status codes in non-conventional ways, or if they communicate errors
        differently (e.g. in the response body).

        .. image:: ../images/200.png

        Args:
            response: A `requests.Response`_ object.

        Raises:
            FatalAPIError: If the request is not retriable.
            RetriableAPIError: If the request is retriable.

        .. _requests.Response:
            https://requests.readthedocs.io/en/latest/api/#requests.Response
        """
        if (
            response.status_code in self.extra_retry_statuses
            or 500 <= response.status_code < 600
        ):
            msg = self.response_error_message(response)
            raise RetriableAPIError(msg, response)

        elif 403 == response.status_code and self.is_json(response.text) and response.json()["meta"]["errors"][0]["type"]=="INSUFFICIENT_SCOPE":
            msg = self.response_error_message(response)
            raise  ScopeNotWorkingForEmployerID(msg)

        elif 400 <= response.status_code < 500:
           msg = self.response_error_message(response)
           raise FatalAPIError(msg)

    def is_json(self, myjson):
        try:
            json.loads(myjson)
        except ValueError as e:
            return False
        return True


    def response_error_message(self, response: requests.Response) -> str:
        """Build error message for invalid http statuses.

        WARNING - Override this method when the URL path may contain secrets or PII

        Args:
            response: A `requests.Response`_ object.

        Returns:
            str: The error message
        """
        full_path = urlparse(response.url).path or self.path
        if 400 <= response.status_code < 500:
            error_type = "Client"
        else:
            error_type = "Server"

        return (
            f"{response.status_code} {error_type} Error: "
            f"{response.reason} for path: {full_path}. "
            f"{response.text=}"
        )

    def get_records(self, context: dict | None) -> Iterable[dict[str, Any]]:
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        try:
            for record in self.request_records(context):
                transformed_record = self.post_process(record, context)
                if transformed_record is None:
                    # Record filtered out during post_process()
                    continue
                yield transformed_record
        except ScopeNotWorkingForEmployerID as e:
            self.logger.warning(e)
