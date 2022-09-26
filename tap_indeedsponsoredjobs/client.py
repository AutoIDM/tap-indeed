"""REST client handling, including IndeedSponsoredJobsStream base class."""

import requests
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable

from memoization import cached

from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.streams import RESTStream
from singer_sdk.pagination import BaseHATEOASPaginator, first, BaseAPIPaginator

from tap_indeedsponsoredjobs.auth import IndeedSponsoredJobsAuthenticator


SCHEMAS_DIR = Path(__file__).parent / Path("./schemas")

class HATEOASPaginator(BaseHATEOASPaginator):
    def get_next_url(self, response):
        """Override this method to extract a HATEOAS link from the response.
        Args:
            response: API response object.
        """
        try: 
            return first(
                    extract_jsonpath("$['meta']['links'][?(@['rel']=='next')]['href']", response.json())
                )
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

        if self.authenticator:
            if context and context.get["_sdc_employer_id"]:
                authenticator = self.authenticator(context.get["_sdc_employer_id"])
            else:
                authenticator = self.authenticator
            authenticator.authenticate_request(request)

        return self.requests_session.prepare_request(request)

    @property
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
        params["perPage"]=10000000000
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
