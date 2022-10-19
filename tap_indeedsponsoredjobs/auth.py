"""IndeedSponsoredJobs Authentication."""

import requests
import backoff
import logging
from urllib.parse import urlparse
from singer_sdk.authenticators import OAuthAuthenticator
from singer_sdk.streams import RESTStream
from singer_sdk.helpers._util import utc_now
from requests import Request
from requests import Session
from typing import Callable, Generator
from singer_sdk.exceptions import FatalAPIError, RetriableAPIError


class IndeedSponsoredJobsAuthenticator(OAuthAuthenticator):
    """Authenticator class for IndeedSponsoredJobs."""
    
    def __init__(
        self,
        stream: RESTStream,
        auth_endpoint,
        oauth_scopes,
        default_expiration=None,
        employerid=None
    ) -> None:
        """Create a new authenticator.

        Args:
            stream: The stream instance to use with this authenticator.
            auth_endpoint: API username.
            oauth_scopes: API password.
            default_expiration: Default token expiry in seconds.
        """
        super().__init__(stream=stream, auth_endpoint=auth_endpoint, oauth_scopes=oauth_scopes, default_expiration=default_expiration)
        self._employerid = employerid
        self._user_agent = stream.http_headers["User-Agent"] #Cloud Flare is blocking us with a 1020 error.
        self._session = stream.requests_session

    @property
    def employerid(self):
        """Employer ID so we can auth as each client individually

        Returns:
            employerid 
        """
        return self._employerid

    @property
    def oauth_request_body(self) -> dict:
        """Define the OAuth request body for the IndeedSponsoredJobs API."""
        oauth_request_body =  {
            'scope': self.oauth_scopes,
            'client_id': self.config["client_id"],
            'client_secret': self.config["client_secret"],
            'grant_type': 'client_credentials',
        }
        if self.employerid:
            oauth_request_body["employer"]=self.employerid
        return oauth_request_body

    @classmethod
    def create_multiemployerauth_for_stream(cls, stream):
        return cls(
            stream=stream,
            auth_endpoint="https://apis.indeed.com/oauth/v2/tokens",
            oauth_scopes="employer.advertising.subaccount.read employer.advertising.account.read employer.advertising.campaign.read employer.advertising.campaign_report.read employer_access",
        )
    
    @classmethod
    def create_singleemployerauth_for_stream(cls, stream, employerid):
        return cls(
            stream=stream,
            auth_endpoint="https://apis.indeed.com/oauth/v2/tokens",
            oauth_scopes="employer.advertising.subaccount.read employer.advertising.account.read employer.advertising.campaign.read employer.advertising.campaign_report.read",
            employerid=employerid,
        )
    
    @property
    def auth_headers(self) -> dict:
        """Return a dictionary of auth headers to be applied.

        These will be merged with any `http_headers` specified in the stream.

        Returns:
            HTTP headers for authentication.
        """
        if not self.is_token_valid():
            self.update_access_token()
        result = super().auth_headers
        result["Authorization"] = f"Bearer {self.access_token}"
        return result
    
    def update_access_token(self) -> None:
        """Update `access_token` along with: `last_refreshed` and `expires_in`.

        Raises:
            RuntimeError: When OAuth login fails.
        """
        request_time = utc_now()
        auth_request_payload = self.oauth_request_payload
        #Back off for Auth requsts as we do so many sometimes they fail with Docusign
        decorated_request = self.request_decorator(self.req)
        #Using a shared session with the Stream here
        token_response = decorated_request(session=self._session, url=self.auth_endpoint, data=auth_request_payload, headers={"User-Agent":self._user_agent})
        self.logger.info("OAuth authorization attempt was successful.")
        token_json = token_response.json()
        self.access_token = token_json["access_token"]
        self.expires_in = token_json.get("expires_in", self._default_expiration)
        if self.expires_in is None:
            self.logger.debug(
                "No expires_in receied in OAuth response and no "
                "default_expiration set. Token will be treated as if it never "
                "expires."
            )
        self.last_refreshed = request_time
    
    def req(self, session: Session, url, data, headers) -> requests.Response:
        response = session.post(url=url, data=data, headers=headers)
        self.validate_response(response)
        return response

    
    def request_decorator(self, func: Callable) -> Callable:
        """Instantiate a decorator for handling request failures.

        Uses a wait generator defined in `backoff_wait_generator` to
        determine backoff behaviour. Try limit is defined in
        `backoff_max_tries`, and will trigger the event defined in
        `backoff_handler` before retrying. Developers may override one or
        all of these methods to provide custom backoff or retry handling.

        Args:
            func: Function to decorate.

        Returns:
            A decorated method.
        """
        decorator: Callable = backoff.on_exception(
            self.backoff_wait_generator,
            (
                RetriableAPIError,
                requests.exceptions.ReadTimeout,
                requests.exceptions.ConnectionError,
            ),
            max_tries=10,
            on_backoff=self.backoff_handler,
        )(func)
        return decorator

    def backoff_wait_generator(self):
        return backoff.expo(factor=2)
	
    def backoff_handler(self, details) -> None:
        """Adds additional behaviour prior to retry.

          By default will log out backoff details, developers can override
          to extend or change this behaviour.

          Args:
              details: backoff invocation details
                  https://github.com/litl/backoff#event-handlers
          """
        logging.error(
            "Backing off {wait:0.1f} seconds after {tries} tries "
            "calling function {target} with args {args} and kwargs "
            "{kwargs}".format(**details)
        )
    
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
            #400 Code because we get these randomly from Docusign
            response.status_code == 400 or (response.status_code >= 500 <= response.status_code < 600)
        ):
            msg = self.response_error_message(response)
            raise RetriableAPIError(msg, response)

        elif 400 <= response.status_code < 500:
           msg = self.response_error_message(response)
           raise FatalAPIError(msg)
    def response_error_message(self, response: requests.Response) -> str:
        """Build error message for invalid http statuses.

        WARNING - Override this method when the URL path may contain secrets or PII

        Args:
            response: A `requests.Response`_ object.

        Returns:
            str: The error message
        """
        full_path = urlparse(response.url).path
        if 400 <= response.status_code < 500:
            error_type = "Client"
        else:
            error_type = "Server"

        return (
            f"{response.status_code} {error_type} Error: "
            f"{response.reason} for path: {full_path}. "
            f"{response.text=}"
        )
