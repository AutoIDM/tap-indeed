"""IndeedSponsoredJobs Authentication."""

import requests
from singer_sdk.authenticators import OAuthAuthenticator
from singer_sdk.streams import Stream as RESTStreamBase
from singer_sdk.helpers._util import utc_now


class IndeedSponsoredJobsAuthenticator(OAuthAuthenticator):
    """Authenticator class for IndeedSponsoredJobs."""
    
    def __init__(
        self,
        stream: RESTStreamBase,
        auth_endpoint,
        oauth_scopes,
        default_expiration=None,
        employerid=None,
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
