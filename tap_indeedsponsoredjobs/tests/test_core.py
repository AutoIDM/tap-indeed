"""Tests standard tap features using the built-in SDK tests library."""

import datetime
import pytest
import json
from tap_indeedsponsoredjobs.auth import IndeedSponsoredJobsAuthenticator
from tap_indeedsponsoredjobs.tap import TapIndeedSponsoredJobs

from singer_sdk.testing import get_standard_tap_tests
import responses

from tap_indeedsponsoredjobs.tap import TapIndeedSponsoredJobs

SAMPLE_CONFIG = {
    "start_date": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d"),
    "client_id": "abc",
    "client_secret": "abc",
    }

@pytest.fixture
def mocked_responses():
    with responses.RequestsMock() as rsps:
        yield rsps



def test_auth_backoff(mocked_responses):
    body = { "scope": "employer.advertising.subaccount.read employer.advertising.account.read employer.advertising.campaign.read employer.advertising.campaign_report.read employer_access",
      "client_id": "abc",
      "client_secret": "abc",
      "grant_type": "client_credentials",
    }
    mocked_responses.add(
            responses.POST,
            "https://apis.indeed.com/oauth/v2/tokens",
            body = json.dumps(body),
            status=400,
            content_type="application/json"
            )
    tap = TapIndeedSponsoredJobs(config=SAMPLE_CONFIG, parse_env_config=False)
    employer_stream = tap.streams["employers"]
    auth = IndeedSponsoredJobsAuthenticator(stream=employer_stream, oauth_scopes=body["scope"], auth_endpoint="https://apis.indeed.com/oauth/v2/tokens")
    headers = auth.auth_headers


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests():
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(
        TapIndeedSponsoredJobs,
        config=SAMPLE_CONFIG
    )
    for test in tests:
        test()


# TODO: Create additional tests as appropriate for your tap.
