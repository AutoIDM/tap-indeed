"""Tests standard tap features using the built-in SDK tests library."""

import datetime
import json

import backoff
import pytest
import responses
from singer_sdk.exceptions import RetriableAPIError

from tap_indeedsponsoredjobs.auth import IndeedSponsoredJobsAuthenticator
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


def test_auth_backoff(mocked_responses, monkeypatch):
    body = {
        "scope": "employer.advertising.subaccount.read employer.advertising.account.read employer.advertising.campaign.read employer.advertising.campaign_report.read employer_access",
        "client_id": "abc",
        "client_secret": "abc",
        "grant_type": "client_credentials",
    }
    mocked_responses.add(
        responses.POST,
        "https://apis.indeed.com/oauth/v2/tokens",
        body=json.dumps(body),
        status=400,
        content_type="application/json",
    )
    monkeypatch.setattr(
        IndeedSponsoredJobsAuthenticator,
        "backoff_wait_generator",
        lambda self: backoff.constant(interval=0),
    )
    tap = TapIndeedSponsoredJobs(config=SAMPLE_CONFIG, parse_env_config=False)
    employer_stream = tap.streams["employers"]
    auth = IndeedSponsoredJobsAuthenticator.create_multiemployerauth_for_stream(
        employer_stream
    )
    with pytest.raises(RetriableAPIError):
        auth.get_auth_headers()
    # 400s from the token endpoint are retried before giving up
    assert len(mocked_responses.calls) == 10


# Run standard built-in tap tests from the SDK:
# Disabled as we don't have a good way to test these without production credentails
# def test_standard_tap_tests():
#    """Run standard tap tests from the SDK."""
#    tests = get_standard_tap_tests(TapIndeedSponsoredJobs, config=SAMPLE_CONFIG)
#    for test in tests:
#       test()


# TODO: Create additional tests as appropriate for your tap.
