"""End-to-end sync tests against a mocked Indeed API."""

import datetime
import json
import re

import pytest
import responses

from tap_indeedsponsoredjobs.tap import TapIndeedSponsoredJobs

ADS = "https://apis.indeed.com/ads"
START_DATE = (datetime.date.today() - datetime.timedelta(days=2)).isoformat()


def _json(rsps, url, body):
    rsps.add(responses.GET, re.compile(re.escape(url) + r"(\?.*)?$"), json=body)


def register_mocks(rsps):
    """Register mocked Indeed API responses."""
    rsps.add(
        responses.POST,
        "https://apis.indeed.com/oauth/v2/tokens",
        json={"access_token": "token", "expires_in": 3600},
    )
    _json(
        rsps,
        "https://secure.indeed.com/v2/api/appinfo",
        {"employers": [{"id": "emp1", "name": "Employer One"}]},
    )
    _json(
        rsps,
        f"{ADS}/v1/campaigns",
        {
            "data": {
                "Campaigns": [{"Name": "Campaign 1", "Id": "c1", "Status": "ACTIVE"}]
            },
            "meta": {"links": []},
        },
    )
    _json(rsps, f"{ADS}/v1/campaigns/c1/budget", {"data": {"budgetMonthlyLimit": 250}})
    _json(
        rsps,
        f"{ADS}/v1/campaigns/c1",
        {
            "data": {
                "Name": "Campaign 1",
                "Id": "c1",
                "Type": "SPONSORED",
                "Status": "ACTIVE",
                "CurrencyCode": "USD",
                "TrackingToken": "tt",
                "Objective": {"description": "d", "objectiveType": "o"},
                "NonSpendingReasons": [{"type": "t", "description": "d"}],
                "SpendingChannels": ["INDEED"],
            }
        },
    )
    _json(
        rsps,
        f"{ADS}/v1/campaigns/c1/properties",
        {
            "data": {
                "dateCreated": "2025-01-01",
                "firstSpendDate": "2025-01-02",
                "lastSpendDate": "2025-01-03",
            }
        },
    )
    _json(
        rsps,
        f"{ADS}/v1/campaigns/c1/jobDetails",
        {
            "data": {
                "entries": [
                    {
                        "jobKey": "jk1",
                        "refNum": "r1",
                        "title": "Engineer",
                        "location": "Remote",
                    }
                ]
            }
        },
    )
    _json(
        rsps,
        f"{ADS}/v1/campaigns/c1/stats",
        {
            "data": {
                "entries": [
                    {
                        "Name": "Campaign 1",
                        "Id": "c1",
                        "Date": START_DATE,
                        "Clicks": 3,
                        "Impressions": 40,
                        "Conversions": 1,
                        "CurrencyCode": "USD",
                        "Cost": 1.5,
                    }
                ]
            }
        },
    )
    _json(rsps, f"{ADS}/v1/stats", {"data": {"location": "/v1/stats/report/r1"}})
    rsps.add(
        responses.GET,
        re.compile(re.escape(f"{ADS}/v1/stats/report/r1") + r"(\?.*)?$"),
        body="Campaign ID,Clicks\nc1,3\nc1,4\n",
    )


@pytest.fixture
def mocked_api():
    with responses.RequestsMock(assert_all_requests_are_fired=False) as rsps:
        register_mocks(rsps)
        yield rsps


@pytest.mark.parametrize("threading_enable", [False, True])
def test_sync_all(mocked_api, capsys, threading_enable):
    tap = TapIndeedSponsoredJobs(
        config={
            "client_id": "abc",
            "client_secret": "abc",
            "campaign_status": "Active",
            "start_date": START_DATE,
            "threading_enable": threading_enable,
        },
        parse_env_config=False,
    )
    tap.sync_all()

    records: dict = {}
    for line in capsys.readouterr().out.splitlines():
        message = json.loads(line)
        if message["type"] == "RECORD":
            records.setdefault(message["stream"], []).append(message["record"])

    assert records["employers"] == [{"id": "emp1", "name": "Employer One"}]
    assert records["campaigns"] == [
        {
            "Name": "Campaign 1",
            "Id": "c1",
            "Status": "ACTIVE",
            "_sdc_employer_id": "emp1",
        }
    ]
    assert records["campaign_budget"] == [
        {
            "budgetMonthlyLimit": 250,
            "_sdc_employer_id": "emp1",
            "_sdc_campaign_id": "c1",
        }
    ]
    assert records["campaign_info"][0]["Objective"] == {
        "description": "d",
        "objectiveType": "o",
    }
    assert records["campaign_property"][0]["firstSpendDate"] == "2025-01-02"
    assert records["campaign_job_detail"][0]["jobKey"] == "jk1"
    assert records["campaign_performance_stats"][0]["Cost"] == 1.5
    assert records["campaign_performance_stats"][0]["_sdc_campaign_id"] == "c1"

    # One report per day between start_date and today, two CSV lines each
    report = records["employer_stats_report"]
    assert len(report) == 4
    assert {r["_sdc_start_date"][:10] for r in report} == {
        START_DATE,
        (datetime.date.today() - datetime.timedelta(days=1)).isoformat(),
    }
    assert report[0]["Campaign ID"] == "c1"
    assert report[0]["Clicks"] == "3"
    assert report[0]["_sdc_line_number"] == "2"
    assert report[0]["_sdc_employer_id"] == "emp1"
