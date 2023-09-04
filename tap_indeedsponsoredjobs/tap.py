"""IndeedSponsoredJobs tap class."""

import datetime
from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_indeedsponsoredjobs.streams import (
    CampaignBudget,
    CampaignInfo,
    CampaignJobDetails,
    CampaignProperties,
    Campaigns,
    Employers,
)


class TapIndeedSponsoredJobs(Tap):
    """IndeedSponsoredJobs tap class."""

    name = "tap-indeedsponsoredjobs"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "campaign_status",
            th.StringType,
            required=True,
            default="Active",
            description=(
                "Campaign Status to filter on. Defaults to 'Active' alternatives are "
                "ACTIVE, DELETED, PAUSED",
            ),
        ),
        th.Property(
            "client_id",
            th.StringType,
            required=True,
            description="client_id from https://secure.indeed.com/account/apikeys",
        ),
        th.Property(
            "client_secret",
            th.StringType,
            required=True,
            description="client_secret from https://secure.indeed.com/account/apikeys",
        ),
        th.Property(
            "employer_ids",
            th.ArrayType(th.StringType),
            required=False,
            description=(
                "List of employer ids to use for children of the employer stream. If "
                "left unset, will pull all employer ids."
            ),
        ),
        th.Property(
            "threading_enable",
            th.BooleanType,
            required=True,
            default=False,
            description=("Whether to use threading to run some streams in parallel."),
        ),
        th.Property(
            "start_date",
            th.StringType,
            required=True,
            default=str(datetime.date.today() - datetime.timedelta(days=365)),
            description=(
                "Defaults to today minus 365, only used for the stats endpoint"
                "Note that the Campaign Performance Stats stream will only go back "
                "a total of 365 days."
            ),
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""

        campaign_threaded_data = {}

        return [
            Employers(tap=self),
            Campaigns(campaign_threaded_data=campaign_threaded_data, tap=self),
            CampaignProperties(campaign_threaded_data=campaign_threaded_data, tap=self),
            CampaignJobDetails(campaign_threaded_data=campaign_threaded_data, tap=self),
            CampaignBudget(campaign_threaded_data=campaign_threaded_data, tap=self),
            CampaignInfo(campaign_threaded_data=campaign_threaded_data, tap=self),
            # CampaignPerformanceStats(tap=self),
            # EmployerStatsReport(tap=self),
        ]


if __name__ == "__main__":
    TapIndeedSponsoredJobs.cli()
