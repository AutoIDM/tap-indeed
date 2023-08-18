# tap-indeedsponsoredjobs
# `tap-indeedsponsoredjobs`

## Capabilities

* `catalog`
* `state`
* `discover`
* `about`
* `stream-maps`
* `schema-flattening`

## Settings

## Settings

| Setting             | Required | Default | Description |
|:--------------------|:--------:|:-------:|:------------|
| campaign_status     | True     | Active  | Campaign Status to filter on. Defaults to 'Active' alternatives are ACTIVE, DELETED, PAUSED |
| client_id           | True     | None    | client_id from https://secure.indeed.com/account/apikeys |
| client_secret       | True     | None    | client_secret from https://secure.indeed.com/account/apikeys |
| employer_ids        | False    | None    | List of employer ids to use for children of the employer stream. If left unset, will pull all employer ids. |
| start_date          | True     | today - 365 days | Defaults to today minus 365, only used for the stats endpointNote that the Campaign Performance Stats stream will only go back a total of 365 days. |
| stream_maps         | False    | None    | Config object for stream maps capability. For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). |
| stream_map_config   | False    | None    | User-defined config values to be used within map expressions. |
| flattening_enabled  | False    | None    | 'True' to enable schema flattening and automatically expand nested properties. |
| flattening_max_depth| False    | None    | The max depth to flatten schemas. |

A full list of supported settings and capabilities is available by running: `tap-indeedsponsoredjobs --about`


Built with the [Meltano Tap SDK](https://sdk.meltano.com) for Singer Taps.

###  INSUFFICIENT SCOPE Error
Something you may see an error like
```
 WARNING:tap-indeedsponsoredjobs:403 Client Error: Forbidden for path: /ads/v1/campaigns. response.text='{"meta":{"status":403,"errors":[{"type":"INSUFFICIENT_SCOPE","description":"The access token you provided doesn\'t have permission to access this API. Required scopes missing: employer.advertising.campaign.read"}],"rootLocation":"https://apis.indeed.com/ads","perPage":null,"links":null},"data":null}' 
 ```

 This is expected and is an error we get with some employer's. We are properly requesting the right scopes. This may be an error with Indeed's API. It may be somethign else, but most records work so ignoring these seems correct.

## Installation

Install from PyPi:

```bash
pipx install tap-indeedsponsoredjobs
```

Install from GitHub:

```bash
pipx install git+https://github.com/AutoIDM/tap-indeedsponsoredjobs.git@main
```

## Configuration

### Accepted Config Options

<!--
Developer TODO: Provide a list of config options accepted by the tap.

This section can be created by copy-pasting the CLI output from:

```
tap-indeedsponsoredjobs --about --format=markdown
```
-->

A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-indeedsponsoredjobs --about
```

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

<!--
Developer TODO: If your tap requires special access on the source system, or any special authentication requirements, provide those here.
-->

## Usage

You can easily run `tap-indeedsponsoredjobs` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-indeedsponsoredjobs --version
tap-indeedsponsoredjobs --help
tap-indeedsponsoredjobs --config CONFIG --discover > ./catalog.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tap_indeedsponsoredjobs/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-indeedsponsoredjobs` CLI interface directly using `poetry run`:

```bash
poetry run tap-indeedsponsoredjobs --help
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

<!--
Developer TODO:
Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any "TODO" items listed in
the file.
-->

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-indeedsponsoredjobs
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-indeedsponsoredjobs --version
# OR run a test `elt` pipeline:
meltano elt tap-indeedsponsoredjobs target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.

Built with the [Meltano SDK](https://sdk.meltano.com) for Singer Taps and Targets.
