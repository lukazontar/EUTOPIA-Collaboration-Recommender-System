# ORCID: Update Memberships
## Description
Mage pipeline that takes ORCID members affiliated with EUTOPIA universities that were updated in last few hours and ingests this data to BigQuery table: `ORCID.ORCID_MEMBER`.

## Secrets:
- `ORCID_CLIENT_ID`: ORCID Client ID.
- `ORCID_CLIENT_SECRET`: ORCID Client Secret.

*See here: [ORCID: Registering a Public API client](https://info.orcid.org/documentation/integration-guide/registering-a-public-api-client/)*

## Global Variables
- `bigquery_dataset`: BigQuery dataset name. 
  - Default: `ORCID`.
- `bigquery_project`: BigQuery project name. 
  - Default: `collaboration-recommender`.
- `environment`: Environment name. 
  - Default: `DEV`.
- `n_fetch_rows_orcid`: Number of rows to fetch from ORCID API. 
  - Default: `1000`.
- `n_orcid_api_call_limit`: Number of API calls to ORCID API before pause. 
  - Default: `24`.
- `orcid_access_token`: ORCID Access Token (set automatically).

## Process
1. Fetch ORCID access token using client ID and secret.
2. Get a list of ORCID member IDs associated with EUTOPIA universities and changed within last few hours from ORCID API.
3. For each ORCID member, fetch their details from ORCID API.
4. Ingest the data to BigQuery table: `ORCID.ORCID_MEMBER`.