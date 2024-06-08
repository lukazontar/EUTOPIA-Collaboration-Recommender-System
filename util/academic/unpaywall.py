import json

from requests.exceptions import HTTPError

from util.common.helpers import make_request, MAILTO_EMAIL

BASE_URL = "http://api.unpaywall.org/v2/"


def fetch_doi_metadata(doi: str) -> str:
    """
    Get metadata for a DOI from Unpaywall.
    :param doi: DOI to get metadata for.
    :return: Return the metadata as a dictionary.
    """
    # Get the URL for the request
    request_url = f"{BASE_URL}{doi}?email={MAILTO_EMAIL}"

    try:
        # Make the request
        data = make_request(url=request_url, params={})
    # Except if HTTPError returns 404 status code, otherwise raise the error
    except HTTPError as e:
        if e.response.status_code == 422:
            return dict()
        else:
            raise e

    # Return the metadata
    return data


def process_doi(item: str,
                iteration_settings: dict,
                metadata: dict) -> dict:
    """
    Accepts DOI of the article and returns an updated iteration settings (including Unpaywall JSON).
    :param item:  The DOI of the article
    :param iteration_settings:  The settings for the current iteration including list of records to offload to BigQuery and total number of records processed so far.
    :return: The updated settings for the current iteration.
    """
    record_json = fetch_doi_metadata(doi=item)
    record = dict(
        DOI=item,
        JSON=json.dumps(record_json)
    )

    # Update the settings for the current iteration
    iteration_settings['batch'].append(record)
    iteration_settings['n_iterations'] += 1
    iteration_settings['total_records'] += 1

    # Return the updated settings for the current iteration
    return iteration_settings
