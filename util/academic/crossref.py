import gzip
import json
import os

from requests import HTTPError
from util.academic.eutopia import is_eutopia_affiliated_string
from util.common.helpers import make_request, MAILTO_EMAIL

BASE_URL = "https://api.crossref.org/works/"
PARAMS = {"mailto": MAILTO_EMAIL}


def is_crossref_doi(doi: str) -> bool:
    """
    Check if the DOI is a CrossRef DOI.
    :param doi:  DOI to check
    :return: Return True if the DOI is a CrossRef DOI, False otherwise.
    """
    try:
        # Get the agency ID for the DOI
        agency_url = f"{BASE_URL}{doi}/agency"
        agency_data = make_request(agency_url, PARAMS)
        agency_id = agency_data.get("message", {}).get("agency", {}).get("id")

        # Check if the agency ID is CrossRef
        return agency_id == 'crossref'
    # Except if HTTPError returns 404 status code, otherwise raise the error
    except HTTPError as e:
        if e.response.status_code == 404:
            return dict()
        else:
            raise e


def fetch_doi_metadata(doi: str) -> dict:
    """
    Get metadata for a DOI from CrossRef.
    :param doi: DOI to get metadata for.
    :return: Return the metadata as a dictionary.
    """
    if not is_crossref_doi(doi):
        return dict()

    try:
        metadata_url = f"{BASE_URL}{doi}"
        metadata_data = make_request(metadata_url, PARAMS)
        return metadata_data.get("message")
    # Except if HTTPError returns 404 status code, otherwise raise the error
    except HTTPError as e:
        if e.response.status_code == 404:
            return dict()
        else:
            raise e


def prcess_json_gz_items(items: list) -> list:
    """
    Process a list of items from a .json.gz file, searching for university names in the files and filtering the items that contain university names.
    :param items: The list of items to process.
    :return: The list of items that contain university names.
    """
    filtered_items = list()
    for item in items:
        item_string = json.dumps(item)
        if is_eutopia_affiliated_string(string=item_string):
            filtered_items.append(
                dict(
                    DOI=item['DOI'],
                    JSON=item_string
                )
            )
    return filtered_items


def process_json_gz(item: str,
                    iteration_settings: dict,
                    metadata: dict) -> dict:
    """
    Processes a single item from a .json.gz file from the Crossref historic data dump, searching for university names in the files and offloading the results to BigQuery.
    :param item: The item to process.
    :param metadata: The metadata for the current iteration including the folder path.
    :param iteration_settings:  The settings for the current iteration including list of records to offload to BigQuery and total number of records processed so far..
    :return: The updated settings for the current iteration.
    """
    if item.endswith('.json.gz'):
        filepath = os.path.join(metadata['folder_path'], item)
        with gzip.open(filepath, 'rb') as f:
            # Load the JSON content and extract the 'items' attribute
            items = json.loads(f.read()).get('items', [])
            # Check if the file content contains any of the university names
            new_batch = prcess_json_gz_items(items=items)

        # Update the settings for the current iteration
        iteration_settings['batch'].extend(new_batch)
        iteration_settings['n_iterations'] += 1
        iteration_settings['total_records'] += len(new_batch)

    # Return the updated settings for the current iteration
    return iteration_settings


def query_top_n_by_keyword(keyword: str,
                           n: int) -> list:
    """
    Query the top N DOIs by keyword concatenated to a string to be input into the text embedding model.
    :param keyword: Keyword to search for.
    :param n: Number of DOIs to return.
    :return: List of DOIs.
    """

    # Query the top N by keyword, sorted by relevance, only select title and abstract
    url = f"https://api.crossref.org/works?query={keyword}&sort=relevance&rows={n}&filter=has-abstract:true"
    response = make_request(url, PARAMS)
    return [dict(
        DOI=article['DOI'],
        JSON=json.dumps(article)
    ) for article in response.get("message")['items']][:n]


def process_reference_article(item: str,
                              iteration_settings: dict,
                              metadata: dict) -> dict:
    """
    Processes a single item from the list of reference articles, fetching the metadata from Crossref and offloading the results to BigQuery.
    :param item: The item to process.
    :param metadata: The metadata for the current iteration including the folder path.
    :param iteration_settings:  The settings for the current iteration including list of records to offload to BigQuery and total number of records processed so far.
    :return: The updated settings for the current iteration.
    """

    reference_article_doi = item['REFERENCE_ARTICLE_DOI']

    # Extract DOI from potential URL
    if reference_article_doi.startswith('http'):
        reference_article_doi = reference_article_doi.split('/')[-1]
        # Remove any trailing paramters like: ?download=true
    if '?' in reference_article_doi:
        reference_article_doi = reference_article_doi.split('?')[0]

    data = fetch_doi_metadata(doi=reference_article_doi)

    # Update the settings for the current iteration
    iteration_settings['batch'].append(
        dict(
            DOI=reference_article_doi,
            JSON=json.dumps(data)
        )
    )
    iteration_settings['n_iterations'] += 1
    iteration_settings['total_records'] += 1

    # Return the updated settings for the current iteration
    return iteration_settings
