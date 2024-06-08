import json
import tarfile
import time

import requests
import xmltodict

from util.academic.eutopia import is_eutopia_affiliated_string


def process_tarfile_file(item: tarfile.TarInfo,
                         metadata: dict,
                         iteration_settings: dict) -> dict:
    """
    Processes a member of a .tar.gz file, searching for university names in the files and offloading the results to BigQuery.
    :param item: The member of the .tar.gz file to process.
    :param metadata: The metadata for the current iteration including data about the tar file.
    :param iteration_settings:  The settings for the current iteration including list of records to offload to BigQuery and total number of records processed so far..
    :return: The updated settings for the current iteration.
    """
    # Check if the member is a file
    if item.isfile():
        # Extract the file content
        file_content = metadata['tar'].extractfile(item).read().decode('utf-8')

        # Check if the file content contains any of the university names
        if is_eutopia_affiliated_string(string=file_content):
            record_json = json.dumps(xmltodict.parse(file_content))
            # Extract XML
            record = dict(
                FILEPATH=item.path,
                JSON=record_json
            )

            # Update the settings for the current iteration
            iteration_settings['batch'].append(record)
            iteration_settings['n_iterations'] += 1
            iteration_settings['total_records'] += 1

    # Return the updated settings for the current iteration
    return iteration_settings


def process_orcid_id(item: str,
                     metadata: dict,
                     iteration_settings: dict) -> dict:
    """
    Fetches ORCID summary of a member via the ORCID API.
    :param item: The member ORCID ID to process.
    :param metadata: The metadata for the current iteration including
    :param iteration_settings:  The settings for the current iteration including list of records to offload to BigQuery and total number of records processed so far..
    :return: The updated settings for the current iteration.
    """
    if item is None or item == '':
        return iteration_settings

    # Fetch the ORCID summary
    access_token = metadata['orcid_access_token']

    # Define headers
    headers_record: dict = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/json'
    }

    # Get the ORCID record
    response = requests.get(url=f'https://pub.orcid.org/v3.0/{item}/record',
                            headers=headers_record)

    # Pop the first element from the queue (if queue is full)
    if len(metadata['req_limit_queue']) >= 24:
        # Delete the first element
        metadata['req_limit_queue'].pop(0)

    # Put the current time in the queue
    metadata['req_limit_queue'].append(time.time())

    # Define the record
    record = dict(
        ORCID_ID=item,
        JSON=json.dumps(response.json())
    )

    # Check if the number of requests is at the limit and check if the first request that went into queue happened
    # less than a second ago
    if (len(metadata['req_limit_queue']) >= 24
            and metadata['req_limit_queue'][0] > metadata['req_limit_queue'][-1] - 1):
        # Difference between the last and first request
        diff = metadata['req_limit_queue'][-1] - metadata['req_limit_queue'][0]
        # Sleep for the difference to avoid exceeding the limit
        time.sleep(1 - diff)

    # Update the settings for the current iteration
    iteration_settings['batch'].append(record)
    iteration_settings['n_iterations'] += 1
    iteration_settings['total_records'] += 1

    # Return the updated settings for the current iteration
    return iteration_settings


def fetch_access_token(client_id: str,
                       client_secret: str) -> str:
    """
    Get access token from ORCID API and save it to global variables.
    :return: access token
    """
    # Define headers
    headers_token: dict = {
        "Accept": "application/json"
    }

    # Define URL
    url_token: str = "https://orcid.org/oauth/token"

    # Define data
    data_token: dict = {
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': 'client_credentials',
        'scope': '/read-public'
    }

    # POST request
    response_token: requests.Response = requests.post(url=url_token,
                                                      headers=headers_token,
                                                      data=data_token)
    # Get access from response
    access_token = response_token.json().get('access_token')

    # Return the access token
    return access_token
