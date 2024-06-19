"""
Script: Fetch ORCID Historic API Metadata

This script processes the ORCID IDs of the authors that are included in the network and fetches the metadata from ORCID.
The metadata is then offloaded to BigQuery for further analysis. Main point for ingesting this data is to get better data on author employment and affiliations.
"""

# -------------------- IMPORT LIBRARIES --------------------

import os
import sys

import pandas as pd
from box import Box
from google.cloud import bigquery
from loguru import logger

from util.academic.orcid import process_orcid_id, fetch_access_token
from util.common.helpers import iterative_offload_to_bigquery, set_logger
from util.common.query import get_datalake_authors

# Add the root directory of the project to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# -------------------- GLOBAL VARIABLES --------------------
# The path to the configuration file
PATH_TO_CONFIG_FILE = 'config.yml'


def exclude_loaded_authors(_df: pd.DataFrame,
                           client: bigquery.Client,
                           table_id_tarfile: str,
                           table_id_api: str) -> pd.DataFrame:
    """
    This function filters out the authors that are already loaded in the ORCID API and tarfile author tables.
    :param _df: DataFrame containing the authors
    :param client: BigQuery client
    :param table_id_tarfile: Table ID of the ORCID tarfile author table
    :param table_id_api: Table ID of the ORCID API author table
    :return: DataFrame containing the authors that are not already loaded in the ORCID API and tarfile author tables
    """
    # Copy the dataframe to avoid modifying the original
    df_authors = _df.copy()

    # ---------- EXCLUDE ALREADY LOADED AUTHORS VIA API ----------
    # Get the authors that are already loaded in the ORCID API author table
    loaded_authors_in_api_offload = client.query(
        f'SELECT DISTINCT ORCID_ID FROM {table_id_api}').result().to_dataframe()

    # Filter the authors that are not already loaded in the ORCID API author table
    df_authors = df_authors[~df_authors.AUTHOR_ORCID_ID.isin(loaded_authors_in_api_offload.ORCID_ID)]

    # ---------- EXCLUDE ALREADY LOADED AUTHORS VIA TARFILE ----------
    # Get the authors that are already loaded in the ORCID tarfile author table
    loaded_authors_in_tarfile_offload = client.query(
        f'SELECT DISTINCT FILEPATH FROM {table_id_tarfile}').result().to_dataframe()
    # Extract ORCID ID from the file path
    loaded_authors_in_tarfile_offload['ORCID_ID'] = loaded_authors_in_tarfile_offload.FILEPATH.str.extract(
        r'(\d{4}-\d{4}-\d{4}-\d{4})')
    # Filter the authors that are not already loaded in the ORCID tarfile author table
    df_authors = df_authors[~df_authors.AUTHOR_ORCID_ID.isin(loaded_authors_in_tarfile_offload.ORCID_ID)]

    # Return the filtered authors
    return df_authors


# -------------------- MAIN SCRIPT --------------------
if __name__ == '__main__':
    # Set logger 
    set_logger()
    # Load the configuration files
    config = Box.from_yaml(filename=PATH_TO_CONFIG_FILE)
    secrets = Box.from_yaml(filename=config.PATH_TO_SECRETS_FILE)

    # Full target table ID
    api_offload_table_id = f"{config.GCP.PROJECT_ID}.{config.GCP.INGESTION_SCHEMA}.{config.HISTORIC.ORCID_API.TARGET_TABLE_NAME}"
    tarfile_offload_table_id = f"{config.GCP.PROJECT_ID}.{config.GCP.INGESTION_SCHEMA}.{config.HISTORIC.ORCID_TARFILE.TARGET_TABLE_NAME}"

    # Create a BigQuery client
    bq_client = bigquery.Client(project=config.GCP.PROJECT_ID)

    # Print that the authors are fetched
    logger.info("Fetching missing authors...")

    # Get the authors that are included in the network
    authors = get_datalake_authors(client=bq_client,
                                   schema=config.GCP.INGESTION_SCHEMA)

    # Filter out authors without ORCID IDs or where ORCID ID is ''
    authors = authors[authors.AUTHOR_ORCID_ID.notnull() & (authors.AUTHOR_ORCID_ID != '')]
    # Extract ORCID ID from the ORCID URL
    authors['AUTHOR_ORCID_ID'] = authors.AUTHOR_ORCID_ID.apply(lambda item: item.split('/')[-1] if item else None)

    # Filter out authors that are already loaded in the ORCID API and tarfile author tables
    authors = exclude_loaded_authors(_df=authors,
                                     client=bq_client,
                                     table_id_tarfile=tarfile_offload_table_id,
                                     table_id_api=api_offload_table_id)

    metadata = dict(
        orcid_access_token=fetch_access_token(client_id=secrets.ORCID.CLIENT_ID,
                                              client_secret=secrets.ORCID.CLIENT_SECRET),
        req_limit_queue=list()  # Queue to limit the number of requests per second
    )

    # Print that the authors are fetched and that the missing authors are being processed
    logger.info("Missing authors selected, processing...")

    # Process the authors
    iterative_offload_to_bigquery(
        iterable=authors.AUTHOR_ORCID_ID,
        function_process_single=process_orcid_id,
        table_id=api_offload_table_id,
        client=bq_client,
        max_records=config.HISTORIC.ORCID_API.N_MAX_RECORDS,
        max_iterations_to_offload=config.HISTORIC.ORCID_API.N_MAX_ITERATIONS_TO_OFFLOAD,
        metadata=metadata
    )

    # Print final message
    logger.info(
        f"Finished processing ORCID API historic data. All the data was successfully offloaded to {api_offload_table_id}.")
