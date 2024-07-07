"""
Script: Fetch Unpaywall DOI Metadata

This script processes the DOIs of the articles that are included in the network and fetches the metadata from Unpaywall.
The metadata is then offloaded to BigQuery for further analysis. Main point for ingesting this data is to get links to full-text PDFs.
"""

# -------------------- IMPORT LIBRARIES --------------------

import os
import sys

from box import Box
from google.cloud import bigquery
from loguru import logger

# Add the root directory of the project to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from util.academic.unpaywall import process_doi
from util.common.helpers import iterative_offload_to_bigquery, set_logger
from util.common.query import get_datalake_articles

# -------------------- GLOBAL VARIABLES --------------------
# The path to the configuration file
PATH_TO_CONFIG_FILE = 'config.yml'

# -------------------- MAIN SCRIPT --------------------
if __name__ == '__main__':
    # Set logger 
    set_logger()
    # Load the configuration file
    config = Box.from_yaml(filename=PATH_TO_CONFIG_FILE)

    # Full target table ID
    target_table_id = f"{config.GCP.PROJECT_ID}.{config.GCP.INGESTION_SCHEMA}.{config.HISTORIC.UNPAYWALL.TARGET_TABLE_NAME}"

    # Create a BigQuery client
    bq_client = bigquery.Client(project=config.GCP.PROJECT_ID)

    # Print that the process has started
    logger.info("Fetching Unpaywall metadata for EUTOPIA articles...")

    # Get the articles that are included in the network
    articles = get_datalake_articles(client=bq_client,
                                     schema=config.GCP.INGESTION_SCHEMA)

    # Get the articles that are already loaded in the Unpaywall metadata table
    loaded_articles = bq_client.query(f'SELECT DISTINCT DOI FROM {target_table_id}').result().to_dataframe()

    # Filter the articles that are not already loaded in the Unpaywall metadata table
    articles = articles[~articles.ARTICLE_DOI.isin(loaded_articles.DOI)]

    # Print that missing articles have been selected
    logger.info("Missing articles have been selected.")

    # Process the articles
    iterative_offload_to_bigquery(
        iterable=articles.ARTICLE_DOI,
        function_process_single=process_doi,
        table_id=target_table_id,
        client=bq_client,
        max_records=config.HISTORIC.UNPAYWALL.N_MAX_RECORDS,
        max_iterations_to_offload=config.HISTORIC.UNPAYWALL.N_MAX_ITERATIONS_TO_OFFLOAD
    )

    # Print that the process has finished
    logger.info(f"Unpaywall metadata for EUTOPIA articles fetched and offloaded to table {target_table_id}.")
