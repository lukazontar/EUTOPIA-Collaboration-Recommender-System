"""
Script: Fetch Crossref reference articles

This script processes the DOIs of the reference articles of the articles that are included in the network and fetches the metadata from Crossref.
We assume that the reference articles are an important data point for article embeddings and similarity calculations.
"""

# -------------------- IMPORT LIBRARIES --------------------
from box import Box
from google.cloud import bigquery
from loguru import logger

from util.academic.crossref import process_reference_article
from util.common.helpers import iterative_offload_to_bigquery, set_logger
from util.common.query import get_datalake_reference_articles

# -------------------- GLOBAL VARIABLES --------------------
PATH_TO_CONFIG_FILE = 'config.yml'

# -------------------- MAIN SCRIPT --------------------
if __name__ == '__main__':
    # Set logger 
    set_logger()
    # Load the configuration file
    config = Box.from_yaml(filename=PATH_TO_CONFIG_FILE)

    # Full target table ID
    target_table_id = f"{config.GCP.PROJECT_ID}.{config.GCP.INGESTION_SCHEMA}.{config.HISTORIC.CROSSREF_REFERENCE_ARTICLES.TARGET_TABLE_NAME}"

    # Create a BigQuery client
    bq_client = bigquery.Client(project=config.GCP.PROJECT_ID)

    # Print that the process has started
    logger.info("Fetching Crossref reference article metadata for EUTOPIA articles...")

    # Get the articles that are included in the network
    articles = get_datalake_reference_articles(client=bq_client,
                                               schema=config.GCP.INGESTION_SCHEMA)

    # Get the articles that are already loaded in the Crossref reference article metadata table
    loaded_articles = bq_client.query(f'SELECT DISTINCT DOI FROM {target_table_id}').result().to_dataframe()

    # Filter the articles that are not already loaded in the Crossref reference article metadata table
    articles = articles[~articles.REFERENCE_ARTICLE_DOI.isin(loaded_articles.DOI)]

    # Print that missing articles have been selected
    logger.info("Missing articles have been selected.")

    # Process the articles
    iterative_offload_to_bigquery(
        iterable=articles.to_dict('records'),
        function_process_single=process_reference_article,
        table_id=target_table_id,
        client=bq_client,
        max_records=config.HISTORIC.CROSSREF_REFERENCE_ARTICLES.N_MAX_RECORDS,
        max_iterations_to_offload=config.HISTORIC.CROSSREF_REFERENCE_ARTICLES.N_MAX_ITERATIONS_TO_OFFLOAD
    )

    # Print that the process has finished
    logger.info(
        f"Crossref reference article metadata for EUTOPIA articles fetched and offloaded to table {target_table_id}.")
