"""
Script: Derive new collaborations

This script reads through the published articles and defines the ones that are new collaborations based on the authors'
affiliations and historic collaborations.
It also calculates the Novelty Collaboration Index (NCI) for each article.
New collaboration is defined on author level and institution level, whereas NCI is calculated on publication level.

This script is designed to be run incrementally and logs the calculation details to a separate table in BigQuery besides
the table where final results are stored.

"""
import os
import sys

from box import Box
from google.cloud import bigquery
from google.cloud import storage
from loguru import logger
from tqdm import tqdm

# Add the root directory of the project to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from util.collaboration_novelty.graph import fetch_collaboration_graph, save_graphs
from util.collaboration_novelty.process import process_article_collaboration_novelty
from util.collaboration_novelty.query import query_collaboration_batch, query_collaboration_n_batches
from util.common.helpers import offload_batch_to_bigquery, set_logger

# -------------------- IMPORT LIBRARIES --------------------

# -------------------- GLOBAL VARIABLES --------------------
PATH_TO_CONFIG_FILE = 'config.yml'

# -------------------- MAIN SCRIPT --------------------

if __name__ == '__main__':
    # Set logger 
    set_logger()
    # Load the configuration file
    config = Box.from_yaml(filename=PATH_TO_CONFIG_FILE)

    # Full table IDs
    source_table_id = f"{config.GCP.PROJECT_ID}.{config.GCP.READ_SCHEMA}.{config.ANALYTICS.COLLABORATION_NOVELTY.SOURCE_TABLE_NAME}"
    target_table_id_collaboration_novelty_metadata = f"{config.GCP.PROJECT_ID}.{config.GCP.ANALYTICS_SCHEMA}.{config.ANALYTICS.COLLABORATION_NOVELTY.TARGET_TABLE_NAME_COLLABORATION_NOVELTY_METADATA}"
    target_table_id_collaboration_novelty_index = f"{config.GCP.PROJECT_ID}.{config.GCP.ANALYTICS_SCHEMA}.{config.ANALYTICS.COLLABORATION_NOVELTY.TARGET_TABLE_NAME_COLLABORATION_NOVELTY_INDEX}"

    # Create a BigQuery client and a Google Cloud Storage client
    bq_client = bigquery.Client(project=config.GCP.PROJECT_ID)
    storage_client = storage.Client(project=config.GCP.PROJECT_ID)
    bucket = storage_client.get_bucket(bucket_or_name=config.GCP.BUCKET_NAME)

    logger.info("Fetching number of batches...")
    N = query_collaboration_n_batches(bq_client=bq_client,
                                      source_table_id=source_table_id,
                                      target_table_id=target_table_id_collaboration_novelty_index,
                                      batch_size=config.ANALYTICS.COLLABORATION_NOVELTY.BATCH_SIZE,
                                      min_year=config.ANALYTICS.COLLABORATION_NOVELTY.MIN_YEAR)

    # # Get the author and institution collaboration history
    G_a, G_i = fetch_collaboration_graph(
        bucket=bucket,
        graph_a_blob_name=config.ANALYTICS.COLLABORATION_NOVELTY.GRAPH_AUTHOR_COLLABORATION_BLOB_NAME,
        graph_i_blob_name=config.ANALYTICS.COLLABORATION_NOVELTY.GRAPH_INSTITUTION_COLLABORATION_BLOB_NAME
    )

    logger.info("Iterating through batches...")
    # Iterate through all the batches
    for ix in tqdm(range(N)):
        df_batch = query_collaboration_batch(bq_client=bq_client,
                                             source_table_id=source_table_id,
                                             target_table_id=target_table_id_collaboration_novelty_index,
                                             batch_size=config.ANALYTICS.COLLABORATION_NOVELTY.BATCH_SIZE,
                                             min_year=config.ANALYTICS.COLLABORATION_NOVELTY.MIN_YEAR)

        # Initialize the lists to store the collaboration novelty index and the metadata
        cni_rows, metadata_rows = list(), list()

        # Iterate through all the articles
        for iy, article_sid in enumerate(df_batch['ARTICLE_SID'].unique()):
            # Get the article rows
            df_batch_article = df_batch[df_batch['ARTICLE_SID'] == article_sid]
            # Get the authors and institutions pairs for the article
            author_institution_pairs = df_batch_article[['AUTHOR_SID', 'INSTITUTION_SID']].drop_duplicates()

            # Process the article
            cni_row_i, metadata_rows_i = process_article_collaboration_novelty(
                article_sid=article_sid,
                df=df_batch_article,
                G_a=G_a,
                G_i=G_i,
            )

            # Append the collaboration to the lists
            cni_rows.append(cni_row_i)
            metadata_rows.extend(metadata_rows_i)

        # Write the results to BigQuery
        # 1. Collaboration Novelty Index
        offload_batch_to_bigquery(lst_batch=cni_rows,
                                  table_id=target_table_id_collaboration_novelty_index,
                                  client=bq_client,
                                  verbose=False)

        # 2. Collaboration Novelty Metadata
        offload_batch_to_bigquery(lst_batch=metadata_rows,
                                  table_id=target_table_id_collaboration_novelty_metadata,
                                  client=bq_client,
                                  verbose=False)

    # Save the graphs to Google Cloud Storage
    save_graphs(bucket=bucket,
                _G_a=G_a,
                _G_i=G_i,
                graph_i_blob_name=config.ANALYTICS.COLLABORATION_NOVELTY.GRAPH_INSTITUTION_COLLABORATION_BLOB_NAME,
                graph_a_blob_name=config.ANALYTICS.COLLABORATION_NOVELTY.GRAPH_AUTHOR_COLLABORATION_BLOB_NAME)
