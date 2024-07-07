"""
Script: Combine embeddings of research topics and their top N articles

This script reads through the CERIF research topics and their top N articles and combines the embeddings of the research topics and their top N articles.

"""

# -------------------- IMPORT LIBRARIES --------------------

import os
import sys

from box import Box
from google.cloud import bigquery

# Add the root directory of the project to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from util.common.helpers import iterative_offload_to_bigquery, set_logger
from util.embedding.helpers import get_model_and_tokenizer, split_list_to_batch
from util.embedding.research_topic import embed_research_topic_top_n_articles_batch


# -------------------- GLOBAL VARIABLES --------------------
# The path to the configuration file
PATH_TO_CONFIG_FILE = 'config.yml'

# -------------------- MAIN SCRIPT --------------------
if __name__ == '__main__':
    # Set logger
    set_logger()
    # Load the configuration file
    config = Box.from_yaml(filename=PATH_TO_CONFIG_FILE)

    # Create a BigQuery client
    bq_client = bigquery.Client(project=config.GCP.PROJECT_ID)

    # Set model and tokenizer used to embed the text
    model, tokenizer = get_model_and_tokenizer(model_name=config.TEXT_EMBEDDING.MODEL_NAME)

    # --------------- Table: TEXT_EMBEDDING_CERIF_RESEARCH_TOPIC ---------------
    source_table_id = f"{config.GCP.PROJECT_ID}.{config.GCP.READ_SCHEMA}.{config.EMBEDDING.RESEARCH_TOPIC_TOP_N_ARTICLES.SOURCE_TABLE_NAME}"
    target_table_id = f"{config.GCP.PROJECT_ID}.{config.GCP.ANALYTICS_SCHEMA}.{config.EMBEDDING.RESEARCH_TOPIC_TOP_N_ARTICLES.TARGET_TABLE_NAME}"

    # Full target table ID
    # Extract the texts to embed
    df = bq_client.query(
        f"SELECT * FROM {source_table_id}").result().to_dataframe()

    # Split into batches
    batches = split_list_to_batch(lst=df,
                                  batch_size=config.EMBEDDING.RESEARCH_TOPIC_TOP_N_ARTICLES.BATCH_SIZE)

    # Configure the load job to replace data on an existing table
    data_schema = [
        bigquery.SchemaField("RESEARCH_TOPIC_CODE", "STRING"),
        bigquery.SchemaField("ARTICLE_DOI", "STRING"),
        bigquery.SchemaField("EMBEDDING_TENSOR_SHAPE", "INT64", mode="REPEATED"),
        bigquery.SchemaField("EMBEDDING_TENSOR_DATA", "FLOAT64", mode="REPEATED")
    ]

    # Initialize the metadata
    metadata = {
        'model': model,
        'tokenizer': tokenizer
    }

    # Process the embeddings
    iterative_offload_to_bigquery(
        iterable=batches,
        function_process_single=embed_research_topic_top_n_articles_batch,
        table_id=target_table_id,
        client=bq_client,
        max_records=config.EMBEDDING.RESEARCH_TOPIC_TOP_N_ARTICLES.N_MAX_RECORDS,
        max_iterations_to_offload=config.EMBEDDING.RESEARCH_TOPIC_TOP_N_ARTICLES.N_MAX_ITERATIONS_TO_OFFLOAD,
        metadata=metadata,
        data_schema=data_schema
    )
