"""
Script: Embed research topic metadata

This script reads through the CERIF research topics and embeds the research topics using a transformer model.

"""

# -------------------- IMPORT LIBRARIES --------------------

import os
import sys

from box import Box
from google.cloud import bigquery
from loguru import logger
from tqdm import tqdm

# Add the root directory of the project to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from util.common.helpers import offload_batch_to_bigquery, set_logger
from util.embedding.helpers import get_model_and_tokenizer
from util.embedding.research_topic import combine_research_topic_embeddings

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
    source_table_id_research_topic_metadata = f"{config.GCP.PROJECT_ID}.{config.GCP.ANALYTICS_SCHEMA}.{config.EMBEDDING.RESEARCH_TOPIC_METADATA.TARGET_TABLE_NAME}"
    source_table_id_research_topic_top_n_articles = f"{config.GCP.PROJECT_ID}.{config.GCP.ANALYTICS_SCHEMA}.{config.EMBEDDING.RESEARCH_TOPIC_TOP_N_ARTICLES.TARGET_TABLE_NAME}"
    target_table_id = f"{config.GCP.PROJECT_ID}.{config.GCP.ANALYTICS_SCHEMA}.{config.EMBEDDING.RESEARCH_TOPIC_COMBINED.TARGET_TABLE_NAME}"

    # Print that we start extracting the data
    logger.info("Extracting the texts to embed...")

    # Extract the texts to embed
    df_research_topic_metadata = bq_client.query(
        f"SELECT * FROM {source_table_id_research_topic_metadata}").result().to_dataframe()
    df_research_topic_top_n_articles = bq_client.query(
        f"SELECT * FROM {source_table_id_research_topic_top_n_articles}").result().to_dataframe()

    # Print that we start combining embeddings
    logger.info("Combining the embeddings...")

    # Initialize the list of combined embeddings
    lst_embeddings_combined = list()
    # Go through research topic codes and for each average the embeddings
    for research_topic_code in tqdm(df_research_topic_metadata['RESEARCH_TOPIC_CODE'].unique()):
        # Generate the combined embedding
        embedding_combined = combine_research_topic_embeddings(research_topic_code=research_topic_code,
                                                               df_research_topic_metadata=df_research_topic_metadata,
                                                               df_research_topic_top_n_articles=df_research_topic_top_n_articles)

        # Append the combined embedding to the list
        lst_embeddings_combined.append(
            {
                'RESEARCH_TOPIC_CODE': research_topic_code,
                'EMBEDDING_TENSOR_SHAPE': list(embedding_combined.shape),
                'EMBEDDING_TENSOR_DATA': embedding_combined.flatten().tolist()
            }
        )

    # Configure the load job to replace data on an existing table
    data_schema = [
        bigquery.SchemaField("RESEARCH_TOPIC_CODE", "STRING"),
        bigquery.SchemaField("EMBEDDING_TENSOR_SHAPE", "INT64", mode="REPEATED"),
        bigquery.SchemaField("EMBEDDING_TENSOR_DATA", "FLOAT64", mode="REPEATED")
    ]

    # Offload the DataFrame to BigQuery, truncating the existing table
    offload_batch_to_bigquery(client=bq_client,
                              lst_batch=lst_embeddings_combined,
                              table_id=target_table_id,
                              data_schema=data_schema)

    # Print that the embeddings have been combined and stored to target table
    logger.info(f"Combined embeddings have been stored to table {target_table_id}.")
