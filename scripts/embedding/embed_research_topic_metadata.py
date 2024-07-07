"""
Script: Embed research topic metadata

This script reads through the CERIF research topics and embeds the research topics using a transformer model.

"""

# -------------------- IMPORT LIBRARIES --------------------

import os
import sys

from box import Box
from google.cloud import bigquery

# Add the root directory of the project to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from util.common.helpers import set_logger
from util.embedding.helpers import get_model_and_tokenizer, split_list_to_batch
from util.embedding.research_topic import embed_research_topic_metadata

# Add the root directory of the project to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

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
    source_table_id = f"{config.GCP.PROJECT_ID}.{config.GCP.READ_SCHEMA}.{config.EMBEDDING.RESEARCH_TOPIC_METADATA.SOURCE_TABLE_NAME}"
    target_table_id = f"{config.GCP.PROJECT_ID}.{config.GCP.ANALYTICS_SCHEMA}.{config.EMBEDDING.RESEARCH_TOPIC_METADATA.TARGET_TABLE_NAME}"

    # Full target table ID
    # Extract the texts to embed
    df = bq_client.query(f"SELECT * FROM {source_table_id}").result().to_dataframe()

    # Split into batches
    research_topics = split_list_to_batch(lst=df,
                                          batch_size=config.EMBEDDING.RESEARCH_TOPIC_METADATA.BATCH_SIZE)

    # Embed the research topics
    df_embeddings = embed_research_topic_metadata(lst_research_topics=research_topics,
                                                  model=model,
                                                  tokenizer=tokenizer)

    # Configure the load job to replace data on an existing table
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        schema=[
            bigquery.SchemaField("RESEARCH_TOPIC_CODE", "STRING"),
            bigquery.SchemaField("EMBEDDING_TENSOR_SHAPE", "INT64", mode="REPEATED"),
            bigquery.SchemaField("EMBEDDING_TENSOR_DATA", "FLOAT64", mode="REPEATED")
        ]
    )

    # Offload the DataFrame to BigQuery, truncating the existing table
    job = bq_client.load_table_from_dataframe(
        dataframe=df_embeddings,
        destination=target_table_id,
        job_config=job_config
    )
