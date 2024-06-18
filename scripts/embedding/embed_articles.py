"""
Script: Embed articles

This script reads through the articles that are included in the network and embeds the articles using a transformer model.

"""

# -------------------- IMPORT LIBRARIES --------------------

from box import Box
from google.cloud import bigquery
from langdetect import detect

from util.common.helpers import iterative_offload_to_bigquery
from util.embedding.article import embed_article_batch
from util.embedding.helpers import get_model_and_tokenizer, split_list_to_batch

# -------------------- GLOBAL VARIABLES --------------------
PATH_TO_CONFIG_FILE = 'config.yml'

# -------------------- MAIN SCRIPT --------------------
if __name__ == '__main__':
    # Load the configuration file
    config = Box.from_yaml(filename=PATH_TO_CONFIG_FILE)

    # Full table IDs
    source_table_id = f"{config.GCP.PROJECT_ID}.{config.GCP.READ_SCHEMA}.{config.EMBEDDING.ARTICLE.SOURCE_TABLE_NAME}"
    target_table_id = f"{config.GCP.PROJECT_ID}.{config.GCP.ANALYTICS_SCHEMA}.{config.EMBEDDING.ARTICLE.TARGET_TABLE_NAME}"

    # Create a BigQuery client
    bq_client = bigquery.Client(project=config.GCP.PROJECT_ID)

    # Set model and tokenizer used to embed the text
    model, tokenizer = get_model_and_tokenizer(model_name=config.TEXT_EMBEDDING.MODEL_NAME)

    print("[INFO] Fetching articles to query...")

    # Get the articles that are included in the network
    articles = bq_client.query(f"""
        SELECT * 
        FROM {source_table_id} S
        LEFT JOIN {target_table_id} T
        ON T.DOI = S.ARTICLE_DOI
        WHERE T.DOI IS NULL
        LIMIT 30000
        """).result().to_dataframe()

    print("[INFO] Filtering non-supported languages...")

    # Filter out unsupported language articles
    articles = articles[
        articles.EMBEDDING_INPUT.apply(lambda x: detect(x) in config.EMBEDDING.ARTICLE.SUPPORTED_LANGUAGES)]

    # Split articles into batches
    articles = split_list_to_batch(lst=articles,
                                   batch_size=config.EMBEDDING.ARTICLE.BATCH_SIZE)

    # Create metadata
    metadata = dict(
        tokenizer=tokenizer,
        model=model
    )

    # Print that the articles are being embedded
    print("[INFO] Embedding articles...")

    # Define the schema
    data_schema = [
        bigquery.SchemaField("DOI", "STRING"),
        bigquery.SchemaField("EMBEDDING_TENSOR_SHAPE", "INT64", mode="REPEATED"),
        bigquery.SchemaField("EMBEDDING_TENSOR_DATA", "FLOAT64", mode="REPEATED")
    ]

    # Process the articles
    iterative_offload_to_bigquery(
        iterable=articles,
        function_process_single=embed_article_batch,
        table_id=target_table_id,
        client=bq_client,
        max_records=config.EMBEDDING.ARTICLE.N_MAX_RECORDS,
        max_iterations_to_offload=config.EMBEDDING.ARTICLE.N_MAX_ITERATIONS_TO_OFFLOAD,
        metadata=metadata,
        data_schema=data_schema
    )

    # Print that the embedding is done and that the articles were stored in the target table
    print(f"[INFO] Embedding done. Articles stored in table {target_table_id}.")
