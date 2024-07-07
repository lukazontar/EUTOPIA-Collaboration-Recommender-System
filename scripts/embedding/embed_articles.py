"""
Script: Embed articles

This script reads through the articles that are included in the network and embeds the articles using a transformer model.

"""
import os
import sys
from multiprocessing import Pool

from box import Box
from google.cloud import bigquery
from langdetect import detect
from loguru import logger

sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))

from util.common.helpers import process_worker_batch, set_logger
from util.embedding.article import embed_article_batch
from util.embedding.helpers import get_model_and_tokenizer, split_list_to_batch

# -------------------- IMPORT LIBRARIES --------------------

# -------------------- GLOBAL VARIABLES --------------------
PATH_TO_CONFIG_FILE = 'config.yml'

# -------------------- MAIN SCRIPT --------------------
if __name__ == '__main__':
    # Set the logger
    set_logger()
    # Load the configuration file
    config = Box.from_yaml(filename=PATH_TO_CONFIG_FILE)

    # Full table IDs
    source_table_id = f"{config.GCP.PROJECT_ID}.{config.GCP.READ_SCHEMA}.{config.EMBEDDING.ARTICLE.SOURCE_TABLE_NAME}"
    target_table_id = f"{config.GCP.PROJECT_ID}.{config.GCP.ANALYTICS_SCHEMA}.{config.EMBEDDING.ARTICLE.TARGET_TABLE_NAME}"

    # Create a BigQuery client
    bq_client = bigquery.Client(project=config.GCP.PROJECT_ID)

    # Set model and tokenizer used to embed the text
    model, tokenizer = get_model_and_tokenizer(model_name=config.TEXT_EMBEDDING.MODEL_NAME)

    logger.info('Fetching articles to query...')

    # Get the articles that are included in the network
    articles = bq_client.query(f"""
        SELECT * 
        FROM {source_table_id} S
        LEFT JOIN {target_table_id} T
        ON T.DOI = S.ARTICLE_DOI
        WHERE T.DOI IS NULL
        """).result().to_dataframe()

    logger.info('Filtering non-supported languages...')

    # Filter out unsupported language articles
    articles = articles[
        articles.EMBEDDING_INPUT.apply(lambda x: detect(x) in config.EMBEDDING.ARTICLE.SUPPORTED_LANGUAGES)]

    # Split articles into batches
    article_batches = split_list_to_batch(lst=articles,
                                          batch_size=config.EMBEDDING.ARTICLE.ARTICLE_BATCH_SIZE)
    worker_batches = split_list_to_batch(lst=article_batches,
                                         batch_size=config.EMBEDDING.ARTICLE.WORKER_BATCH_SIZE)

    # Create metadata
    metadata = dict(
        tokenizer=tokenizer,
        model=model,
        bq_project_id=config.GCP.PROJECT_ID
    )

    # Define the schema
    data_schema = [
        bigquery.SchemaField("DOI", "STRING"),
        bigquery.SchemaField("EMBEDDING_TENSOR_SHAPE", "INT64", mode="REPEATED"),
        bigquery.SchemaField("EMBEDDING_TENSOR_DATA", "FLOAT64", mode="REPEATED")
    ]

    # Print that the articles are being embedded
    logger.info('Embedding articles...')

    # Process the large batches of article batches in parallel
    with Pool(processes=config.EMBEDDING.ARTICLE.MAX_WORKERS) as pool:
        results = list()
        # Process each worker batch in parallel
        for ix_worker, worker_batch in enumerate(worker_batches):
            params = dict(
                iterable=worker_batch,
                function_process_single=embed_article_batch,
                table_id=target_table_id,
                metadata=metadata,
                max_records=config.EMBEDDING.ARTICLE.N_MAX_RECORDS,
                max_iterations_to_offload=config.EMBEDDING.ARTICLE.N_MAX_ITERATIONS_TO_OFFLOAD,
                data_schema=data_schema
            )
            result = pool.apply_async(process_worker_batch, kwds=params)
            results.append(result)

        # Wait for all processes to finish
        for ix_worker, result in enumerate(results):
            result.get()
            logger.info(f'Finished embedding articles for worker {ix_worker}.')

        # Close the pool (no more tasks can be submitted)
        pool.close()
        # Join the pool (wait for all processes to finish)
        pool.join()
