"""
Script: Derive article topics

This script reads through the articles that are included in the network and derives the top 3 most probable research topics
for each article based on the article embeddings and the research topic embeddings.

"""

# -------------------- IMPORT LIBRARIES --------------------

from box import Box
from google.cloud import bigquery
from loguru import logger

from util.analytics.article_topic import process_article_embedding_batch
from util.common.helpers import set_logger

# -------------------- GLOBAL VARIABLES --------------------
PATH_TO_CONFIG_FILE = 'config.yml'

# -------------------- MAIN SCRIPT --------------------

if __name__ == '__main__':
    # Set logger 
    set_logger()
    # Load the configuration file
    config = Box.from_yaml(filename=PATH_TO_CONFIG_FILE)

    # Full table IDs
    source_table_id_article_embedding = f"{config.GCP.PROJECT_ID}.{config.GCP.ANALYTICS_SCHEMA}.{config.ANALYTICS.ARTICLE_TOPIC.SOURCE_TABLE_NAME_ARTICLE_EMBEDDING}"
    source_table_id_topic_embedding = f"{config.GCP.PROJECT_ID}.{config.GCP.ANALYTICS_SCHEMA}.{config.ANALYTICS.ARTICLE_TOPIC.SOURCE_TABLE_NAME_TOPIC_EMBEDDING}"
    target_table_id = f"{config.GCP.PROJECT_ID}.{config.GCP.ANALYTICS_SCHEMA}.{config.ANALYTICS.ARTICLE_TOPIC.TARGET_TABLE_NAME}"

    # Create a BigQuery client
    bq_client = bigquery.Client(project=config.GCP.PROJECT_ID)

    logger.info("Fetching all research topic embeddings...")

    # Fetch all research topic embeddings
    topic_embeddings = bq_client.query(f"""
        SELECT  RESEARCH_TOPIC_CODE,
                EMBEDDING_TENSOR_SHAPE,
                EMBEDDING_TENSOR_DATA
        FROM `{source_table_id_topic_embedding}`
    """).result().to_dataframe()

    # Extract the research topic embeddings
    topic_embedding_values = topic_embeddings['EMBEDDING_TENSOR_DATA'].values.tolist()

    logger.info("Fetching article embeddings batch...")

    # Fetch all article embeddings that are not yet in the target table
    while bq_client.query(f"""SELECT COUNT(1)
                                FROM `{source_table_id_article_embedding}` A
                                LEFT JOIN `{target_table_id}` T USING (DOI)
                                WHERE T.DOI IS NULL""").result().to_dataframe().values[0][0] > 0:
        # Fetch all article embeddings that are not yet in the target table
        article_embeddings = bq_client.query(f"""
            SELECT  A.DOI,
                    A.EMBEDDING_TENSOR_SHAPE,
                    A.EMBEDDING_TENSOR_DATA
            FROM `{source_table_id_article_embedding}` A
            LEFT JOIN `{target_table_id}` T USING (DOI)
            WHERE T.DOI IS NULL
            LIMIT 10000
        """).result().to_dataframe()

        # Process the article embedding batch
        process_article_embedding_batch(
            article_embeddings=article_embeddings,
            topic_embeddings=topic_embeddings,
            topic_embedding_values=topic_embedding_values,
            bq_client=bq_client,
            target_table_id=target_table_id
        )
