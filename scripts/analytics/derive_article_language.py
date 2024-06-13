"""
Script: Derive new collaborations

This script reads through the published articles and defines the ones that are new collaborations based on the authors' affiliations and historic collaborations.
It also calculates the Novelty Collaboration Index (NCI) for each article.

"""

# -------------------- IMPORT LIBRARIES --------------------

from box import Box
from google.cloud import bigquery

from util.analytics.article_language import process_article_language
from util.common.helpers import iterative_offload_to_bigquery

# -------------------- GLOBAL VARIABLES --------------------
PATH_TO_CONFIG_FILE = 'config.yml'

# -------------------- MAIN SCRIPT --------------------

if __name__ == '__main__':
    # Load the configuration file
    config = Box.from_yaml(filename=PATH_TO_CONFIG_FILE)

    # Full table IDs
    source_table_id = f"{config.GCP.PROJECT_ID}.{config.GCP.READ_SCHEMA}.{config.ANALYTICS.ARTICLE_LANGUAGE.SOURCE_TABLE_NAME}"
    target_table_id = f"{config.GCP.PROJECT_ID}.{config.GCP.ANALYTICS_SCHEMA}.{config.ANALYTICS.ARTICLE_LANGUAGE.TARGET_TABLE_NAME}"

    # Create a BigQuery client
    bq_client = bigquery.Client(project=config.GCP.PROJECT_ID)

    print("[INFO] Fetching all relevant articles to query...")

    # Query all the articles
    articles = bq_client.query(f"""
    SELECT * 
    FROM {source_table_id} S
    LEFT JOIN {target_table_id} T USING(ARTICLE_DOI)
    WHERE T.ARTICLE_DOI IS NULL
    """).result().to_dataframe()

    print("[INFO] Fetching article languages...")

    # Process the articles
    iterative_offload_to_bigquery(
        iterable=articles.to_dict('records'),
        function_process_single=process_article_language,
        table_id=target_table_id,
        client=bq_client,
        max_records=config.ANALYTICS.ARTICLE_LANGUAGE.N_MAX_RECORDS,
        max_iterations_to_offload=config.ANALYTICS.ARTICLE_LANGUAGE.N_MAX_ITERATIONS_TO_OFFLOAD
    )
