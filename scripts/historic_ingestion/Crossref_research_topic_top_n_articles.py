"""
Script: Fetch CERIF topic Crossref articles

This script reads through the CERIF research topics and fetches top N most relevant articles from Crossref that are related to the research topics.
"""

# -------------------- IMPORT LIBRARIES --------------------

import os
import sys
import pandas as pd

from box import Box
from tqdm import tqdm
from google.cloud import bigquery

from util.academic.crossref import query_top_n_by_keyword

# Add the root directory of the project to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# -------------------- GLOBAL VARIABLES --------------------
# The path to the configuration file
PATH_TO_CONFIG_FILE = 'config.yml'

# -------------------- MAIN SCRIPT --------------------
if __name__ == '__main__':
    # Load the configuration file
    config = Box.from_yaml(filename=PATH_TO_CONFIG_FILE)

    # Full target table ID
    source_table_id_research_topic = f"{config.GCP.PROJECT_ID}.{config.GCP.INGESTION_SCHEMA}.{config.HISTORIC.CERIF.TARGET_TABLE_NAME}"
    target_table_id_article = f"{config.GCP.PROJECT_ID}.{config.GCP.INGESTION_SCHEMA}.{config.HISTORIC.CERIF_RESEARCH_TOPIC_TOP_N_ARTICLES.TARGET_TABLE_NAME}"

    # Create a BigQuery client
    bq_client = bigquery.Client(project=config.GCP.PROJECT_ID)

    # --------------- Main functionality ---------------

    # Print that the process is starting
    print("[INFO] Started fetching Crossref articles related to CERIF research topics...")

    # Read data from the source table
    df_cerif = bq_client.query(f"SELECT * FROM {source_table_id_research_topic}").result().to_dataframe()

    # Initialize list to store articles related to research topics
    lst_articles = list()
    for research_topic in tqdm(df_cerif.iterrows()):
        research_topic = research_topic[1]
        # Query the top N DOIs by keyword concatenated to a string to be input into the text embedding model
        lst_articles_keywords = query_top_n_by_keyword(keyword=research_topic['RESEARCH_TOPIC_NAME'],
                                                       n=config.HISTORIC.CERIF_RESEARCH_TOPIC_TOP_N_ARTICLES.N_ARTICLES)

        # Add keyword and research topic code to the list
        lst_articles_keywords_with_cerif = [
            {
                "CERIF_RESEARCH_TOPIC_CODE": research_topic['RESEARCH_TOPIC_CODE'],
                "DOI": article['DOI'],
                "JSON": article['JSON']
            } for article in lst_articles_keywords
        ]
        # Append the articles to the list
        lst_articles.extend(lst_articles_keywords_with_cerif)

    # Create a DataFrame from the list of articles
    df_articles = pd.DataFrame(lst_articles)

    # Configure the load job to replace data on an existing table
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    # Offload the DataFrame to BigQuery, truncating the existing table
    job = bq_client.load_table_from_dataframe(
        dataframe=df_articles,
        destination=target_table_id_article,
        job_config=job_config
    )

    # Print that the process is finished
    print(
        f"[INFO] Finished fetching Crossref articles related to CERIF research topics and data is saved to table: {target_table_id_article}")
