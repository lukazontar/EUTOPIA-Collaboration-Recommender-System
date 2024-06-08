"""
Script: Fetch CERIF Registry

This script processes the CERIF registry HTML file and extracts the research topics from it. The research topics are then
loaded into the BigQuery table CERIF_RESEARCH_TOPIC. The script also embeds the research topics using a transformer model and
saves the embeddings in the BigQuery table TEXT_EMBEDDING_CERIF_RESEARCH_TOPIC. Since the embeddings
"""

# -------------------- IMPORT LIBRARIES --------------------

import os
import sys

from box import Box
from bs4 import BeautifulSoup
from google.cloud import bigquery

from util.academic.cerif import extract_research_topics

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
    target_table_id_research_topic = f"{config.GCP.PROJECT_ID}.{config.GCP.INGESTION_SCHEMA}.{config.HISTORIC.CERIF.TARGET_TABLE_NAME}"

    # Create a BigQuery client
    bq_client = bigquery.Client(project=config.GCP.PROJECT_ID)

    # Print that the process is starting
    print("[INFO] Started processing the CERIF registry...")
    # Read the HTML file
    with open(config.HISTORIC.CERIF.PATH_TO_HTML_FILE, 'r', encoding='windows-1250') as file:
        html_content = file.read()

    # Parse the HTML content
    html_parsed = BeautifulSoup(html_content, 'html.parser')

    # Extract the research topics
    df_cerif = extract_research_topics(html=html_parsed)

    # Configure the load job to replace data on an existing table
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    # Offload the DataFrame to BigQuery, truncating the existing table
    job = bq_client.load_table_from_dataframe(
        dataframe=df_cerif,
        destination=target_table_id_research_topic,
        job_config=job_config
    )

    # Print that the process is finished
    print(f"[INFO] Finished processing the CERIF registry and data is saved to table: {target_table_id_research_topic}")
