"""
Script: Process Crossref Yearly Dump

This script processes the yearly data dump by Crossref. The data dump includes a folder of .json.gz files that need to
be processed. The script reads the .json.gz files, filters only articles with EUTOPIA-related texts and saves the
metadata in a BigQuery table.
"""

# -------------------- IMPORT LIBRARIES --------------------
import os
import sys

from box import Box
from google.cloud import bigquery

from util.common.helpers import iterative_offload_to_bigquery
from util.academic.crossref import process_json_gz

# Add the root directory of the project to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

# -------------------- GLOBAL VARIABLES --------------------
PATH_TO_CONFIG_FILE = 'config.yml'

# -------------------- MAIN SCRIPT --------------------
if __name__ == '__main__':
    # Load the configuration file
    config = Box.from_yaml(filename=PATH_TO_CONFIG_FILE)

    # Full target table ID
    target_table_id = f"{config.GCP.PROJECT_ID}.{config.GCP.INGESTION_SCHEMA}.{config.HISTORIC.CROSSREF.TARGET_TABLE_NAME}"

    # Create a BigQuery client
    bq_client = bigquery.Client(project=config.GCP.PROJECT_ID)

    # Setup metadata
    metadata = {
        'folder_path': config.HISTORIC.CROSSREF.DATA_FOLDER_PATH
    }

    # Print start message
    print("[INFO] Starting the process of Crossref historic data...")

    # Process Crossref historic data from yearly data dump
    iterative_offload_to_bigquery(
        iterable=os.listdir(config.HISTORIC.CROSSREF.DATA_FOLDER_PATH),
        function_process_single=process_json_gz,
        metadata=metadata,
        table_id=target_table_id,
        max_records=config.HISTORIC.CROSSREF.N_MAX_RECORDS,
        max_iterations_to_offload=config.HISTORIC.CROSSREF.N_MAX_ITERATIONS_TO_OFFLOAD,
        start_iteration=config.HISTORIC.CROSSREF.START_ITERATION,
        client=bq_client
    )

    # Print final message
    print(
        f"[INFO] Finished processing Crossref historic data. All the data was successfully offloaded to {target_table_id}.")
