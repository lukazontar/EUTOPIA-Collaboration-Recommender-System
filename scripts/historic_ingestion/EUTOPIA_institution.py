"""
Script: Fetch EUTOPIA Institution Metadata

This script takes data from the EUTOPIA_INSTITUTION_REGISTRY and offloads it to BigQuery for further analysis along with
some helper functions.
"""

# -------------------- IMPORT LIBRARIES --------------------

import os
import sys

import pandas as pd
from box import Box
from google.cloud import bigquery
from loguru import logger

from util.academic.eutopia import EUTOPIA_INSTITUTION_REGISTRY, EUTOPIA_INSTITUTION_BIGQUERY_COLUMNS
from util.common.helpers import set_logger

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

    # Full target table ID
    target_table_id = f"{config.GCP.PROJECT_ID}.{config.GCP.INGESTION_SCHEMA}.{config.HISTORIC.EUTOPIA.TARGET_TABLE_NAME}"

    # Create a BigQuery client
    bq_client = bigquery.Client(project=config.GCP.PROJECT_ID)

    # ------------ TABLE: EUTOPIA_INSTITUTION ------------
    # Get the EUTOPIA universities
    df_eutopia = pd.DataFrame(
        [EUTOPIA_INSTITUTION_REGISTRY[university] for university in EUTOPIA_INSTITUTION_REGISTRY.keys()]
    )[EUTOPIA_INSTITUTION_BIGQUERY_COLUMNS]

    # Print that we are creating the table
    logger.info(f"Creating the table {target_table_id}...")
    # Configure the load job to replace data on an existing table
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    # Offload the DataFrame to BigQuery, appending it to the existing table
    job = bq_client.load_table_from_dataframe(
        dataframe=df_eutopia,
        destination=target_table_id,
        job_config=job_config
    )

    # Print that the table was created
    logger.info(f"Table {target_table_id} was created.")

    # ------------ FUNCTION: IS_EUTOPIA_AFFILIATED_STRING ------------

    # Print that we are creating the function
    logger.info("Creating the function UDF_IS_EUTOPIA_AFFILIATED_STRING...")
    # Create a BigQuery function to check if a string contains any of the EUTOPIA universities, where the SQL condition is located in EUTOPIA_INSTITUTION_REGISTRY['_SQL_STRING_CONDITION']
    query = F"""
    CREATE OR REPLACE FUNCTION `{config.GCP.PROJECT_ID}.{config.GCP.INGESTION_SCHEMA}.UDF_IS_EUTOPIA_AFFILIATED_STRING`(organization_string STRING)
    RETURNS BOOLEAN AS (
    {
    ' '.join(
        [
            f"{'OR' if i > 0 else ''} {EUTOPIA_INSTITUTION_REGISTRY[institution]['_SQL_STRING_CONDITION']('organization_string')}"
            for i, institution in enumerate(EUTOPIA_INSTITUTION_REGISTRY.keys())
        ]
    )
    }
    )
    """

    # Execute the query
    job = bq_client.query(query.format(project_id=config.GCP.PROJECT_ID, dataset_id=config.GCP.INGESTION_SCHEMA))

    # Print that the function was created
    logger.info("Function UDF_IS_EUTOPIA_AFFILIATED_STRING was created.")

    # ------------ FUNCTION: GET_EUTOPIA_INSTITUTION_ID ------------

    # Print that we are creating the function
    logger.info("Creating the function UDF_GET_EUTOPIA_INSTITUTION_SID...")

    # Create a BigQuery function to get the EUTOPIA institution ID from a string
    query = f"""
    CREATE OR REPLACE FUNCTION `{config.GCP.PROJECT_ID}.{config.GCP.INGESTION_SCHEMA}.UDF_GET_EUTOPIA_INSTITUTION_SID`(organization_string STRING)
        RETURNS STRING AS (
        (
            CASE 
                {
    ' '.join(
        [
            f"WHEN {EUTOPIA_INSTITUTION_REGISTRY[institution]['_SQL_STRING_CONDITION']('organization_string')} THEN '{institution}'"
            for institution in EUTOPIA_INSTITUTION_REGISTRY.keys()
        ]
    )
    }
            END
        )
    )
    """

    # Execute the query
    job = bq_client.query(query.format(project_id=config.GCP.PROJECT_ID, dataset_id=config.GCP.INGESTION_SCHEMA))

    # Print that the function was created
    logger.info("Function UDF_GET_EUTOPIA_INSTITUTION_SID was created.")
