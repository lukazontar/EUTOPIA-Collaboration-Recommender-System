"""
Script: Process ORCID Yearly Dump

This script processes the yearly summary of ORCID records. The summary is a Tarfile file with summaries of ORCID records for a specific year. The summary is an XML file.
The script reads the XML file and checks if it references an affiliation that is a member of the EUTOPIA organization. If it does, the record is saved in a BigQuery table.
"""

# -------------------- IMPORT LIBRARIES --------------------
import tarfile

from box import Box
from google.cloud import bigquery
from loguru import logger

from util.academic.orcid import process_tarfile_file
from util.common.helpers import iterative_offload_to_bigquery, set_logger

# -------------------- GLOBAL VARIABLES --------------------
PATH_TO_CONFIG_FILE = 'config.yml'

# -------------------- MAIN SCRIPT --------------------
if __name__ == '__main__':
    # Set logger 
    set_logger()
    # Load the configuration file
    config = Box.from_yaml(filename=PATH_TO_CONFIG_FILE)

    # Create a BigQuery client
    bq_client = bigquery.Client(project=config.GCP.PROJECT_ID)

    # Full target table ID
    target_table_id = f"{config.GCP.PROJECT_ID}.{config.GCP.INGESTION_SCHEMA}.{config.HISTORIC.ORCID_TARFILE.TARGET_TABLE_NAME}"

    # Print that the process is starting
    logger.info("Started processing the ORCID yearly dump...")
    # Process the tarfile
    with tarfile.open(config.HISTORIC.ORCID_TARFILE.FILE_PATH, "r:gz") as tar:
        # Setup metadata
        metadata = {
            'tar': tar
        }
        # Process Crossref historic data from yearly data dump
        iterative_offload_to_bigquery(
            iterable=tar,
            function_process_single=process_tarfile_file,
            client=bq_client,
            table_id=target_table_id,
            metadata=metadata,
            max_records=config.HISTORIC.ORCID_TARFILE.N_MAX_RECORDS,
            start_iteration=config.HISTORIC.ORCID_TARFILE.START_ITERATION
        )

    # Print that the process is finished
    logger.info(f"Finished processing the ORCID yearly dump and data is saved to table: {target_table_id}")
