"""
Script: Derive new collaborations

This script reads through the published articles and defines the ones that are new collaborations based on the authors' affiliations and historic collaborations.
It also calculates the Novelty Collaboration Index (NCI) for each article.

"""
import itertools

# -------------------- IMPORT LIBRARIES --------------------

from box import Box
from google.cloud import bigquery

from util.analytics.collaboration_novelty import process_article_collaboration_novelty, query_publication_for_year
from util.common.helpers import conditional_offload_batch_to_bigquery

# -------------------- GLOBAL VARIABLES --------------------
PATH_TO_CONFIG_FILE = 'config.yml'

# -------------------- MAIN SCRIPT --------------------

if __name__ == '__main__':
    # Load the configuration file
    config = Box.from_yaml(filename=PATH_TO_CONFIG_FILE)

    # Full table IDs
    source_table_id = f"{config.GCP.PROJECT_ID}.{config.GCP.READ_SCHEMA}.{config.ANALYTICS.COLLABORATION_NOVELTY.SOURCE_TABLE_NAME}"
    target_table_id = f"{config.GCP.PROJECT_ID}.{config.GCP.ANALYTICS_SCHEMA}.{config.ANALYTICS.COLLABORATION_NOVELTY.TARGET_TABLE_NAME}"

    # Create a BigQuery client
    bq_client = bigquery.Client(project=config.GCP.PROJECT_ID)

    print("[INFO] Fetching all available years to query...")

    years = bq_client.query(f"""
    SELECT DISTINCT EXTRACT(YEAR FROM ARTICLE_PUBLICATION_DT) AS YEAR
    FROM {source_table_id}
    WHERE NOT IS_SOLE_AUTHOR_PUBLICATION
    AND EXTRACT(YEAR FROM ARTICLE_PUBLICATION_DT) >= 1995
    ORDER BY YEAR ASC
    """).result().to_dataframe()

    # Initialize the collaboration history dictionaries
    author_collaboration_history = dict()
    institution_collaboration_history = dict()

    # List of collaborations
    collaborations = []

    # Iterate through all the years
    for ix, year in enumerate(years['YEAR']):
        print(f"[INFO] Fetching all the articles for the year {year}...")
        # Get the articles that are included in the network
        articles = query_publication_for_year(bq_client=bq_client,
                                              source_table_id=source_table_id,
                                              year=year)

        print(f"[INFO] Iterating through all the articles for the year {year}...")

        # Iterate through all the articles
        for article_sid in articles['ARTICLE_SID'].unique():
            # Get the authors and institutions of the article
            authors = articles[articles['ARTICLE_SID'] == article_sid]['AUTHOR_SID'].unique()
            institutions = articles[articles['ARTICLE_SID'] == article_sid]['INSTITUTION_SID'].unique()

            # Process the article
            collaboration, author_collaboration_history, institution_collaboration_history = process_article_collaboration_novelty(
                authors=authors,
                institutions=institutions,
                article_sid=article_sid,
                author_collaboration_history=author_collaboration_history,
                institution_collaboration_history=institution_collaboration_history
            )

            # Append the collaboration to the list
            collaborations.append(collaboration)

        # Write the results to BigQuery every N years
        is_offloaded = conditional_offload_batch_to_bigquery(lst_batch=collaborations,
                                                             table_id=target_table_id,
                                                             client=bq_client,
                                                             ix_iter=ix,
                                                             n_max_iterations=config.ANALYTICS.COLLABORATION_NOVELTY.N_MAX_ITERATIONS_TO_OFFLOAD)
        # Reset the list of collaborations if offloaded
        if is_offloaded:
            collaborations = []

    # Write the rest of results to BigQuery
    conditional_offload_batch_to_bigquery(lst_batch=collaborations,
                                          table_id=target_table_id,
                                          client=bq_client,
                                          ix_iter=ix,
                                          n_max_iterations=config.ANALYTICS.COLLABORATION_NOVELTY.N_MAX_ITERATIONS_TO_OFFLOAD)

    # Reset the list of collaborations
    collaborations = []
