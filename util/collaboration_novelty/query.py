import pandas as pd
from google.cloud import bigquery


def query_collaboration_n_batches(bq_client: bigquery.Client,
                                  source_table_id: str,
                                  target_table_id: str,
                                  batch_size: int,
                                  min_year: int) -> int:
    """
    Query the number of batches to process.
    :param bq_client: BigQuery client
    :param source_table_id: Source table ID
    :param target_table_id: Target table ID
    :param batch_size: Batch size
    :param min_year: Minimum year to consider
    :return: Number of batches to process
    """

    query_text = f"""
    WITH LOADED_ARTICLES
        /* Get articles that have not been processed yet. */
         AS (SELECT ARTICLE_SID
             FROM {target_table_id}
             GROUP BY ARTICLE_SID),
     BATCH_OF_ARTICLES
        /* Get a batch of articles given a predefined N excluding sole author publications. */
         AS (SELECT COUNT(DISTINCT ARTICLE_SID) AS N
             FROM {source_table_id} S
                      LEFT JOIN LOADED_ARTICLES T USING (ARTICLE_SID)
             WHERE T.ARTICLE_SID IS NULL
               AND S.IS_SOLE_AUTHOR_PUBLICATION = FALSE
               AND S.AUTHOR_SID <> 'n/a'
               AND EXTRACT(YEAR FROM S.ARTICLE_PUBLICATION_DT) >= {min_year}
               )
    /* Get the number of batches */
    SELECT CEIL(N / {batch_size}) AS N_BATCHES
    FROM BATCH_OF_ARTICLES
    """

    return int(bq_client.query(query_text).result().to_dataframe().iloc[0, 0])


def query_collaboration_batch(bq_client: bigquery.Client,
                              source_table_id: str,
                              target_table_id: str,
                              batch_size: int,
                              min_year: int) -> pd.DataFrame:
    """
    Query a batch of collaborations that has not yet been processed.
    :param bq_client: BigQuery client
    :param source_table_id: Source table ID
    :param target_table_id: Target table ID
    :param batch_size: Batch size
    :param min_year: Minimum year to consider
    :return: DataFrame with the publications for the specific year
    """

    query_text = f"""
    WITH LOADED_ARTICLES
        /* Get articles that have not been processed yet. */
         AS (SELECT ARTICLE_SID
             FROM {target_table_id}
             GROUP BY ARTICLE_SID),
     BATCH_OF_ARTICLES
        /* Get a batch of articles given a predefined N excluding sole author publications. */
         AS (SELECT DISTINCT ARTICLE_SID
             FROM {source_table_id} S
                      LEFT JOIN LOADED_ARTICLES T USING (ARTICLE_SID)
             WHERE T.ARTICLE_SID IS NULL
               AND S.IS_SOLE_AUTHOR_PUBLICATION = FALSE
               AND S.AUTHOR_SID <> 'n/a'
               AND S.INSTITUTION_SID <> 'n/a'
               AND EXTRACT(YEAR FROM S.ARTICLE_PUBLICATION_DT) >= {min_year}
             LIMIT {batch_size})
    /* Get the final batch of articles including all rows for chosen article SIDs */
    SELECT S.ARTICLE_SID,
           S.AUTHOR_SID,
           S.INSTITUTION_SID,
           S.ARTICLE_PUBLICATION_DT
    FROM {source_table_id} S
             INNER JOIN BATCH_OF_ARTICLES B USING (ARTICLE_SID)
    ORDER BY S.ARTICLE_PUBLICATION_DT ASC
    """
    return bq_client.query(query_text).result().to_dataframe()
