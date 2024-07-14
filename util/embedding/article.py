import pandas as pd
from google.cloud import bigquery
from transformers import AutoModel, AutoTokenizer

from util.embedding.helpers import embed_batch


def embed_article_batch(item: str,
                        iteration_settings: dict,
                        metadata: dict) -> dict:
    """
    Embed a batch of input texts using a transformer model.
    :param item: The input batch of texts to embed.
    :param metadata: The metadata for the current iteration including the model and tokenizer for embeddings
    :param iteration_settings:  The settings for the current iteration including list of records to offload to BigQuery and total number of records processed so far.
    :return: The updated settings for the current iteration.
    """
    # Get the metadata
    model: AutoModel = metadata['model']
    tokenizer: AutoTokenizer = metadata['tokenizer']

    # Get the lists of DOIs and articles to embed
    lst_dois = item['ARTICLE_DOI']
    lst_article_full_text = list(item['EMBEDDING_INPUT'])

    # Generate normalized embeddings
    embeddings = embed_batch(lst_to_embed=lst_article_full_text,
                             model=model,
                             tokenizer=tokenizer)

    # Join the embeddings with the DOIs
    new_batch = [
        {
            'DOI': doi,
            'EMBEDDING_TENSOR_SHAPE': list(tensor.shape),
            'EMBEDDING_TENSOR_DATA': tensor.flatten().tolist()
        }
        for doi, tensor in
        zip(lst_dois, embeddings)]

    # Update the settings for the current iteration
    iteration_settings['batch'].extend(new_batch)
    iteration_settings['n_iterations'] += 1
    iteration_settings['total_records'] += len(new_batch)

    # Return the updated settings
    return iteration_settings


def get_article_batch_count(bq_client: bigquery.Client,
                            source_table_id: str,
                            target_table_id: str,
                            batch_size: int) -> int:
    """
    Get the number of article batches that are in the source table but not in the target table.
    :param bq_client: The BigQuery client to use.
    :param source_table_id: The source table to compare.
    :param target_table_id: The target table to compare.
    :return: The number of articles that are in the source table but not in the target table.
    """

    # Define the query
    query = f"""SELECT CEILING(
                            DIV(
                                COUNT(1),
                                {batch_size}
                                )
                        ) AS BATCH_COUNT
                FROM `{source_table_id}` A
                LEFT JOIN `{target_table_id}` T 
                ON T.DOI = A.ARTICLE_DOI
                WHERE T.DOI IS NULL"""

    # Execute the query and return the result
    df = bq_client.query(query).result().to_dataframe()

    # Return the count
    return int(df.values[0][0])


def get_article_batch(bq_client: bigquery.Client,
                      source_table_id: str,
                      target_table_id: str,
                      batch_size: int) -> pd.DataFrame:
    """
    Get a batch of articles that are in the source table but not in the target table.
    :param bq_client: The BigQuery client to use.
    :param source_table_id: The source table to compare.
    :param target_table_id: The target table to compare.
    :return: A batch of articles that are in the source table but not in the target table.
    """

    # Define the query
    query = f"""SELECT * 
                FROM `{source_table_id}` A
                LEFT JOIN `{target_table_id}` T 
                ON T.DOI = A.ARTICLE_DOI
                WHERE T.DOI IS NULL
                LIMIT {batch_size}"""

    # Execute the query and return the result
    return bq_client.query(query).result().to_dataframe()
