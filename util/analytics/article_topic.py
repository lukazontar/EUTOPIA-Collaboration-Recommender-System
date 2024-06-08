import numpy as np
import pandas as pd

from google.cloud import bigquery
from tqdm import tqdm

from util.common.helpers import offload_batch_to_bigquery
from util.embedding.helpers import cosine_similarity


def process_article_embedding_batch(article_embeddings: pd.DataFrame,
                                    topic_embeddings: pd.DataFrame,
                                    topic_embedding_values: np.ndarray,
                                    bq_client: bigquery.Client,
                                    target_table_id: str):
    """
    Process the article embeddings and calculate the cosine similarity between the research topics and the articles.
    :param topic_embedding_values: The research topic embeddings as a list of numpy arrays.
    :param article_embeddings: The article embeddings.
    :param topic_embeddings: The research topic embeddings.
    :param bq_client: The BigQuery client.
    :param target_table_id: The target table ID.
    """
    print("[INFO] Calculating the cosine similarity between the research topics and the articles...")

    # Initialize the article-topic mapping
    article_topic_mapping = []
    for i, article_embedding in enumerate(tqdm(article_embeddings['EMBEDDING_TENSOR_DATA'].values.tolist())):

        # Calculate the cosine similarity between the research topics and the article
        similarities = cosine_similarity(vector=article_embedding,
                                         matrix=topic_embedding_values)

        # Find the 3 top most similar research topics
        top_3_topics = np.argsort(similarities)[::-1][:3]

        # Add all the top 3 topics to the mapping along with ranking
        for rank, topic in enumerate(top_3_topics, 1):
            # Add the article-topic mapping
            article_topic = dict(DOI=article_embeddings['DOI'].iloc[i],
                                 RESEARCH_TOPIC_CODE=topic_embeddings['RESEARCH_TOPIC_CODE'].iloc[topic],
                                 RANK=rank)

            # Append to the article-topic mapping list
            article_topic_mapping.append(article_topic)

    print("[INFO] Offloading the article-topic mapping to BigQuery...")
    offload_batch_to_bigquery(client=bq_client,
                              table_id=target_table_id,
                              lst_batch=article_topic_mapping)
