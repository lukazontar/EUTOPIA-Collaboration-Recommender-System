import pandas as pd
import torch
from tqdm import tqdm
from transformers import AutoModel, AutoTokenizer

from util.embedding.helpers import embed_batch


def embed_research_topic_metadata(lst_research_topics: list,
                                  model: AutoModel,
                                  tokenizer: AutoTokenizer) -> pd.DataFrame:
    """
    Process the embeddings for the CERIF research topics.
    :param research_topics: list of batches of research topics
    :param model: model to use for embeddings
    :param tokenizer: tokenizer to use for embeddings
    :return: DataFrame containing the embeddings
    """

    # Embeddings list
    lst_embeddings = list()

    # Iterate through the batches
    for batch in tqdm(lst_research_topics):
        # List of research topic codes and full text
        lst_research_topic_code = batch['RESEARCH_TOPIC_CODE']
        lst_research_topic_embedding_input = list(batch['EMBEDDING_INPUT'])

        # Generate normalized embeddings for the research topics from CERIF
        cerif_topic_embeddings = embed_batch(lst_to_embed=lst_research_topic_embedding_input,
                                             model=model,
                                             tokenizer=tokenizer)
        # Process the batch
        new_batch = [
            {
                'RESEARCH_TOPIC_CODE': research_topic_code,
                'EMBEDDING_TENSOR_SHAPE': list(tensor.shape),
                'EMBEDDING_TENSOR_DATA': tensor.flatten().tolist()
            }
            for research_topic_code, tensor in
            zip(lst_research_topic_code, cerif_topic_embeddings)
        ]

        # Add the embeddings to the list
        lst_embeddings.extend(new_batch)

    # Create a DataFrame from the embeddings
    return pd.DataFrame(lst_embeddings)


def embed_research_topic_top_n_articles_batch(item: str,
                                              iteration_settings: dict,
                                              metadata: dict) -> dict:
    """
    Process the embeddings for the CERIF research topics and their top N articles.
    :param lst_research_topic_top_n_articles: list of batches of research topics
    :param model: model to use for embeddings
    :param tokenizer: tokenizer to use for embeddings
    :return: DataFrame containing the embeddings
    """

    # Get the metadata
    model: AutoModel = metadata['model']
    tokenizer: AutoTokenizer = metadata['tokenizer']

    # List of research topic codes and DOIs
    lst_research_topic_code = item['RESEARCH_TOPIC_CODE']
    lst_article_doi = list(item['ARTICLE_DOI'])
    lst_embedding_input = list(item['EMBEDDING_INPUT'])

    # Generate normalized embeddings for the research topics from CERIF
    embeddings = embed_batch(lst_to_embed=lst_embedding_input,
                             model=model,
                             tokenizer=tokenizer)

    # Process the batch
    new_batch = [
        {
            'RESEARCH_TOPIC_CODE': research_topic_code,
            'ARTICLE_DOI': article_doi,
            'EMBEDDING_TENSOR_SHAPE': list(tensor.shape),
            'EMBEDDING_TENSOR_DATA': tensor.flatten().tolist()
        }
        for research_topic_code, article_doi, tensor in
        zip(lst_research_topic_code, lst_article_doi, embeddings)
    ]

    # Update the settings for the current iteration
    iteration_settings['batch'].extend(new_batch)
    iteration_settings['n_iterations'] += 1
    iteration_settings['total_records'] += len(new_batch)

    # Return the updated settings
    return iteration_settings


def combine_research_topic_embeddings(research_topic_code: str,
                                      df_research_topic_metadata: pd.DataFrame,
                                      df_research_topic_top_n_articles: pd.DataFrame) -> torch.Tensor:
    """
    Combine the embeddings for the research topic and its top N articles.
    :param research_topic_code: The research topic code to combine the embeddings for.
    :param df_research_topic_metadata: The DataFrame containing the research topic embeddings.
    :param df_research_topic_top_n_articles: The DataFrame containing the top N articles embeddings.
    :return: The combined embedding for the research topic and its top N articles.
    """
    # Fetch research topic embedding
    df_research_topic_batch = df_research_topic_metadata[
        df_research_topic_metadata['RESEARCH_TOPIC_CODE'] == research_topic_code]
    embedding_research_topic = torch.tensor(df_research_topic_batch['EMBEDDING_TENSOR_DATA'].values[0])

    # Fetch top N articles embeddings
    # Filter the top N articles for the given research topic code
    df_research_topic_top_n_articles_batch = df_research_topic_top_n_articles[
        df_research_topic_top_n_articles['RESEARCH_TOPIC_CODE'] == research_topic_code]

    # Check if the DataFrame is empty
    if df_research_topic_top_n_articles_batch.empty:
        embedding_combined = embedding_research_topic
    else:
        # Calculate average embedding
        tensor_data = df_research_topic_top_n_articles_batch['EMBEDDING_TENSOR_DATA'].values
        tensor_data = [torch.tensor(data) for data in tensor_data]
        stacked_tensors = torch.stack(tensor_data)
        embedding_top_n_articles = torch.mean(stacked_tensors, dim=0)

        # Combine the embeddings
        embedding_combined = (0.2 * embedding_research_topic + 0.8 * embedding_top_n_articles)

    # Return the combined embedding
    return embedding_combined
