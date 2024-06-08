import numpy as np
import pandas as pd
import torch
from bs4 import BeautifulSoup
from tqdm import tqdm
from transformers import AutoTokenizer, AutoModel

from util.academic.crossref import query_top_n_by_keyword
from util.embedding.helpers import embed_batch, split_list_to_batch


def get_initial_research_branch() -> dict:
    """
    Get the initial research branch.
    :return: The initial research branch
    """
    return dict(RESEARCH_BRANCH_CODE='n/a', RESEARCH_BRANCH_NAME='Other')


def extract_research_topics(html: BeautifulSoup) -> pd.DataFrame:
    """
    Extract research topics from the CERIF HTML.
    :param html: The CERIF HTML
    :return: DataFrame containing the extracted research topics
    """
    # Initialize variables to keep track of headers
    current_research_branch = get_initial_research_branch()
    current_research_subbranch = get_initial_research_branch()

    # Initialize lists to store the extracted data
    list_research_topic = list()

    for row in html.find_all('tr'):
        # Check for RESEARCH_BRANCH_LEVEL defined as <tr> having class='BarvaGlava', colspan=2 and a <b> tag inside with
        # branch name, a space, a letter, another space and finally 3 digits (the letter and 3 digits are the branch
        # code)
        row_clazz = row.get('class')
        if row_clazz is None:
            continue
        if 'BarvaGlava' in row_clazz and row.find('td', {'colspan': '2'}) and row.find('b'):
            current_research_branch = dict(
                RESEARCH_BRANCH_CODE=' '.join(row.find('b').get_text(strip=True).split(' ')[-2:]),
                RESEARCH_BRANCH_NAME=' '.join(row.find('b').get_text(strip=True).split(' ')[:-2])
            )
            # Reset the subbranch
            current_research_subbranch = get_initial_research_branch()

        # Check for RESEARCH_SUBBRANCH_LEVEL defined as having class='BarvaTR2', colspan=2 and a <b> tag inside with
        # subbranch name, a space, a letter, another space and finally 3 digits (the letter and 3 digits are the
        # subbranch code)
        elif 'BarvaTR2' in row_clazz and row.find('td', {'colspan': '2'}) and row.find('b'):
            current_research_subbranch = dict(
                RESEARCH_BRANCH_CODE=' '.join(row.find('b').get_text(strip=True).split(' ')[-2:]),
                RESEARCH_BRANCH_NAME=' '.join(row.find('b').get_text(strip=True).split(' ')[:-2])
            )
        # Check for RESEARCH_TOPIC_LEVEL defined as having class='BarvaTR1'. 2 <td> tags inside with the topic code
        # and the topic name
        elif 'BarvaTR1' in row_clazz:
            # Get the topic code and name
            topic_code, topic_name = row.find_all('td')

            # Append the extracted data to the list
            list_research_topic.append(dict(
                RESEARCH_BRANCH_CODE=current_research_branch['RESEARCH_BRANCH_CODE'],
                RESEARCH_BRANCH_NAME=current_research_branch['RESEARCH_BRANCH_NAME'],
                RESEARCH_SUBBRANCH_CODE=current_research_subbranch['RESEARCH_BRANCH_CODE'],
                RESEARCH_SUBBRANCH_NAME=current_research_subbranch['RESEARCH_BRANCH_NAME'],
                RESEARCH_TOPIC_CODE=topic_code.get_text(strip=True),
                RESEARCH_TOPIC_NAME=topic_name.get_text(strip=True),
            ))

    # Create a DataFrame from the list and return it
    return pd.DataFrame(list_research_topic)


def generate_crossref_topic_embedding_keyword(keyword: str,
                                              model: AutoModel,
                                              tokenizer: AutoTokenizer,
                                              n_articles: int = 10,
                                              batch_size: int = 8) -> list:
    """
    Generate embeddings for the research topics by extracting top 10 most relevant articles from Crossref API.
    :param batch_size: The batch size
    :param n_articles: The number of articles to extract
    :param keyword: The keyword to search for
    :param model: The model to use for embeddings
    :param tokenizer: The tokenizer to use for embeddings
    :return: The embeddings for the research topics
    """
    # Search for the keyword in the Crossref API
    articles = query_top_n_by_keyword(keyword=keyword,
                                      n=n_articles)

    # Split the articles into batches
    article_batches = split_list_to_batch(lst=articles,
                                          batch_size=batch_size)

    # List of article embeddings
    list_article_embeddings = list()
    for article_batch in article_batches:
        # Generate embeddings for the articles
        article_batch_embeddings = embed_batch(lst_to_embed=article_batch,
                                               model=model,
                                               tokenizer=tokenizer)
        # Add the embeddings to the list
        list_article_embeddings.extend(article_batch_embeddings)

    # Return the list of embeddings
    return list_article_embeddings


def generate_crossref_topic_embedding_batch(batch: pd.DataFrame,
                                            model: AutoModel,
                                            tokenizer: AutoTokenizer,
                                            n_articles: int = 10,
                                            batch_size: int = 8) -> list:
    """
    Generate embeddings for the research topics by extracting top 10 most relevant articles from Crossref API.
    :param batch_size: The batch size.
    :param n_articles: The number of articles to extract.
    :param batch: The batch of research topics.
    :return: The embeddings for the research topics.
    """
    list_topic_embeddings = list()
    for topic in batch['RESEARCH_TOPIC_NAME']:
        # Generate embeddings for the keyword
        keyword_embeddings = generate_crossref_topic_embedding_keyword(keyword=topic,
                                                                       model=model,
                                                                       tokenizer=tokenizer,
                                                                       n_articles=n_articles,
                                                                       batch_size=batch_size)

        # Aggregate the embeddings
        aggregated_topic_embedding = torch.mean(torch.stack(keyword_embeddings), dim=0)

        # Append the aggregated embedding to the list
        list_topic_embeddings.append(aggregated_topic_embedding)

    # Return the list of embeddings
    return list_topic_embeddings


def process_cerif_embedding_batch(batch: pd.DataFrame,
                                  model: AutoModel,
                                  tokenizer: AutoTokenizer,
                                  n_articles: int = 10,
                                  batch_size: int = 8) -> list:
    """
    Process a batch of CERIF research topics. This generates embeddings for the research topics from CERIF and enhances
    them by extracting top n most relevant articles from Crossref API. These articles will then be embedded and
    :param batch: The batch of research topics
    :param model: The model to use for embeddings
    :param tokenizer: The tokenizer to use for embeddings
    :param n_articles: The number of articles to extract from Crossref API to enhance the research topic embeddings.
    :param batch_size: The batch size.
    :return: The embeddings for the research topics.
    """
    # List of research topic codes and full text
    lst_research_topic_code = batch['RESEARCH_TOPIC_CODE']
    lst_research_topic_full_text = list(batch['RESEARCH_TOPIC_FULL_TEXT'])

    # Generate normalized embeddings for the research topics from CERIF
    cerif_topic_embeddings = embed_batch(lst_to_embed=lst_research_topic_full_text,
                                         model=model,
                                         tokenizer=tokenizer)

    # Each topic will be enhanced by extracting top 10 most relevant articles from Crossref API. These articles will
    # then be embedded and aggregated along with the research topic embedding to form a single embedding for the
    # research topic.
    crossref_topic_embeddings = generate_crossref_topic_embedding_batch(batch=batch,
                                                                        model=model,
                                                                        tokenizer=tokenizer,
                                                                        n_articles=n_articles,
                                                                        batch_size=batch_size)

    # Create a new batch combined from 20% CERIF embeddings and 80% Crossref embeddings
    return [
        {
            'RESEARCH_TOPIC_CODE': code,
            'EMBEDDING_TENSOR_SHAPE': list(cerif_tensor.shape),
            'EMBEDDING_TENSOR_DATA': (0.2 * cerif_tensor + 0.8 * crossref_tensor).flatten().tolist()
        }
        for code, cerif_tensor, crossref_tensor in
        zip(lst_research_topic_code, cerif_topic_embeddings, crossref_topic_embeddings)
    ]
