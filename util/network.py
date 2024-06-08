import networkx as nx
import pandas as pd
from google.cloud import bigquery
import pickle


def get_articles(client: bigquery.Client, schema: str = 'DBT_DEV'):
    """
    Get all articles
    :param client: BigQuery client
    :param schema: Schema name
    :return: DataFrame containing all authors
    """
    # Get all authors
    q_articles = f"""
    SELECT A.ARTICLE_SID,
           A.ARTICLE_DOI,
    FROM {schema}.DIM_ARTICLE A
             INNER JOIN {schema}.INT_COLLABORATION F
                        USING (ARTICLE_SID)
    WHERE F.IS_EUTOPIAN_ARTICLE
    GROUP BY ALL;
    """
    # Execute the query and store the result in a DataFrame
    df_articles = client.query(q_articles).to_dataframe()

    # Return the DataFrame
    return df_articles


def get_authors(client: bigquery.Client, schema: str = 'DBT_DEV'):
    """
    Get all authors
    :param client: BigQuery client
    :param schema: Schema name
    :return: DataFrame containing all authors
    """
    # Get all authors
    q_authors = f"""
    SELECT A.AUTHOR_SID,
           A.AUTHOR_FULL_NAME,
           A.AUTHOR_ORCID_ID
    FROM {schema}.DIM_AUTHOR A
             INNER JOIN {schema}.INT_COLLABORATION F
                       USING (AUTHOR_SID)
    WHERE F.IS_EUTOPIAN_ARTICLE
    GROUP BY ALL;
    """

    # Execute the query and store the result in a DataFrame
    df_authors = client.query(q_authors).to_dataframe()

    # Return the DataFrame
    return df_authors


def get_edges(client: bigquery.Client, schema: str = 'DBT_DEV'):
    """
    Get all edges
    :param client: BigQuery client
    :param schema: Schema name
    :return: DataFrame containing all edges
    """

    q_edges = f"""
    SELECT DISTINCT AUTHOR_SID, ARTICLE_SID
    FROM {schema}.INT_COLLABORATION
    WHERE IS_EUTOPIAN_ARTICLE;
    """

    # Execute the query and store the result in a DataFrame
    df_edges = client.query(q_edges).to_dataframe()

    # Return the DataFrame
    return df_edges


def add_author_attributes(B: nx.Graph,
                          authors: pd.DataFrame):
    """
    Add article attributes to the graph
    :param authors: DataFrame containing authors
    :param B: Bipartite graph
    """

    # Add author attributes
    for orcid_id, row in authors.iterrows():
        nx.set_node_attributes(B, {
            row['ORCID_ID']: {'full_name': row['ORCID_MEMBER_FULL_NAME'], 'locale': row['ORCID_MEMBER_LOCALE']}})


def add_article_attributes(B: nx.Graph,
                           articles: pd.DataFrame):
    """
    Add article attributes to the graph
    :param articles: DataFrame containing articles
    :param B: Bipartite graph
    """
    # Add article attributes
    for doi, row in articles.iterrows():
        nx.set_node_attributes(B, {
            row['DOI']: {'title': row['TITLE'], 'publication_date': row['PUBLICATION_DT'], 'type': row['TYPE']}})

