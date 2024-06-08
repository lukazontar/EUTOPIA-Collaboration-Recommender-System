from google.cloud import bigquery


def get_datalake_articles(client: bigquery.Client,
                          project_id: str = 'collaboration-recommender',
                          schema: str = 'DATALAKE'):
    """
    Get all EUTOPIA articles
    :param project_id: BigQuery project ID
    :param client: BigQuery client
    :param schema: Schema name
    :return: DataFrame containing all authors
    """
    # Get all articles that are EUTOPIA articles
    q_articles = f"""
        SELECT *
        FROM `{project_id}`.{schema}.V_ARTICLE;
        """
    # Execute the query and store the result in a DataFrame
    df_articles = client.query(q_articles).to_dataframe()

    # Return the DataFrame
    return df_articles


def get_datalake_reference_articles(client: bigquery.Client,
                                    project_id: str = 'collaboration-recommender',
                                    schema: str = 'DATALAKE'):
    """
    Get all EUTOPIA articles
    :param project_id: BigQuery project ID
    :param client: BigQuery client
    :param schema: Schema name
    :return: DataFrame containing all authors
    """
    # Get all articles that are EUTOPIA articles
    q_articles = f"""
        SELECT *
        FROM `{project_id}`.{schema}.V_REFERENCE_ARTICLE;
    """
    # Execute the query and store the result in a DataFrame
    df_articles = client.query(q_articles).to_dataframe()

    # Return the DataFrame
    return df_articles


def get_datalake_authors(client: bigquery.Client,
                         project_id: str = 'collaboration-recommender',
                         schema: str = 'DATALAKE'):
    """
    Get all authors
    :param project_id: BigQuery project ID
    :param client: BigQuery client
    :param schema: Schema name
    :return: DataFrame containing all authors
    """
    # Get all authors
    q_authors = f"""
        SELECT *
        FROM `{project_id}`.{schema}.V_AUTHOR;
    """

    # Execute the query and store the result in a DataFrame
    df_authors = client.query(q_authors).to_dataframe()

    # Return the DataFrame
    return df_authors
