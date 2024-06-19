import json

import pandas as pd
import redis
from loguru import logger


def fetch(settings,
          query: str, ) -> pd.DataFrame:
    """
    Fetch the data from BigQuery and cache the result.
    :param settings: The settings.
    :param query: The query.
    :return: The data.
    """
    # Check if the query result is already in the cache
    cache_key = f"bigquery_cache:{query}"
    results = None
    try:
        cached_result = settings['redis_client'].get(cache_key)

        if cached_result:
            if settings['config'].DASHBOARD.VERBOSE:
                logger.info(f"Cache hit for query: {query}")
            # Return cached result if available
            return pd.DataFrame(json.loads(cached_result))

        else:
            if settings['config'].DASHBOARD.VERBOSE:
                logger.info(f"Cache miss for query: {query}")
            # Otherwise, query BigQuery
            query_job = settings['bq_client'].query(query)
            results = query_job.result().to_dataframe()

            # Cache the result for future use
            settings['redis_client'].set(cache_key, json.dumps(results.to_dict('records')), ex=3600)  # Cache for 1 hour
    except redis.ConnectionError as e:
        if settings['config'].DASHBOARD.VERBOSE:
            logger.error(f"Redis connection error: {e}")
            query_job = settings['bq_client'].query(query)
            results = query_job.result().to_dataframe()

    return results


def cols_to_title(df_cols: list) -> list:
    """
    Turn column names from snake case to title case and replace underscores with spaces.
    :param df_cols: The column names.
    :return: The column names in title case.
    """
    return [col.replace('_', ' ').title() for col in df_cols]


def overview_cards(settings: dict):
    """
    Get the overview cards.
    :param settings: The settings.
    :return: The overview cards.
    """
    schema = settings['config'].GCP.READ_SCHEMA
    query = f"""
        SELECT COUNT(DISTINCT ARTICLE_SID)                                       AS ARTICLES,
               COUNT(DISTINCT AUTHOR_SID)                                        AS AUTHORS,
               COUNT(DISTINCT IF(IS_SOLE_AUTHOR_PUBLICATION, ARTICLE_SID, NULL)) AS SOLE_AUTHOR_PUBLICATIONS,
               COUNT(DISTINCT IF(IS_INTERNAL_COLLABORATION, ARTICLE_SID, NULL))  AS INTERNAL_COLLABORATIONS,
               COUNT(DISTINCT IF(IS_EXTERNAL_COLLABORATION, ARTICLE_SID, NULL))  AS EXTERNAL_COLLABORATIONS,
               COUNT(DISTINCT IF(IS_EUTOPIAN_COLLABORATION, ARTICLE_SID, NULL))  AS EUTOPIAN_COLLABORATIONS
        FROM {schema}.FCT_COLLABORATION
    """

    # Fetch the data
    data = fetch(settings=settings,
                 query=query)

    # Turn column names from snake case to title case and replace underscores with spaces
    data.columns = cols_to_title(data.columns)
    return data


def overview_trend_eutopia_collaboration(settings: dict):
    """
    Get the trend of Eutopia collaborations.
    :param settings: The settings.
    :return: The trend of Eutopia collaborations.
    """
    schema = settings['config'].GCP.READ_SCHEMA

    query = f"""
        SELECT EXTRACT(YEAR FROM ARTICLE_PUBLICATION_DT) AS YEAR,
               COUNT(DISTINCT IF(IS_EUTOPIAN_COLLABORATION, ARTICLE_SID, NULL)) AS EUTOPIAN_COLLABORATIONS
        FROM {schema}.FCT_COLLABORATION
        WHERE EXTRACT(YEAR FROM ARTICLE_PUBLICATION_DT) >= 2000
        GROUP BY 1
        ORDER BY 1 ASC
    """

    # Fetch the data
    data = fetch(settings=settings,
                 query=query)

    # Turn column names from snake case to title case and replace underscores with spaces
    data.columns = cols_to_title(data.columns)

    return data


def overview_breakdown_publications_by_institution(settings):
    """
    Get the breakdown of publications by institution.
    :param settings: The settings.
    :return: The breakdown of publications by institution.
    """
    schema = settings['config'].GCP.READ_SCHEMA

    query = f"""
        SELECT INSTITUTION_SID             AS INSTITUTION,
               COUNT(DISTINCT ARTICLE_SID) AS ARTICLES
        FROM {schema}.FCT_COLLABORATION
        WHERE INSTITUTION_SID <> 'OTHER'
        GROUP BY 1
        ORDER BY 2 ASC
    """

    # Fetch the data
    data = fetch(settings=settings,
                 query=query)

    # Turn column names from snake case to title case and replace underscores with spaces
    data.columns = cols_to_title(data.columns)

    return data
