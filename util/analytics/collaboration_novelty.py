import itertools

import pandas as pd
from google.cloud import bigquery


def update_collaboration_history(author_collaboration_history: dict,
                                 institution_collaboration_history: dict,
                                 diff: dict) -> tuple:
    """
    Update the collaboration history dictionaries with the new collaboration information.
    :param author_collaboration_history: Author collaboration history including the number of collaborations for each pair of authors
    :param institution_collaboration_history: Institution collaboration history including the number of collaborations for each pair of institutions
    :param diff: Difference between the collaboration history and the new publication
    :return: Updated author and institution collaboration history dictionaries
    """
    # Update the author collaboration history (for old authors)
    for author_tuple in diff['old_authors']:
        # Increment the number of collaborations
        author_collaboration_history[author_tuple] += 1

    # Update the institution collaboration history (for old institutions)
    for institution_tuple in diff['old_institutions']:
        # Increment the number of collaborations
        institution_collaboration_history[institution_tuple] += 1

    # Update the author collaboration history (for new authors)
    for author_tuple in diff['new_authors']:
        # If the author is not in the dictionary, add it
        author_collaboration_history[author_tuple] = 1

    # Update the institution collaboration history (for new institutions)
    for institution_tuple in diff['new_institutions']:
        # If the institution is not in the dictionary, add it
        institution_collaboration_history[institution_tuple] = 1

    return author_collaboration_history, institution_collaboration_history


def collaboration_difference(authors: list,
                             institutions: list,
                             author_collaboration_history: dict,
                             institution_collaboration_history: dict) -> dict:
    """
    Calculate the difference between the collaboration history and the new publication.
    :param author_collaboration_history: Author collaboration history including the number of collaborations for each pair of authors
    :param institution_collaboration_history: Institution collaboration history including the number of collaborations for each pair of institutions
    :param authors: List of authors of the new publication
    :param institutions: List of institutions of the new publication
    :return: Difference between the collaboration history and the new publication
    """
    # Authors
    new_authors = []
    old_authors = []
    # Go through all the pairs of authors
    for author_1, author_2 in itertools.combinations(authors, 2):
        # If the pair of authors is not in the author collaboration history, add it to the new authors
        if (author_1, author_2) not in author_collaboration_history.keys():
            new_authors.append((author_1, author_2))
        else:
            new_authors.append((author_1, author_2))

    # Institutions
    new_institutions = []
    old_institutions = []

    # Go through all the pairs of institutions
    for institution_1, institution_2 in itertools.combinations(institutions, 2):
        # If the pair of institutions is not in the institution collaboration history, add it to the new institutions
        if (institution_1, institution_2) not in institution_collaboration_history.keys():
            new_institutions.append((institution_1, institution_2))
        else:
            new_institutions.append((institution_1, institution_2))

    # Return the difference between the collaboration history and the new publication
    return dict(
        new_authors=new_authors,
        old_authors=old_authors,
        new_institutions=new_institutions,
        old_institutions=old_institutions
    )


def is_new_collaboration(diff: dict) -> bool:
    """
    Check if the collaboration is new.
    :param diff: Difference between the collaboration history and the new publication
    :return: True if the collaboration is new, False otherwise
    """
    return len(diff['new_authors']) == 0 and len(diff['new_institutions']) == 0


def derive_collaboration_novelty_index(diff: dict,
                                        author_collaboration_history: dict,
                                        institution_collaboration_history: dict) -> float:
    """
    Calculate the Novelty Collaboration Impact (NCI).

    Parameters:
    - diff: Difference between the collaboration history and the new publication
    - author_collaboration_history: Author collaboration history including the number of collaborations for each pair of authors
    - institution_collaboration_history: Institution collaboration history including the number of collaborations for each pair of institutions

    Returns:
    - NCI: Novelty Collaboration Impact score
    """

    authors_new = diff['new_authors']
    authors_old = diff['old_authors']
    institutions_new = diff['new_institutions']
    institutions_old = diff['old_institutions']

    # Calculate New Author Pair Factor
    author_pairs = authors_new + authors_old
    N_aa = sum(1 / (1 + author_collaboration_history.get((a1, a2), 0) + author_collaboration_history.get((a2, a1), 0))
               for (a1, a2) in author_pairs)

    # Calculate New Institution Pair Factor
    institution_pairs = institutions_new + institutions_old
    N_ii = sum(1 / (
            1 + institution_collaboration_history.get((i1, i2), 0) + institution_collaboration_history.get((i2, i1),
                                                                                                           0))
               for (i1, i2) in institution_pairs)

    # Calculate Size Adjustment Factor
    S_old = len(authors_old)
    S_a = 1 / (1 + S_old)

    # Calculate NCI
    NCI = N_aa * (1 + N_ii) * S_a

    return NCI


def process_article_collaboration_novelty(article_sid: str,
                                          authors: list,
                                          institutions: list,
                                          author_collaboration_history: dict,
                                          institution_collaboration_history: dict) -> tuple:
    """
    Process the article and derive the collaboration novelty impact. Calculate the difference between the collaboration history and the new publication, the Novelty Collaboration Impact (NCI), and update the collaboration history.
    :param authors: List of authors of the new publication
    :param institutions: List of institutions of the new publication
    :param article_sid: Article SID of the new publication
    :param author_collaboration_history: Author collaboration history including the number of collaborations for each pair of authors
    :param institution_collaboration_history: Institution collaboration history including the number of collaborations for each pair of institutions
    :return: Collaboration object and updated collaboration history
    """
    # Get the collaboration diff
    diff = collaboration_difference(authors=authors,
                                    institutions=institutions,
                                    author_collaboration_history=author_collaboration_history,
                                    institution_collaboration_history=institution_collaboration_history)

    # Calculate the Novelty Collaboration Impact (NCI)
    cni = derive_collaboration_novelty_index(diff=diff,
                                              author_collaboration_history=author_collaboration_history,
                                              institution_collaboration_history=institution_collaboration_history)

    # Init the collaboration object
    collaboration = dict(ARTICLE_SID=article_sid,
                         IS_NEW_COLLABORATION=is_new_collaboration(diff=diff),
                         COLLABORATION_NOVELTY_INDEX=cni)

    # Update the collaboration history
    author_collaboration_history, institution_collaboration_history = update_collaboration_history(
        diff=diff,
        author_collaboration_history=author_collaboration_history,
        institution_collaboration_history=institution_collaboration_history
    )

    # Return the collaboration object and the updated collaboration history
    return collaboration, author_collaboration_history, institution_collaboration_history


def query_publication_for_year(bq_client: bigquery.Client,
                               source_table_id: str,
                               year: int) -> pd.DataFrame:
    """
    Query the publication for a specific year.
    :param bq_client: BigQuery client
    :param source_table_id: Source table ID
    :param year: Year to query
    :return: DataFrame with the publications for the specific year
    """
    return bq_client.query(f"""
            SELECT ARTICLE_SID, AUTHOR_SID, INSTITUTION_SID, ARTICLE_PUBLICATION_DT 
            FROM {source_table_id}
            WHERE NOT IS_SOLE_AUTHOR_PUBLICATION
            AND EXTRACT(YEAR FROM ARTICLE_PUBLICATION_DT) = {year}
            ORDER BY ARTICLE_PUBLICATION_DT ASC
        """).result().to_dataframe()
