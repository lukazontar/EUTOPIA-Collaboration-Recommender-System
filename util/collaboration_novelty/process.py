import networkx as nx
import pandas as pd

from util.collaboration_novelty.difference import update_collaboration, collaboration_difference
from util.common.helpers import element_in_flattened_list


def derive_collaboration_novelty_index(diff: dict,
                                       G_a: nx.Graph,
                                       G_i: nx.Graph) -> float:
    """
    Calculate the Novelty Collaboration Impact (NCI).
    :param diff: Difference between the collaboration history and the new publication
    :param G_a: Collaboration author graph
    :param G_i: Collaboration institution graph
    :return: Novelty Collaboration Impact score
    """
    authors_new = diff['new_authors']
    authors_old = diff['old_authors']
    institutions_new = diff['new_institutions']
    institutions_old = diff['old_institutions']

    # Calculate New Author Pair Factor
    author_pairs = authors_new + authors_old
    N_aa = sum(1 / (1 + G_a.get_edge_data(a1, a2, default={'weight': 0})['weight']) for (a1, a2) in author_pairs)

    # Calculate New Institution Pair Factor
    institution_pairs = institutions_new + institutions_old
    N_ii = sum(1 / (1 + G_i.get_edge_data(i1, i2, default={'weight': 0})['weight']) for (i1, i2) in institution_pairs)

    # Calculate Size Adjustment Factor
    S_old = len(authors_old)
    S_a = 1 / (1 + S_old)

    # Calculate NCI
    NCI = N_aa * (1 + N_ii) * S_a

    return NCI


def process_article_collaboration_novelty(article_sid: str,
                                          df: pd.DataFrame,
                                          G_a: nx.Graph,
                                          G_i: nx.Graph) -> tuple:
    """
    Process the article and derive the collaboration novelty impact. Calculate the difference between the collaboration
    history and the new publication, the Novelty Collaboration Impact (NCI), and update the collaboration history.
    :param df: DataFrame with the article metadata including the authors and institutions
    :param article_sid: Article SID of the new publication
    :param G_a: Collaboration author graph
    :param G_i: Collaboration institution graph
    :return: Collaboration novelty index and metadata objects for the new publication
    """

    # Get authors and institutions
    author_affiliations = df[['AUTHOR_SID', 'INSTITUTION_SID']].drop_duplicates()

    # Get the collaboration diff based on author collaborations
    diff = collaboration_difference(G=G_a,
                                    author_affiliations=author_affiliations)

    # Calculate the Collaboration Novelty Index (CNI)
    cni = derive_collaboration_novelty_index(diff=diff,
                                             G_a=G_a,
                                             G_i=G_i)

    # Init the collaboration object
    cni_row = dict(ARTICLE_SID=article_sid,
                   COLLABORATION_NOVELTY_INDEX=cni)

    # Init the metadata object for each combination of author and institution
    metadata_rows = [
        dict(ARTICLE_SID=article_sid,
             AUTHOR_SID=author_sid,
             INSTITUTION_SID=institution_sid,
             ARTICLE_PUBLICATION_DT=df['ARTICLE_PUBLICATION_DT'].iloc[0],
             IS_NEW_AUTHOR_COLLABORATION=element_in_flattened_list(element=author_sid,
                                                                   list_of_lists=diff['new_authors']),
             IS_NEW_INSTITUTION_COLLABORATION=element_in_flattened_list(element=institution_sid,
                                                                        list_of_lists=diff['new_institutions'])
             )
        for (author_sid, institution_sid) in df[['AUTHOR_SID', 'INSTITUTION_SID']].drop_duplicates().values
    ]

    # Update the collaboration history
    update_collaboration(
        diff=diff,
        G_a=G_a,
        G_i=G_i
    )

    # Return the collaboration object and the updated collaboration history
    return cni_row, metadata_rows
