import pickle

import networkx as nx
from google.cloud import storage
from google.cloud.exceptions import NotFound
from loguru import logger


def fetch_collaboration_graph(bucket: storage.Bucket,
                              graph_a_blob_name: str,
                              graph_i_blob_name: str) -> tuple:
    """
    Query the collaboration history stored in a tuple of graphs.
    :param bucket: Google Cloud Storage client
    :param graph_a_blob_name: Author collaboration graph blob name
    :param graph_i_blob_name: Institution collaboration graph blob name
    :return: Tuple of author and institution collaboration graphs
    """

    # Init the graphs
    G_a = nx.Graph()
    G_i = nx.Graph()

    try:
        # Fetch blob from Google Cloud Storage
        G_a_blob = bucket.blob(blob_name=graph_a_blob_name)
        G_i_blob = bucket.blob(blob_name=graph_i_blob_name)

        # Download the data
        G_a_data = G_a_blob.download_as_string()
        G_i_data = G_i_blob.download_as_string()

        # Deserialize the data
        G_a = pickle.loads(G_a_data)
        G_i = pickle.loads(G_i_data)
        return G_a, G_i
    except NotFound as e:
        logger.error("Could not find the collaboration graphs from Google Cloud Storage.")

    return G_a, G_i


def save_graphs(bucket: storage.Bucket,
                graph_a_blob_name: str,
                graph_i_blob_name: str,
                _G_a: nx.Graph,
                _G_i: nx.Graph):
    """
    Save the graphs to Google Cloud Storage
    :param graph_a_blob_name: Author collaboration graph blob name
    :param graph_i_blob_name: Institution collaboration graph blob name
    :param bucket: Google Cloud Storage client
    :param _G_a: Author collaboration graph
    :param _G_i: Institution collaboration graph
    """

    # Serialize the graphs
    G_a_data = pickle.dumps(_G_a)
    G_i_data = pickle.dumps(_G_i)

    # Create a blob for each graph and upload the data
    G_a_blob = bucket.blob(blob_name=graph_a_blob_name)
    G_a_blob.upload_from_string(G_a_data, content_type='application/octet-stream')

    G_i_blob = bucket.blob(blob_name=graph_i_blob_name)
    G_i_blob.upload_from_string(G_i_data, content_type='application/octet-stream')
