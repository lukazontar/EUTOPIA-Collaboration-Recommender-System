import numpy as np
import pandas as pd
import torch.nn.functional as F
from torch import Tensor
from tqdm import tqdm
from transformers import AutoModel, AutoTokenizer


def average_pool(last_hidden_states: Tensor,
                 attention_mask: Tensor) -> Tensor:
    """
    Average pooling of the last hidden states of a transformer model.
    :param last_hidden_states: The last hidden states of a transformer model.
    :param attention_mask: The attention mask of the input.
    :return: The average pooled embeddings.
    """
    # Mask the last hidden states
    last_hidden = last_hidden_states.masked_fill(~attention_mask[..., None].bool(), 0.0)
    # Sum the last hidden states and divide by the number of non-padded tokens
    return last_hidden.sum(dim=1) / attention_mask.sum(dim=1)[..., None]


def get_model_and_tokenizer(model_name: str) -> tuple:
    """
    Get the model and tokenizer for a given model name.
    :param model_name: The name of the model.
    :return: The model and tokenizer.
    """
    # Load the model and tokenizer
    model = AutoModel.from_pretrained(model_name)
    tokenizer = AutoTokenizer.from_pretrained(model_name)

    return model, tokenizer


def split_list_to_batch(lst: list, batch_size: int):
    """
    Split a list into batches.
    :param lst: The list to split.
    :param batch_size: The size of each batch.
    :return: A list of batches.
    """
    return [lst[i:i + batch_size] for i in range(0, len(lst), batch_size)]


def embed_batch(lst_to_embed: list,
                model: AutoModel,
                tokenizer: AutoTokenizer) -> list:
    """
    Embed a batch of input texts using a transformer model.
    :param lst_to_embed: The input batch of texts to embed.
    :param model: The transformer model to use for embeddings.
    :param tokenizer: The tokenizer to use for embeddings.
    :return: The embeddings of the input texts.
    """
    # Tokenize the input texts
    batch_dict = tokenizer(lst_to_embed, max_length=512, padding=True, truncation=True, return_tensors='pt')

    # Get the embeddings from the model
    outputs = model(**batch_dict)
    # Average pool the embeddings
    embeddings = average_pool(outputs.last_hidden_state, batch_dict['attention_mask'])

    # Normalize embeddings
    normalized_embeddings = F.normalize(embeddings, p=2, dim=1)

    # Return the normalized embeddings
    return normalized_embeddings


def cosine_similarity(vector: np.ndarray,
                      matrix: np.ndarray) -> float:
    """
    Calculate the cosine similarity between a vector and a matrix.
    :param vector: Numpy vector
    :param matrix: Numpy matrix
    :return: Cosine similarity between the vector and the matrix
    """

    # Calculate the dot product between the vector and each row of the matrix
    dot_product = np.dot(matrix, vector.T).flatten()

    # Calculate the norm of the matrix and the vector
    matrix_norms = np.linalg.norm(matrix, axis=1)
    vector_norm = np.linalg.norm(vector)

    # Calculate the cosine similarity
    cosine_similarities = dot_product / (matrix_norms * vector_norm)

    return cosine_similarities
