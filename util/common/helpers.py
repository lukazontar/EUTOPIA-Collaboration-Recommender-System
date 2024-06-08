import backoff
import requests
import pandas as pd
from google.cloud import bigquery
from tqdm import tqdm

MAILTO_EMAIL = 'luka.zontar.consulting@gmail.com'


# ------------------------------ make_request ------------------------------
# Exponential backoff on HTTP errors (status code >= 500) and RequestException
@backoff.on_exception(backoff.expo,
                      (requests.exceptions.RequestException, requests.exceptions.HTTPError),
                      max_tries=8,
                      giveup=lambda e: e.response is not None and e.response.status_code < 500)
def make_request(url: str,
                 params: dict) -> dict:
    """
    Make a request to the given URL with the given parameters
    :param url: URL to make the request to.
    :param params: Parameters to include in the request.
    :return: JSON response from the request.
    """
    response = requests.get(url, params=params)
    response.raise_for_status()  # Raise an exception for HTTP errors

    return response.json()


# ------------------------------ offload_batch_to_bigquery ------------------------------
def offload_batch_to_bigquery(lst_batch: list,
                              table_id: str,
                              client: bigquery.Client,
                              data_schema: list = None) -> None:
    """
    Offloads a batch of records to BigQuery.
    :param data_schema: The schema of the data to offload.
    :param client: BigQuery client.
    :param lst_batch: List of records to offload.
    :param table_id: The ID of the destination table in BigQuery.
    """
    # Convert the list of records to a DataFrame
    df_batch = pd.DataFrame(lst_batch)

    # Configure the load job to append data to an existing table
    if data_schema is not None:
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            schema=data_schema
        )
    else:
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        )

    # Offload the DataFrame to BigQuery, appending it to the existing table
    job = client.load_table_from_dataframe(
        dataframe=df_batch,
        destination=table_id,
        job_config=job_config
    )

    # Wait for the load job to complete
    job.result()

    # Print info message on success
    print(f"[INFO] [INFO] Offloaded a batch of {len(df_batch)} items to BigQuery.")


def get_empty_iteration_settings(total_records: int = 0) -> dict:
    """
    Returns an empty iteration settings dictionary.
    :return: An empty iteration settings dictionary.
    """
    return {
        'batch': list(),
        'n_iterations': 0,
        'total_records': total_records
    }


#
def iterative_offload_to_bigquery(iterable: list,
                                  function_process_single: callable,
                                  table_id: str,
                                  client: bigquery.Client,
                                  metadata: dict = None,
                                  max_records: int = None,
                                  max_iterations_to_offload: int = 5000,
                                  start_iteration: int = 0,
                                  data_schema: list = None) -> None:
    """
    A wrapper function to process a list of items in a historic data dump and offload the results to BigQuery.
    :param schema: The schema of the data to offload.
    :param start_iteration: The iteration to start from.
    :param max_iterations_to_offload:  The maximum number of iterations to offload to BigQuery.
    :param iterable: The iterable to process.
    :param function_process_single: The function to process a single item.
    :param metadata: Metadata to pass to the processing function.
    :param table_id: The ID of the destination table in BigQuery.
    :param max_records: The maximum number of records to process. Set to None to process all records.
    :param client: BigQuery client.
    """

    # Initialize an empty list for batching results and index for total records
    iteration_settings = get_empty_iteration_settings()

    for ix, item in enumerate(tqdm(iterable)):
        if ix < start_iteration:
            continue

        # Process a single item
        iteration_settings = function_process_single(item=item,
                                                     iteration_settings=iteration_settings,
                                                     metadata=metadata)

        # Check if the number of iterations has reached maximum
        if iteration_settings['n_iterations'] >= max_iterations_to_offload:
            # Offload the DataFrame to BigQuery
            offload_batch_to_bigquery(lst_batch=iteration_settings['batch'],
                                      table_id=table_id,
                                      client=client)

            # Get a new empty iteration settings
            iteration_settings = get_empty_iteration_settings(iteration_settings['total_records'])

            # Check if the total number of records processed so far has reached the maximum
            if max_records is not None and iteration_settings['total_records'] >= max_records:
                break

    # Offload any remaining rows in the DataFrame to BigQuery
    if len(iteration_settings['batch']) > 0:
        offload_batch_to_bigquery(lst_batch=iteration_settings['batch'],
                                  table_id=table_id,
                                  client=client,
                                  data_schema=data_schema)


def conditional_offload_batch_to_bigquery(lst_batch: list,
                                          table_id: str,
                                          client: bigquery.Client,
                                          ix_iteration: int,
                                          n_max_iterations: int):
    """
    Offloads a batch of records to BigQuery every N iterations.
    :param lst_batch: list of records to offload.
    :param table_id: The ID of the destination table in BigQuery.
    :param client: BigQuery client.
    :param ix_iteration: The current iteration index.
    :param n_max_iterations: The maximum number of iterations before offloading the batch.
    :return: True if the batch is offloaded, False otherwise.
    """

    # Offload the batch to BigQuery every N iterations
    if ix_iteration > 0 and ix_iteration % n_max_iterations == 0:
        offload_batch_to_bigquery(lst_batch=lst_batch,
                                  table_id=table_id,
                                  client=client)
        return True

    # Return False if the batch is not offloaded
    return False
