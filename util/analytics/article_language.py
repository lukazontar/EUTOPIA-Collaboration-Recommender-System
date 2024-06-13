from langdetect import detect, LangDetectException


def process_article_language(item: str,
                             iteration_settings: dict,
                             metadata: dict) -> dict:
    """
    Processes a single article by detecting the language of the article.
    :param item: The item to process.
    :param metadata: The metadata for the current iteration.
    :param iteration_settings:  The settings for the current iteration including list of records to offload to BigQuery and total number of records processed so far.
    :return: The updated settings for the current iteration.
    """

    language_input = item['LANGUAGE_INPUT']
    article_doi = item['ARTICLE_DOI']
    try:
        language = detect(language_input)
    except LangDetectException:
        language = 'n/a'

    # Update the settings for the current iteration
    iteration_settings['batch'].append(
        dict(
            ARTICLE_DOI=article_doi,
            ARTICLE_LANGUAGE=language
        )
    )
    iteration_settings['n_iterations'] += 1
    iteration_settings['total_records'] += 1

    # Return the updated settings for the current iteration
    return iteration_settings
