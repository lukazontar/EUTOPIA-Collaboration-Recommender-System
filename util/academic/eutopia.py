# Registry of EUTOPIA institutions
from google.cloud import bigquery

EUTOPIA_INSTITUTION_BIGQUERY_COLUMNS = [
    'INSTITUTION_SID',
    'INSTITUTION_NAME',
    'INSTITUTION_PRETTY_NAME',
    'INSTITUTION_COUNTRY',
    'INSTITUTION_LANGUAGE',
    'INSTITUTION_COUNTRY_ISO2'
]

EUTOPIA_INSTITUTION_REGISTRY = {
    'UBBCLUJ': {
        'INSTITUTION_SID': 'UBBCLUJ',
        'INSTITUTION_NAME': 'Babes-Bolyai University',
        'INSTITUTION_PRETTY_NAME': 'Babeș-Bolyai University',
        'INSTITUTION_COUNTRY': 'Romania',
        'INSTITUTION_LANGUAGE': 'Romanian',
        'INSTITUTION_COUNTRY_ISO2': 'RO',
        '_SQL_STRING_CONDITION': lambda col_name: f"LOWER({col_name}) LIKE '%bolyai%'",
        '_PYTHON_STRING_CONDITION': lambda lower_str: 'bolyai' in lower_str
    },
    'VUB': {
        'INSTITUTION_SID': 'VUB',
        'INSTITUTION_NAME': 'Vrije Universiteit Brussel',
        'INSTITUTION_PRETTY_NAME': 'Vrije Universiteit Brussel',
        'INSTITUTION_COUNTRY': 'Belgium',
        'INSTITUTION_LANGUAGE': 'Dutch, French',
        'INSTITUTION_COUNTRY_ISO2': 'BE',
        '_SQL_STRING_CONDITION': lambda col_name: f"LOWER({col_name}) LIKE '%vrije%'",
        '_PYTHON_STRING_CONDITION': lambda lower_str: 'vrije' in lower_str

    },
    'UNIVE': {
        'INSTITUTION_SID': 'UNIVE',
        'INSTITUTION_NAME': 'Ca Foscari University of Venice',
        'INSTITUTION_PRETTY_NAME': 'Ca\'Foscari University of Venice',
        'INSTITUTION_COUNTRY': 'Italy',
        'INSTITUTION_LANGUAGE': 'Italian',
        'INSTITUTION_COUNTRY_ISO2': 'IT',
        '_SQL_STRING_CONDITION': lambda col_name: f"LOWER({col_name}) LIKE '%ca__foscari%'",
        '_PYTHON_STRING_CONDITION': lambda lower_str: 'ca\' foscari' in lower_str

    },
    'CY': {
        'INSTITUTION_SID': 'CY',
        'INSTITUTION_NAME': 'CY Cergy Paris Universite',
        'INSTITUTION_PRETTY_NAME': 'CY Cergy Paris Université',
        'INSTITUTION_COUNTRY': 'France',
        'INSTITUTION_LANGUAGE': 'French',
        'INSTITUTION_COUNTRY_ISO2': 'FR',
        '_SQL_STRING_CONDITION': lambda col_name: f"LOWER({col_name}) LIKE '%cergy%'",
        '_PYTHON_STRING_CONDITION': lambda str: 'cergy' in str

    },
    'TU_DRESDEN': {
        'INSTITUTION_SID': 'TU_DRESDEN',
        'INSTITUTION_NAME': 'Technische Universitat Dresden',
        'INSTITUTION_PRETTY_NAME': 'Technische Universität Dresden',
        'INSTITUTION_COUNTRY': 'Germany',
        'INSTITUTION_LANGUAGE': 'German',
        'INSTITUTION_COUNTRY_ISO2': 'DE',
        '_SQL_STRING_CONDITION': lambda col_name: f"LOWER({col_name}) LIKE '%dresden%'",
        '_PYTHON_STRING_CONDITION': lambda str: 'dresden' in str

    },
    'GU': {
        'INSTITUTION_SID': 'GU',
        'INSTITUTION_NAME': 'University of Gothenburg',
        'INSTITUTION_PRETTY_NAME': 'University of Gothenburg',
        'INSTITUTION_COUNTRY': 'Sweden',
        'INSTITUTION_LANGUAGE': 'Swedish',
        'INSTITUTION_COUNTRY_ISO2': 'SE',
        '_SQL_STRING_CONDITION': lambda
            col_name: f"LOWER({col_name}) LIKE '%universit%gothenburg%' OR LOWER({col_name}) LIKE '%gothenburg%universit%'",
        '_PYTHON_STRING_CONDITION': lambda lower_str: 'univer' in lower_str and 'gothenburg' in lower_str
    },
    'UNI_LJ': {
        'INSTITUTION_SID': 'UNI_LJ',
        'INSTITUTION_NAME': 'University of Ljubljana',
        'INSTITUTION_PRETTY_NAME': 'University of Ljubljana',
        'INSTITUTION_COUNTRY': 'Slovenia',
        'INSTITUTION_LANGUAGE': 'Slovene',
        'INSTITUTION_COUNTRY_ISO2': 'SI',
        '_SQL_STRING_CONDITION': lambda
            col_name: f"LOWER({col_name}) LIKE '%ljubljan%univer%' OR LOWER({col_name}) LIKE '%univer%ljubljan%'",
        '_PYTHON_STRING_CONDITION': lambda lower_str: 'univer' in lower_str and 'ljubljan' in lower_str

    },
    'UNL': {
        'INSTITUTION_SID': 'UNL',
        'INSTITUTION_NAME': 'NOVA University Lisbon',
        'INSTITUTION_PRETTY_NAME': 'NOVA University Lisbon',
        'INSTITUTION_COUNTRY': 'Portugal',
        'INSTITUTION_LANGUAGE': 'Portuguese',
        'INSTITUTION_COUNTRY_ISO2': 'PT',
        '_SQL_STRING_CONDITION': lambda col_name: f"LOWER({col_name}) LIKE '%nova%lisbo%'",
        '_PYTHON_STRING_CONDITION': lambda lower_str: 'nova' in lower_str and 'lisbo' in lower_str

    },
    'UPF': {
        'INSTITUTION_SID': 'UPF',
        'INSTITUTION_NAME': 'Pompeu Fabra University-Barcelona',
        'INSTITUTION_PRETTY_NAME': 'Pompeu Fabra University-Barcelona',
        'INSTITUTION_COUNTRY': 'Spain',
        'INSTITUTION_LANGUAGE': 'Spanish, Catalan',
        'INSTITUTION_COUNTRY_ISO2': 'ES',
        '_SQL_STRING_CONDITION': lambda col_name: f"LOWER({col_name}) LIKE '%pompeu fabra%'",
        '_PYTHON_STRING_CONDITION': lambda lower_str: 'pompeu fabra' in lower_str

    },
    'WARWICK': {
        'INSTITUTION_SID': 'WARWICK',
        'INSTITUTION_NAME': 'University of Warwick',
        'INSTITUTION_PRETTY_NAME': 'University of Warwick',
        'INSTITUTION_COUNTRY': 'United Kingdom',
        'INSTITUTION_LANGUAGE': 'English',
        'INSTITUTION_COUNTRY_ISO2': 'GB',
        '_SQL_STRING_CONDITION': lambda col_name: f"LOWER({col_name}) LIKE '%warwick%'",
        '_PYTHON_STRING_CONDITION': lambda lower_str: 'warwick' in lower_str

    }
}


def is_eutopia_affiliated_string(string: str) -> bool:
    """
    Check if a string contains any of the EUTOPIA universities.

    Args:
        string (str): The string to check.

    Returns:
        bool: True if the string contains any of the EUTOPIA universities, False otherwise.
    """

    # Map dictionary to string and to lowercase
    string_lower = string.lower()

    # Check if the string contains any of the EUTOPIA universities
    return any(
        [EUTOPIA_INSTITUTION_REGISTRY[institution]['_PYTHON_STRING_CONDITION'](string_lower)
         for institution in EUTOPIA_INSTITUTION_REGISTRY.keys()]
    )


def get_eutopia_institution_id(string: str) -> str:
    """
    Get the EUTOPIA institution ID from a string.
    :param string: The string to check.
    :return: The EUTOPIA institution ID.
    """

    # Map dictionary to string and to lowercase
    string_lower = string.lower()

    # Check if the string contains any of the EUTOPIA universities
    for institution in EUTOPIA_INSTITUTION_REGISTRY.keys():
        if EUTOPIA_INSTITUTION_REGISTRY[institution]['_PYTHON_STRING_CONDITION'](string_lower):
            return institution
    return None
