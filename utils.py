import re
import datetime
from datetime import timedelta
from google.cloud import bigquery
from sentence_transformers import SentenceTransformer

def fetch_current_set_date_arguements() -> datetime:
    """
    Fetch date variables for the current set

    INPUT
    -----
        None

    OUTPUT
    ------
        start, end: Both are datetime values
    """
    end = datetime.datetime.now().date()
    start = end  - timedelta(days = 7)
    return start, end

def fetch_historical_set_date_arguements() -> datetime:
    """
    Fetch date variables for the Historical set

    INPUT
    -----
        None

    OUTPUT
    ------
        start, end: Both are datetime values
    """
    end = datetime.datetime.now().date() - timedelta(days = 8)
    start = end  - timedelta(days = 30)
    return start, end


def spawn_bigquery_client():
    """
    Function to create bigquery client

    INPUT
    -----
        None

    OUTPUT
    ------
        bigquery_client: Bigquery Client object to fetch data with
    """
    bigquery_client = bigquery.Client()
    return bigquery_client

def load_model()-> SentenceTransformer:
    """
    Function to load Sentence Transformers Model, to create Embeddings

    INPUT
    -----
        None

    OUTPUT
    ------
        Sentence Transformer model for embeddings creation
    """
    checkpoint = 'intfloat/multilingual-e5-base'
    model = SentenceTransformer(checkpoint)
    return model

def classify_SKW(search_keyword : str) -> str:
    """
    Function to categorize SKWs based on their comprising characters.
    SKWs can be purely alphabetical, numeric or alphanumerical.
    If the SKW, cannot be categorized due to limitations of this rule-based approach,
    The function returns an empty whitespace

    INPUT 
    -----
        search_keyword: Search Keyword to be categorized

    OUTPUT
    ------
        category: Category of the search keyword as described above
        either: is_numeric, is_alphabetic, is_alphanumeric, ' '
    """
    if bool(re.search("^[0-9:. ]+$", search_keyword)) == True:
        return 'is_numeric'
    if bool(re.search(r"^[a-zA-Z* ]+$", search_keyword)) == True:
        return 'is_alphabetic'
    if bool(re.search(r"^[a-zA-Z0-9:. ]+$", search_keyword)) == True:
        return 'is_alphanumeric'
    else:
        return ' '