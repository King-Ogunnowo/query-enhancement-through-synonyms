import pandas as pd
from dependency import utils
from sentence_transformers import SentenceTransformer

def fetch_data(bigquery_client, query:str) -> pd.DataFrame:
    """
    Function to fetch data according to input query

    INPUT
    -----
        bigquery_client: Bigquery client
        query: SQL Query in form of string

    OUTPUT
    ------
        data: pandas dataframe object containing requested data
    """
    data = bigquery_client.query(query).to_dataframe()
    return data

def preprocess_historical_data(data:pd.DataFrame, embedding_model:SentenceTransformer) -> pd.DataFrame:
    """
    Function to preprocess the historical dataset.
    Preprocessing steps include:
        - identify the alphabetic and alphanumeric SKWs (the numeric ones are not needed for now)
        - Vectorize the SKWs in the result of the previous step

    INPUT
    -----
        data: Pandas Dataframe containing the historial set

    OUTPUT
    ------
        data Pandas DataFrame containing preprocessed data
    """
    data['status'] = data['normalised_skw'].apply(lambda x: utils.classify_SKW(x))
    preprocessed_data = data.loc[data['status'] != 'is_numeric'].reset_index(drop = True)
    preprocessed_data['embeddings'] = list(embedding_model.encode(preprocessed_data['normalised_skw'].tolist()))
    return preprocessed_data