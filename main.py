"""
Weighted Semantic Query Relaxation Module

Author: Oluwaseyi E. Ogunnowo
version: 0.0.1
date: 18/11/2023
"""

import os
import logging
import numpy as np
import pandas as pd

from sentence_transformers import SentenceTransformer
from dependency import (
    utils, process, queries
)
import sentence_transformers
print(pd.__version__, np.__version__, sentence_transformers.__version__)

logging.basicConfig(
    format='%(levelname)s %(asctime)s %(message)s', 
    level=logging.DEBUG
)

def relax_queries(
    embedding_model:SentenceTransformer, 
    historical_data:pd.DataFrame, 
    current_data:pd.DataFrame,
    N:int
) -> list:
    """
    Function to relax Search Keywords in the current set

    LOGIC OF RELAXATION
    -------------------

    This method to query relaxation is called Weighted Semantic Query Relaxation

    It works by finding search keywords in historical data, which are similar to the ones in the current set
    The Identified SKWs are further ranked by their historical CTR from highest to lowest. 

    Depending on the Number of SKWs needed (specified by the parameter N), the relaxed search keywords are returned

        An important point to note:
        ---------------------------
        The SKWs are only relaxed with historical SKWs within the same L1 and L2 category
    
    INPUT
    -----
        embedding model: Sentence Transformer pretrained model to be used for creating embeddings
        historical data: pandas DataFrame consisting of 1 month worth of SKWs, their historical CTR, L1 and L2 categories
        current  data: pandas DataFrame consisting of 1 week of SKWs to be relaxed
        N: integer determining the number of SKWs to output

    OUTPUT
    ------
        relaxed_queries: A list of relaxed queries  
    """
    relaxed_queries = []
    for index in range(len(current_data)):
        search_keyword_embedding = embedding_model.encode(
            current_data['normalised_skw'].iloc[index]
        )
        l2_category = current_data['l2_category'].iloc[index]
        historical_subset = historical_data.loc[historical_data['l2_category'] == l2_category]
        historical_subset['similarity_scores'] = np.dot(
            search_keyword_embedding,
            np.array(historical_subset['embeddings'].tolist()).T
        )
        historical_subset = historical_subset.loc[
            historical_subset['normalised_skw'] != current_data['normalised_skw'].iloc[index]
        ]
        historical_subset = historical_subset.sort_values(
            by = ['similarity_scores', 'CTR'],
            ascending = [False, False]
        ).reset_index(drop = True)
        if N == 1:
            relaxed_queries.append(historical_subset['normalised_skw'].iloc[0])
        elif N > 1:
            relaxed_queries.append(historical_subset['normalised_skw'].iloc[0:N+1].tolist())
    return relaxed_queries
        
    

def main():
    """
    The MAIN Function
    -----------------

    This function ties all parts together into one call: main()

    How it works
    ------------
    It follows these steps:
        1. Fetches the last week of data consisting of SKWs to relax
        2. Fetches a historical dataset consisting of one month of data
        3. Preprocesses the historical dataset
        4. Relaxes the SKWs in the current set, by comparing them in with those in the historical set (Please see docstring of function relaxed_queries())
        5. Returns output of step 4

    INPUT
    -----
        None

    OUTPUT
    ------
        pandas DataFrame containing the original SKWs as well as the relaxed ones
    """
    logging.info("run commenced")
    try:
        logging.info("Fetching historical data")
        curr_set_start_date, curr_set_end_date = utils.fetch_current_set_date_arguements()
        hist_set_start_date, hist_set_end_date = utils.fetch_historical_set_date_arguements()
        bigquery_client = utils.spawn_bigquery_client()
        embedding_model = utils.load_model()
        historical_data = process.preprocess_historical_data(
            data = process.fetch_data(
                                        bigquery_client = bigquery_client,
                                        query = queries.hist_set_query.format(
                                            hist_set_start_date, 
                                            hist_set_end_date
                                        )
                             ), 
            embedding_model = embedding_model
        )
        logging.info("Fetching current set of SKWs to relax")
        current_data = process.fetch_data(
            bigquery_client = bigquery_client,
            query = queries.curr_set_query.format(
                curr_set_start_date,
                curr_set_end_date
            )
        )
        current_data['status'] = current_data['normalised_skw'].apply(utils.classify_SKW)
        current_data = current_data.loc[current_data['status'] != 'is_numeric']
        logging.info("relaxing search keywords")
        current_data['relaxed_queries'] = relax_queries(
            embedding_model = embedding_model,
            historical_data = historical_data,
            current_data = current_data,
            N = 1
        )
        logging.info("Run completed successfully, with Search Keywords relaxed")
    except Exception as e:
        logging.error(f"Run Failed for this reason: {e}")
    return current_data

if __name__ == '__main__':
    print(main())
