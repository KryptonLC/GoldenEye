import requests
import json
import pandas as pd
from sqlalchemy import create_engine


import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config import lunar_key, connection_string


def get_lunar_symbols() -> tuple[int, pd.DataFrame] :
    """
    Call LunarCrush API to receive full list of available coins. 
    
    Args:
    None

    Returns:
    Tuple[int,pd.DataFrame]: A tuple containing the result code and raw data as a pandas DataFrame.
    """
    key_code = lunar_key["key_outlook"]["code"]

    url = "https://lunarcrush.com/api4/public/coins/list/v1"
    headers = {
        'Authorization': f'Bearer {key_code}'
    }

    try:
        response = requests.request("GET", url=url, headers=headers)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error: {e.response.status_code} - {e.response.reason}")
        return e.response.status_code, pd.DataFrame()
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return 9000, pd.DataFrame()
    
    data = json.loads(response.text.encode('utf-8'))
    symbols_list = pd.DataFrame.from_dict(data['data'])
    
    return 1, symbols_list


#result, data = get_lunar_symbols()
#print(result)
#print(data)

def read_lunar_symbols() -> tuple[int, pd.DataFrame]:
    """
    Reads table [symbols]. Returns tuple: result code, dataframe.
    
    Returns:
    Tuple[int,pd.DataFrame]: result code and full public.symbols table
    """
    engine = create_engine(connection_string)

    try:
        with engine.connect() as connection:
            symbols_df = pd.read_sql_table('symbols', connection)
        return 1, symbols_df
    except Exception as e:
        print(f"Error reading symbols table: {e}")
        return 9004, pd.DataFrame()

#result, data = read_lunar_symbols()
#print(result)
#print(data)


def save_lunar_symbols(symbols_df) -> tuple[int,str]:
    """
    Save the symbols data frame into the 'symbols' table.
    
    Args:
    symbols_list (pd.DataFrame): DataFrame containing symbols data to be saved.
    
    Returns:
    Tuple[int, str]: result code and message indicating the outcome.
    """

    symbols_df_short = symbols_df[['id','name','symbol','topic']]

    # read sql
    result_sql, symbols_sql = read_lunar_symbols()
    if result_sql != 1:
        return result_sql, "Failed to read existing symbols from the database."


    # Find new symbols that are not in the existing database
    symbols_to_insert = symbols_df_short[~symbols_df_short['id'].isin(symbols_sql['id'])]
    symbols_to_insert['status'] = "New symbol" 
    symbols_to_insert['last_update'] = pd.Timestamp.now()
    symbols_to_insert['last_timestamp'] = pd.Timestamp.now().timestamp()
    
    if symbols_to_insert.empty:
        return 1, 'No new symbols to insert.'

    try:
        # Insert the new symbols into the SQL database
        engine = create_engine(connection_string)
        with engine.connect() as connection:
            symbols_to_insert.to_sql('symbols', connection, if_exists='append', index=False)
        return 1, f"Inserted {len(symbols_to_insert)} new symbols into the database."
    except Exception as e:
        return 9003, f"Failed to insert new symbols: {e}"

#result1, symbols_df = get_lunar_symbols()
#result2, msg = save_lunar_symbols(symbols_df)
#print(result1)
#print(symbols_df)
#print(result2)
#print(msg)