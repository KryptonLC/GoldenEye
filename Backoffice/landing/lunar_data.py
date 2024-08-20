import pandas as pd
import requests
import json
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config import lunar_key, connection_string
from utils import register_api_request
from lunar_symbols import read_lunar_symbols

def get_lunar_data(symbol_id: int = 3, start_time: str = "01.01.2020", end_time: str = "19.08.2024") -> tuple[int, pd.DataFrame]:
    """
    Call LunarCrush API to receive symbol's hourly timeframe data.

    Args:
    symbol_id (int): The ID of the symbol.
    start_time (str): Start date in the format "dd.mm.yyyy". Default is "01.01.2020".
    end_time (str): End date in the format "dd.mm.yyyy". Default is "19.08.2024".

    Returns:
    Tuple[int, pd.DataFrame]: A tuple containing the result code and raw data as a pandas DataFrame.
    """
    # Combine date with default start and end times
    start_datetime = f"{start_time} 00:00"
    end_datetime = f"{end_time} 23:00"

    # Convert to Unix timestamps
    start_unix = int(datetime.strptime(start_datetime, "%d.%m.%Y %H:%M").timestamp())
    end_unix = int(datetime.strptime(end_datetime, "%d.%m.%Y %H:%M").timestamp())

    key_code = lunar_key["key_outlook"]["code"]

    # Construct the API URL with the symbol_id and Unix timestamps
    url = f"https://lunarcrush.com/api4/public/coins/{symbol_id}/time-series/v2?bucket=hour&interval=all&start={start_unix}&end={end_unix}"
    headers = {
        'Authorization': f"Bearer {key_code}"
    }

    try:
        register_api_request("LunarCrush", "key_outlook", "get_lunar_data", url)
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        

    except requests.exceptions.HTTPError as e:
        print(f"HTTP Error: {e.response.status_code} - {e.response.reason}")
        return e.response.status_code, pd.DataFrame()
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return 9000, pd.DataFrame()

    try:
        data = response.json()
        if not data.get('data'):
            print(f"No data returned from LunarCrush for symbol_id {symbol_id}.")
            return 2, pd.DataFrame()
        data_df = pd.DataFrame(data['data'])
    except (json.JSONDecodeError, KeyError) as e:
        print(f"Error processing JSON data: {e}")
        return 9001, pd.DataFrame()

    # Rename 'time' column to 'time_unix'
    data_df.rename(columns={'time': 'time_unix'}, inplace=True)

    # Add 'symbol_id' column
    data_df['symbol_id'] = symbol_id

    # Convert 'time_unix' to 'datetime' and add as a new column
    data_df['datetime'] = pd.to_datetime(data_df['time_unix'], unit='s')

    # Define the final column order
    final_columns = [
        'symbol_id',
        'datetime',
        'time_unix',
        'open',
        'high',
        'low',
        'close',
        'volume_24h',
        'market_cap',
        'circulating_supply',
        'sentiment',
        'contributors_active',
        'contributors_created',
        'posts_active',
        'posts_created',
        'interactions',
        'social_dominance',
        'galaxy_score',
        'volatility',
        'alt_rank',
        'spam'
    ]

    # Add missing columns with None (NaN) values
    for col in final_columns:
        if col not in data_df.columns:
            data_df[col] = None

    # Reorder columns according to the final column order
    data_df = data_df[final_columns]

    return 1, data_df

# Example usage
#result, data_df = get_lunar_data(3, "01.01.2020", "19.08.2024")
#print(result)
#print(data_df.head())

def save_lunar_data(data_df: pd.DataFrame, table_name: str = 'buffer_lunar_data') -> tuple[int, str]:
    """
    Save the processed LunarCrush data DataFrame into the specified SQL table.

    Args:
    data_df (pd.DataFrame): The DataFrame containing LunarCrush data to be saved.
    table_name (str): The name of the SQL table to insert the data into. Default is 'landing.buffer_lunar_data'.

    Returns:
    Tuple[int, str]: A tuple containing the result code and the SQL server's message as a string.
    """
    try:
        # Create SQLAlchemy engine using the connection string
        engine = create_engine(connection_string)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # Save the DataFrame to the SQL table
        with engine.connect() as connection:
            data_df.to_sql(table_name, connection, schema='landing', if_exists='append', index=False)
        
        #update symbols table
        symbol_id = int(data_df['symbol_id'].max())
        max_time_unix = int(data_df['time_unix'].max())
        last_update_time = datetime.now()
        query = text("""
            UPDATE public.symbols
            SET last_update = :last_update_time, last_timestamp = :max_timestamp
            WHERE id = :symbol_id;
            """)
        
        session.execute(query, {'last_update_time': last_update_time, 'max_timestamp': max_time_unix, 'symbol_id': symbol_id})
        session.commit()

        # If the operation succeeds, return a success code and message
        return 1, "Data successfully inserted into the table."
    
        

    except Exception as e:
        # If an error occurs, return a failure code and the error message
        return 9005, str(e)
    
