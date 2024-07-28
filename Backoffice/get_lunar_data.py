import requests
import json
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text, TIMESTAMP
from sqlalchemy.orm import sessionmaker
import config
import time
import gc
import logging

# Configure logging
logging.basicConfig(filename='Backoffice/log_get_lunar_data.log', level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s')

def get_lunar_data(symbol_id: int, symbol_ticker: str, start_date: datetime, end_date: datetime) -> int:
    """
    Fetch data from LunarCrush API for a given symbol, save it to the database, and update the lunar_symbols table.
    
    Parameters:
    symbol_id (int): The ID of the symbol.
    symbol_ticker (str): The ticker symbol (e.g., "BTC").
    start_date (datetime): The start date for data retrieval.
    end_date (datetime): The end date for data retrieval.
    
    Returns:
    int: Status code indicating the result of the operation (1 for success, 2 for no data returned, other integers for various errors).
    """
    # Convert start_date and end_date to Unix timestamps
    start_unix = int(start_date.timestamp())
    end_unix = int(end_date.timestamp())
    
    # Construct the URL with the provided dates
    url = f"https://lunarcrush.com/api4/public/coins/{symbol_id}/time-series/v2?bucket=hour&interval=all&start={start_unix}&end={end_unix}"

    headers = {
        'Authorization': f"Bearer {config.lunarcrush_api_key}"
    }

    # Initialize session variable
    session = None

    try:
        # Measure time for requesting data
        start_time = time.time()
        response = requests.request("GET", url, headers=headers)
        request_time = time.time() - start_time

        # Error handling for different status codes
        if response.status_code == 200:
            try: 
                data = json.loads(response.text.encode('utf8'))
            except json.JSONDecodeError as e:
                error_message = f"JSONDecodeError: {e.msg} at line {e.lineno} column {e.colno} (char {e.pos})"
                log_message = f"{symbol_id} | {symbol_ticker} | {error_message}"
                logging.error(log_message)
                print(log_message)
                return 600  # Custom error code for JSON decode error

        else:
            error_messages = {
                400: "Error 400: Bad Request: The server could not understand the request.",
                401: "Error 401: Unauthorized: Access is denied due to invalid credentials.",
                403: "Error 403: Forbidden: You do not have permission to access this resource.",
                404: "Error 404: Not Found: The requested resource could not be found.",
                429: "Error 429: Too Many Requests: You have exceeded the rate limit.",
                500: "Error 500: Internal Server Error: The server encountered an error. Check symbol_id",
                504: "Error 504: Gateway Timeout. Server not responding."
            }
            error_message = error_messages.get(response.status_code, f"Unexpected Error: {response.status_code}")
            log_message = f"{symbol_id} | {symbol_ticker} | {error_message}"
            logging.error(log_message)
            print(log_message)
            return response.status_code

        # Check if 'data' key exists in the response and if data is not empty
        if 'data' not in data or len(data['data']) == 0:
            log_message = f"{symbol_id} | {symbol_ticker} | No data returned"
            logging.info(log_message)
            print(log_message)
            return 2  # No data returned

        # Measure time for converting to DataFrame
        start_time = time.time()
        data_df = pd.DataFrame(data['data'])
        conversion_time = time.time() - start_time

        # Add symbol_id, symbol_ticker, and datetime to DataFrame
        data_df.insert(0, 'symbol_id', symbol_id)
        data_df.insert(1, 'symbol_ticker', symbol_ticker)
        data_df.insert(2, 'datetime', pd.to_datetime(data_df['time'], unit='s'))  # Ensure 'datetime' is in timestamp format


        # Find the maximum timestamp in the data
        max_timestamp = int(data_df['time'].max())

        # Prepare the table name in lowercase
        schema_name = 'symbol_data'
        table_name = f"data_{symbol_id}_{symbol_ticker}".lower()

        # Database connection
        engine = create_engine(config.connection_string)
        Session = sessionmaker(bind=engine)
        session = Session()

        # Measure time for saving DataFrame to database
        start_time = time.time()
        data_df.to_sql(table_name, engine, schema=schema_name, if_exists='replace', index=False, dtype={'datetime': TIMESTAMP})

        saving_time = time.time() - start_time

        # Measure time for updating the lunar_symbols table
        last_update_time = datetime.now()
        update_query = text("""
        UPDATE public.lunar_symbols
        SET last_update = :last_update_time, last_timestamp = :max_timestamp
        WHERE id = :symbol_id;
        """)
        start_time = time.time()
        session.execute(update_query, {'last_update_time': last_update_time, 'max_timestamp': max_timestamp, 'symbol_id': symbol_id})
        session.commit()
        update_time = time.time() - start_time

        # Print confirmation message
        print(f"{symbol_id} | {symbol_ticker} | R:{request_time:.4f} C:{conversion_time:.4f} S:{saving_time:.4f} U:{update_time:.4f}")

        log_message = f"{symbol_id} | {symbol_ticker} | Success"
        logging.info(log_message)
        print(log_message)
        return 1  # Success
    
    except requests.exceptions.RequestException as e:
        error_message = f"Request Exception: {e}"
        log_message = f"{symbol_id} | {symbol_ticker} | {error_message}"
        logging.error(log_message)
        print(log_message)
        return 500
    finally:
        if session:
            session.close()
        # Explicitly delete the dataframe and force garbage collection
        if 'data_df' in locals():
            del data_df
        gc.collect()

