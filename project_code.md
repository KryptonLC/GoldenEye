# Project Folder Structure

```
./
    .gitignore
    Backoffice/
        check_request_limit.py
        config.py
        utils.py
        drafts/
            ETL_lunar.py
            get_lunar_data.py
            get_symbol_list.py
            landing_lunar.py
            update_symbol_list.py
        landing/
            landing_process.py
            lunar_data.py
            lunar_symbols.py
        loading/
            dash_data_functions.py
        staging/
            lunar_data_month.sql
            MV lunar_data_etl.sql
    Data/
        lunar_data_2024.sql
    Frontoffice/
        dash_app.py
        assets/
            styles.css

```

# Project Code

## .

### .gitignore

```python
# .gitignore

*__pycache__*
*.log

Backoffice/config.py
Backoffice/drafts
Data/~$lunar_Data.xlsx

```

## Backoffice

### check_request_limit.py

```python

```

## Backoffice

### config.py

```python
db_params = {
    "database" : "GoldenEye",
    "user" : "postgres",
    "password" : "admin",
    "host": "localhost",  # Add the host parameter if necessary
    "port": "5432"  # Add the port parameter if necessary
}
connection_string = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}"
goldeneye_db = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}"
lunarcrush_api_key = "gksfns8wdjl35uk987bavvolmlskmtghrxkw8icb"
    



lunar_key = {
    "key_outlook" : {"code" : "gksfns8wdjl35uk987bavvolmlskmtghrxkw8icb", "minute_usage" : 0, "daily_usage" : 0},
    "key_gmail" : {"code" : "gksfns8wdjl35uk987bavvolmlskmtghrxkw8icb", "minute_usage" : 0, "daily_usage" : 0},
}
```

## Backoffice

### utils.py

```python
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from config import connection_string

def register_api_request(service: str, key_name: str, function_name: str, url: str):
    """
    Register an API request in the database by logging the service, key_name, 
    function_name, url, and current timestamp.

    Args:
    service (str): The name of the service (e.g., "LunarCrush", "Binance").
    key_name (str): The name of the API key used.
    function_name (str): The name of the function that made the API call.
    url (str): The URL that was requested.

    Returns:
    None
    """
    try:
        # Create SQLAlchemy engine using the connection string
        engine = create_engine(connection_string)
        Session = sessionmaker(bind=engine)
        session = Session()

        # Ensure the session is connected to the public schema
        session.execute(text("SET search_path TO public"))

        # Insert the log entry into the database
        query = text("""
            INSERT INTO api_request_logs (service, key_name, timestamp, function_name, url)
            VALUES (:service, :key_name, :timestamp, :function_name, :url)
        """)
        session.execute(query, {
            'service': service,
            'key_name': key_name,
            'timestamp': datetime.now(),
            'function_name': function_name,
            'url': url
        })
        session.commit()
        
    except Exception as e:
        session.rollback()
        raise RuntimeError(f"Failed to register API request: {e}")
    finally:
        session.close()


def read_key_usage(default_keys=None) -> dict:
    """
    Reads the key usage information from the public.key_usage view for all keys.
    Initializes with default values if no records are found.

    Args:
    default_keys (list): A list of default key names to initialize with if no records are found.

    Returns:
    dict: A dictionary where each key is the key_name and the value is a dictionary
          containing the usage counts for the last minute, hour, and day.
    """
    if default_keys is None:
        default_keys = ["key_outlook"]

    try:
        # Create SQLAlchemy engine using the connection string
        engine = create_engine(connection_string)
        with engine.connect() as connection:
            # Query the view for all keys' usage data
            query = text("""
                SELECT 
                    key_name,
                    request_count_minute,
                    request_count_hour,
                    request_count_day
                FROM 
                    public.key_usage
            """)
            result = connection.execute(query).fetchall()

            # Initialize usage data with default keys and zero counts
            usage_data = {
                key: {"minute": 0, "hour": 0, "day": 0} for key in default_keys
            }

            # Update usage_data with actual data from the query result
            for row in result:
                usage_data[row[0]] = {  # Access the first column as row[0] which is key_name
                    "minute": row[1],   # Access the second column as row[1] which is request_count_minute
                    "hour": row[2],     # Access the third column as row[2] which is request_count_hour
                    "day": row[3]       # Access the fourth column as row[3] which is request_count_day
                }

            return usage_data

    except Exception as e:
        raise RuntimeError(f"Failed to read key usage: {e}")
    
import time
from sqlalchemy import create_engine, text
from config import connection_string

def dump_lunar_buffer():
    """
    Dumps the data from landing.buffer_lunar_data into landing.lunar_data
    and then truncates the buffer table. Measures and prints the execution time.
    Waits for confirmation that the transaction succeeded.
    """
    try:
        # Create SQLAlchemy engine using the connection string
        engine = create_engine(connection_string)
        
        start_time = time.time()
        
        with engine.connect() as connection:
            with connection.begin():  # Start a transaction block
                # Execute the insert and truncate operations
                query = text("""
                    INSERT INTO landing.lunar_data (symbol_id, datetime, time_unix, open, high, low, close, volume_24h, market_cap, 
                                                    circulating_supply, sentiment, contributors_active, contributors_created, 
                                                    posts_active, posts_created, interactions, social_dominance, galaxy_score, 
                                                    volatility, alt_rank, spam)
                    SELECT symbol_id, datetime, time_unix, open, high, low, close, volume_24h, market_cap, circulating_supply, 
                           sentiment, contributors_active, contributors_created, posts_active, posts_created, interactions, 
                           social_dominance, galaxy_score, volatility, alt_rank, spam
                    FROM landing.buffer_lunar_data;
                """)
                
                connection.execute(query)

                # Truncate the buffer table after the insert is successful
                connection.execute(text("TRUNCATE TABLE landing.buffer_lunar_data;"))
        
        elapsed_time = time.time() - start_time
        #print(f"🔄️ Dumping the buffer: {elapsed_time:.2f} sec")

    except Exception as e:
        print(f"Failed to dump lunar buffer: {e}")


```

## drafts

### ETL_lunar.py

```python
import time
import config
from datetime import datetime, timedelta
from get_lunar_data import get_lunar_data
from get_symbol_list import get_symbol_list

def main():
    # Get the list of symbols
    symbol_list = get_symbol_list()

    # Define the start and end dates for data retrieval
    
    

    # Define the API call interval in seconds (6 seconds to ensure no more than 10 requests per minute)
    api_call_interval = 6

    # Save the start time of the program
    start_time = datetime.now() + timedelta(hours=1)
    print(f"ETL process started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    # Initialize the counter for total iterations
    total_iterations = 0

    # Loop through the symbol list
    for index, row in symbol_list.iterrows():
        symbol_id = row['id']
        symbol_ticker = row['symbol']
        
        # Retry trick: continue while true
        while True:
            start_date = datetime(2019, 1, 1)
            end_date = datetime.now()
        
            iteration_start_time = time.time()
        
            # Request data from Lunar
            result = get_lunar_data(symbol_id, symbol_ticker, start_date, end_date)

            # Check the result of the function call
            if result == 1:
                #print(f"Data for {symbol_ticker} (ID {symbol_id}) retrieved successfully.")
                break
            elif result == 2:
                print(f"No data returned for {symbol_ticker} (ID {symbol_id}).")
                break
            elif result == 429:
                print(f"Error 429: Too Many Requests. Waiting for 1 hour...")
                time.sleep(3600)  # Wait for 1 hour
                continue  # Retry the same symbol after waiting
            elif result == 504:
                print(f"Error 504: Gateway Timeout. Server not responding. Waiting 10 seconds...")
                time.sleep(10)  # Wait for 10 seconds
                continue  # Retry the same symbol after waiting
            else:
                print(f"{symbol_ticker} | {symbol_id}) | Error: {result}")

            # Measure the end time of this iteration
            iteration_end_time = time.time()

            # Calculate the time taken for this iteration
            iteration_duration = iteration_end_time - iteration_start_time

            # If the iteration took less than the API call interval, wait the remaining time
            if iteration_duration < api_call_interval:
                time_to_wait = api_call_interval - iteration_duration + 1
                print(f"Waiting for {time_to_wait:.2f} seconds to maintain rate limit.")
                time.sleep(time_to_wait)
        
        # Increment the total iterations counter
        total_iterations += 1

    # Print the total number of iterations and the end time
    end_time = datetime.now()
    print(f"ETL process completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Total number of iterations: {total_iterations}")

if __name__ == "__main__":
    main()


```

## drafts

### get_lunar_data.py

```python
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


```

## drafts

### get_symbol_list.py

```python
import pandas as pd
from sqlalchemy import create_engine
import config

def get_symbol_list():
    # Connect to DB
    engine = create_engine(config.connection_string)

    with engine.connect() as connection:
        symbol_list_df = pd.read_sql_table('lunar_symbols',connection)
    
    return symbol_list_df

symbol_list = get_symbol_list()
print(symbol_list)
```

## drafts

### landing_lunar.py

```python

```

## drafts

### update_symbol_list.py

```python
import requests
import json
import pandas as pd
from config import db_params
from datetime import datetime
from sqlalchemy import create_engine

# Function to update LunarCrush data
def update_symbol_list():
    # Get list of symbols from LunarCrush API
    url = "https://lunarcrush.com/api4/public/coins/list/v1"
    headers = {
        'Authorization': 'Bearer gksfns8wdjl35uk987bavvolmlskmtghrxkw8icb'
    }

    response = requests.request("GET", url, headers=headers)
    data = json.loads(response.text.encode('utf8'))
    new_data_df = pd.DataFrame.from_dict(data['data'])

    # Database connection
    connection_string = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}"
    engine = create_engine(connection_string)

    # Load existing data from database
    with engine.connect() as connection:
        existing_data_df = pd.read_sql_table('lunar_symbols', connection)

    # Prepare new data for comparison
    new_data_df['last_update'] = datetime.now()
    new_data_df['status'] = 'Active'

    # Merge data
    merged_df = pd.merge(existing_data_df, new_data_df, on='id', how='outer', suffixes=('_old', ''))

    # Ensure old columns are combined properly
    merged_df['status'] = merged_df.apply(
        lambda row: 'New symbol' if pd.isnull(row['name_old']) else row['status_old'], axis=1)
    merged_df['status'] = merged_df.apply(
        lambda row: 'Decomissioned' if pd.isnull(row['name']) and not pd.isnull(row['name_old']) else row['status'], axis=1)
    merged_df['last_update'] = merged_df.apply(
        lambda row: datetime.now() if row['status'] in ['New symbol', 'Decomissioned'] else row['last_update_old'], axis=1)

    # Select the necessary columns
    final_df = merged_df[['id', 'name', 'symbol', 'topic', 'status', 'last_update']]

    # Save the updated data to the database
    final_df.to_sql('lunar_symbols', engine, if_exists='replace', index=False)

    print(final_df.info())

# Call the function

```

## landing

### landing_process.py

```python
import time
from datetime import datetime, timedelta, timezone
import pandas as pd

import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils import read_key_usage, dump_lunar_buffer
from config import lunar_key


from lunar_symbols import read_lunar_symbols
from lunar_data import get_lunar_data, save_lunar_data



def landing_process():

    key_usage = read_key_usage()

    time_pairs = [
        ("01.01.2016", "31.12.2017"), 
        ("01.01.2018", "31.12.2019"),
        ("01.01.2020", "31.12.2021"),
        ("01.01.2022", "31.12.2023"),
        ("01.01.2024", "31.12.2025")
    ]

    result_code, symbols_df = read_lunar_symbols()
    symbols_df = symbols_df.sort_values(by='symbol_id', ascending=True)
    
    symbols_df = symbols_df[symbols_df['include_etl'] == True]

    for index, row in symbols_df.iterrows():
        symbol_id = row['symbol_id']
        symbol_ticker = row['symbol_ticker']

        # Check key_usage limits
        if key_usage["key_outlook"]["minute"] >= 10:
            print("Minute limit reached. Waiting 5 seconds.")
            time.sleep(5)
            key_usage = read_key_usage()
            continue

        if key_usage["key_outlook"]["day"] >= 2000:
            print("Daily limit reached. Waiting 1 hour.")
            time.sleep(3600)
            key_usage = read_key_usage()
            continue

        # Loop through each pair of start_time and end_time
        for start_time, end_time in time_pairs:
            # Measure time for get_lunar_data
            start_time_get = time.time()
            result_code, data_df = get_lunar_data(symbol_id, start_time, end_time)
            time1 = time.time() - start_time_get
            len_data_df = len(data_df) if not data_df.empty else 0

            print(f"{symbol_id} | {symbol_ticker} | {start_time}:{end_time} | #{len_data_df} | {key_usage['key_outlook']['minute']}/10 & {key_usage['key_outlook']['day']}/2000")

            # Measure time for save_lunar_data
            start_time_save = time.time()
            if not data_df.empty:
                save_result_code, save_message = save_lunar_data(data_df)
            #    print(f"Data saved for {symbol_ticker} ({symbol_id}) from {start_time} to {end_time}: {save_message}")
            #else:
            #    print(f"No data returned for {symbol_ticker} ({symbol_id}) from {start_time} to {end_time}. Skipping save.")
            time2 = time.time() - start_time_save

            # Measure time for read_key_usage
            start_time_read = time.time()
            key_usage = read_key_usage()
            time3 = time.time() - start_time_read

            # Print timing information
            time_total = time1+time2+time3
            print(f"{symbol_id} | get: {time1:.3f} | save: {time2:.3f} | read: {time3:.3f} | total: {time_total:.3f}")
            
            if time_total < 45:
                time.sleep(45-time_total+1)


        # After processing all time pairs for the current symbol_id, dump the buffer
        dump_lunar_buffer()


def landing_process_etl():
    key_usage = read_key_usage()

    result_code, symbols_df = read_lunar_symbols()
    symbols_df = symbols_df.sort_values(by='last_update', ascending=True)
    symbols_df = symbols_df[symbols_df['include_etl'] == True]
    n = len(symbols_df)

    for i, (index, row) in enumerate(symbols_df.iterrows(), start=1):  # start=1 for 1-based index
        symbol_id = row['symbol_id']
        symbol_ticker = row['symbol_ticker']

        # Check key_usage limits
        if key_usage["key_outlook"]["minute"] >= 10:
            print("Minute limit reached. Waiting 5 seconds.")
            time.sleep(5)
            key_usage = read_key_usage()
            continue

        if key_usage["key_outlook"]["day"] >= 2000:
            print("Daily limit reached. Waiting 1 hour.")
            time.sleep(3600)
            key_usage = read_key_usage()
            continue

        # Get the last timestamp from the DataFrame
        last_timestamp = row['last_timestamp']
        start_time = datetime.fromtimestamp(last_timestamp).replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        end_time = datetime.now().replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)


        # Format the start_time and end_time as "%d.%m.%Y %H:%M" before passing to get_lunar_data
        start_time_str = start_time.strftime("%d.%m.%Y %H:%M")
        end_time_str = end_time.strftime("%d.%m.%Y %H:%M")

        # Measure time for get_lunar_data
        start_time_get = time.time()
        result_code, data_df = get_lunar_data(
            symbol_id, 
            start_time_str,  # Pass formatted string
            end_time_str  # Pass formatted string
        )
      
        time1 = time.time() - start_time_get
        len_data_df = len(data_df) if not data_df.empty else 0

        print(f"{i}/{n} | {symbol_id} | {symbol_ticker} | {start_time_str} - {end_time_str} | #{len_data_df} | {key_usage['key_outlook']['minute']}/10 & {key_usage['key_outlook']['day']}/2000")

        # Measure time for save_lunar_data
        start_time_save = time.time()
        if not data_df.empty:
            save_result_code, save_message = save_lunar_data(data_df)
        time2 = time.time() - start_time_save

        # Measure time for read_key_usage
        start_time_read = time.time()
        key_usage = read_key_usage()
        time3 = time.time() - start_time_read

        # Print timing information
        time_total = time1 + time2 + time3
        print(f"{symbol_id} | get: {time1:.3f} | save: {time2:.3f} | read: {time3:.3f} | total: {time_total:.3f}")

        dump_lunar_buffer()

        if time_total < 45:
            time.sleep(45 - time_total)

        

if __name__ == "__main__":
    try:
        while True:  # Continuous loop until manually stopped
            landing_process_etl()
    except KeyboardInterrupt:
        print("Landing process manually stopped.")
```

## landing

### lunar_data.py

```python
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

def get_lunar_data(symbol_id: int = 3, start_time: str = "01.01.2020 00:00", end_time: str = "19.08.2025 23:00") -> tuple[int, pd.DataFrame]:
    """
    Call LunarCrush API to receive symbol's hourly timeframe data.

    Args:
    symbol_id (int): The ID of the symbol.
    start_time (str): Start date in the format "dd.mm.yyyy hh:mm".
    end_time (str): End date in the format "dd.mm.yyyy hh:mm".

    Returns:
    Tuple[int, pd.DataFrame]: A tuple containing the result code and raw data as a pandas DataFrame.
    """
    # Combine date with default start and end times
    start_datetime = f"{start_time}"
    end_datetime = f"{end_time}"

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
            #print(f"No data returned from LunarCrush for symbol_id {symbol_id}.")
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
    

```

## landing

### lunar_symbols.py

```python
import requests
import json
import pandas as pd
from sqlalchemy import create_engine


import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config import lunar_key, connection_string
from utils import register_api_request

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
        register_api_request("LunarCrush", "key_outlook", "get_lunar_symbols", url)
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
            symbols_df = symbols_df.rename(columns={
                'id': 'symbol_id',
                'name': 'symbol_name',
                'symbol': 'symbol_ticker'
            })
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
```

## loading

### dash_data_functions.py

```python
# dash data preparation here
```

## staging

### lunar_data_month.sql

```python
/*
WITH windowed_data AS (
    SELECT
        symbol_id,
        extract(year from datetime) as year,
        extract(month from datetime) as month,

        first_value(open) over (partition by symbol_id, extract(year from datetime), extract(month from datetime) order by datetime asc) as price_open,
        last_value(close) over (partition by symbol_id, extract(year from datetime), extract(month from datetime) order by datetime asc) as price_close,

        last_value(market_cap) over (partition by symbol_id, extract(year from datetime), extract(month from datetime) order by datetime asc) as market_cap,
        last_value(circulating_supply) over (partition by symbol_id, extract(year from datetime), extract(month from datetime) order by datetime asc) as circulating_supply,

        first_value(posts_active) over (partition by symbol_id, extract(year from datetime), extract(month from datetime) order by datetime asc) as posts_active_open,
        last_value(posts_active) over (partition by symbol_id, extract(year from datetime), extract(month from datetime) order by datetime asc) as posts_active_close,

        first_value(posts_created) over (partition by symbol_id, extract(year from datetime), extract(month from datetime) order by datetime asc) as posts_created_open,
        last_value(posts_created) over (partition by symbol_id, extract(year from datetime), extract(month from datetime) order by datetime asc) as posts_created_close,

        first_value(interactions) over (partition by symbol_id, extract(year from datetime), extract(month from datetime) order by datetime asc) as interactions_open,
        last_value(interactions) over (partition by symbol_id, extract(year from datetime), extract(month from datetime) order by datetime asc) as interactions_close,

        high, low, volume_24h, volatility, sentiment, contributors_active, contributors_created,
        posts_active, posts_created, interactions, social_dominance, galaxy_score, alt_rank
    FROM landing.lunar_data
    WHERE symbol_id = 3
)
SELECT 
    symbol_id,
    year,
    month,
    price_open,
    MAX(high) as price_high,
    MIN(low) as price_low,
    price_close,

    SUM(volume_24h) as volume_24h,
    market_cap,
    circulating_supply,
    AVG(volatility) as volatility,

    MIN(sentiment) as sentiment_min,
    MAX(sentiment) as sentiment_max,
    MIN(contributors_active) as contributors_active_min,
    MAX(contributors_created) as contributors_active_max,

    posts_active_open,
    MAX(posts_active) as posts_active_high,
    MIN(posts_active) as posts_active_low,
    posts_active_close,

    posts_created_open,
    MAX(posts_created) as posts_created_high,
    MIN(posts_created) as posts_created_low,
    posts_created_close,

    interactions_open,
    MAX(interactions) as interactions_high,
    MIN(interactions) as interactions_low,
    interactions_close,

    MIN(social_dominance) as social_dominance_min,
    MAX(social_dominance) as social_dominance_max,
    MIN(galaxy_score) as galaxy_score_min,
    MAX(galaxy_score) as galaxy_score_max,
    MIN(alt_rank) as alt_rank_min,
    MAX(alt_rank) as alt_rank_max
FROM windowed_data
GROUP BY 
    symbol_id,
    year,
    month,
    price_open,
    price_close,
    market_cap,
    circulating_supply,
    posts_active_open,
    posts_active_close,
    posts_created_open,
    posts_created_close,
    interactions_open,
    interactions_close;


;
*/

with intervals as (
	SELECT 
	    start,
	    (start + interval '1 hour' - interval '1 second') AS end
	FROM generate_series(
	    DATE_TRUNC('hour', NOW()) - interval '24 hours', 
	    DATE_TRUNC('hour', NOW()), 
	    interval '1 hour'
	) AS start
)

	
select distinct
	a.start
	,a.end
    ,extract(year from a.end) as year
    ,extract(month from a.end) as month
	,extract(day from a.end) as day
	,b.symbol_id
	
	,first_value(b.open) over w as open
	,max(b.high) over w as high
	,min(b.low) over w as low
	,last_value(b.close) over w as close
	,avg(volume_24h) over w as volume_24h
	,avg(market_cap) over w as market_cap
	,avg(circulating_supply) over w as circulating_supply
	,avg(volatility) over w as volatility

	,avg(sentiment) over w as sentiment

	,first_value(contributors_active) over w as contributors_active_open
	,max(contributors_active) over w as contributors_active_high
	,min(contributors_active) over w as contributors_active_low
	,last_value(contributors_active) over w as contributors_active_close

	,first_value(contributors_created) over w as contributors_created_open
	,max(contributors_created) over w as contributors_created_high
	,min(contributors_created) over w as contributors_created_low
	,last_value(contributors_created) over w as contributors_created_close

	,first_value(posts_active) over w as posts_active_open
	,max(posts_active) over w as posts_active_high
	,min(posts_active) over w as posts_active_low
	,last_value(posts_active) over w as posts_active_close

	,first_value(posts_created) over w as posts_created_open
	,max(posts_created) over w as posts_created_high
	,min(posts_created) over w as posts_created_low
	,last_value(posts_created) over w as posts_created_close

	,first_value(interactions) over w as interactions_open
	,max(interactions) over w as interactions_high
	,min(interactions) over w as interactions_low
	,last_value(interactions) over w as interactions_close

	,first_value(social_dominance) over w as social_dominance_open
	,max(social_dominance) over w as social_dominance_high
	,min(social_dominance) over w as social_dominance_low
	,last_value(social_dominance) over w as social_dominance_close

	,avg(galaxy_score) over w as galaxy_score
	,avg(alt_rank) over w as alt_rank
	
from intervals a
inner join landing.lunar_data b on
	b.datetime >= a.start 
	and b.datetime < a.end
where b.symbol_id between 1 and 10
	
window w as (
	partition by b.symbol_id, a.start 
	order by b.symbol_id,b.datetime asc 
	rows between unbounded preceding and unbounded following
	)
order by b.symbol_id,a.start
```

## staging

### MV lunar_data_etl.sql

```python
CREATE MATERIALIZED VIEW landing.lunar_data_etl AS
WITH include_etl AS 
(
    SELECT id, name, symbol, last_update
    FROM public.symbols 
    WHERE include_etl = TRUE
)
SELECT 
    b.symbol,
    a.* 
FROM 
    landing.lunar_data a 
INNER JOIN 
    include_etl b 
ON 
    a.symbol_id = b.id
    AND a.datetime >= b.last_update - INTERVAL '72 hours'
ORDER BY 
    a.symbol_id ASC, 
    a.datetime ASC;


select * from landing.lunar_data_etl
refresh materialized view landing.lunar_data_etl
```

## Data

### lunar_data_2024.sql

```python
select 
	b.symbol,
	date(datetime) as date,
	TO_CHAR(datetime, 'HH24:MI:SS') AS time,
	a.*
from landing.lunar_data a
left join public.symbols b on a.symbol_id = b.id
where symbol_id in (1,2,3,10)
	and EXTRACT(YEAR FROM datetime) = 2024
```

## Frontoffice

### dash_app.py

```python
from dash import Dash, dcc, html, Output, Input
import dash_bootstrap_components as dbc

app = Dash(__name__, external_stylesheets=[dbc.themes.SOLAR])

# --------------------------------- Data ------------------------------------






# ---------------------------- Layout Elements ------------------------------------
header = dbc.Row(
    dbc.Col(
        html.H1("GoldenEye - Attention Trading", className="text-center text-primary"),
        className="bg-light py-2"
    ),
    className="header"
)

content = dbc.Row(
    [
        dbc.Col(html.Div("Market Stats", className="left-column bg-secondary text-white p-3"), width=4),
        dbc.Col(html.Div("Symbol Data", className="right-column bg-dark text-white p-3"), width=8),
    ],
    className="content-row"
)

app.layout = dbc.Container([header, content], fluid=True, className="dash-container")


# ---------------------------- Callbacks ------------------------------------




# ---------------------------- Run App ------------------------------------

if __name__ == '__main__':
    app.run_server(port=8000)

```

## assets

### styles.css

```python
/* Ensure the body and html take up the full height and have no margins */
html, body {
    height: 100%;
    margin: 0;
    padding: 0;
    overflow: hidden; /* Prevents scrolling and ensures the content fits the screen */
}

/* The main container should fill the full height of the viewport with no margins */
.dash-container {
    height: 100vh; /* Full height of the viewport */
    width: 100vw;  /* Full width of the viewport */
    display: flex;
    flex-direction: column;
    margin: 0; /* No margin around the container */
    padding: 0; /* No padding inside the container */
}

/* Header styling */
.header {
    height: 7vh; /* Adjust height of the header */
    display: flex;
    align-items: center;
    justify-content: center;
    margin: 0;
    flex-shrink: 0;
}

/* Content row should fill the remaining space */
.content-row {
    flex: 1;
    display: flex;
    margin: 0;
    padding: 0;
}

/* Left and right columns should take up full height and width */
.left-column, .right-column {
    height: 100%; /* Full height */
    display: flex;
    align-items: stretch;
    justify-content: stretch;
}

/* Additional utility classes for padding, no need to redefine colors */
.p-3 {
    padding: 1rem !important;
}

```

