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
    Reads the key usage information from the public.key_usage_summary view for all keys.
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
                    public.key_usage_summary
            """)
            result = connection.execute(query).fetchall()

            # Initialize usage data with default keys and zero counts
            usage_data = {
                key: {"minute": 0, "hour": 0, "day": 0} for key in default_keys
            }

            # Update usage_data with actual data from the query result
            for row in result:
                usage_data[row['key_name']] = {
                    "minute": row['request_count_minute'],
                    "hour": row['request_count_hour'],
                    "day": row['request_count_day']
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
    """
    try:
        # Create SQLAlchemy engine using the connection string
        engine = create_engine(connection_string)
        
        start_time = time.time()
        
        with engine.connect() as connection:
            # Execute the insert and truncate operations
            insert_query = text("""
                INSERT INTO landing.lunar_data (symbol_id, datetime, time_unix, open, high, low, close, volume_24h, market_cap, 
                                                circulating_supply, sentiment, contributors_active, contributors_created, 
                                                posts_active, posts_created, interactions, social_dominance, galaxy_score, 
                                                volatility, alt_rank, spam)
                SELECT symbol_id, datetime, time_unix, open, high, low, close, volume_24h, market_cap, circulating_supply, 
                       sentiment, contributors_active, contributors_created, posts_active, posts_created, interactions, 
                       social_dominance, galaxy_score, volatility, alt_rank, spam
                FROM landing.buffer_lunar_data;
            """)
            truncate_query = text("TRUNCATE TABLE landing.buffer_lunar_data;")
            
            connection.execute(insert_query)
            connection.execute(truncate_query)
        
        elapsed_time = time.time() - start_time
        print(f"Dumping the buffer: {elapsed_time:.2f} sec")

    except Exception as e:
        print(f"Failed to dump lunar buffer: {e}")

