import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from config import connection_string
from sqlalchemy import create_engine

import pandas as pd



hello = "Maciej, cieszę się, że znów robimy coś razem 😊"


def read_market_data_summary_1_24() -> pd.DataFrame:
    """
    Reads market_data_summary_1_24 and returns a DataFrame with the latest record for each symbol_id,
    and includes calculated percentage change and absolute delta for the last 1 hour and 24 hours.

    Returns:
    pd.DataFrame: DataFrame with the latest data and additional columns for performance metrics.
    """
    
    engine = create_engine(connection_string)
    
    try:
        with engine.connect() as connection:
            # Read the data from the materialized view
            query = "SELECT * FROM landing.market_data_summary_1_24 ORDER BY symbol_id, datetime ASC"
            data = pd.read_sql_query(query, connection)
        
        # Ensure the data is sorted by symbol_id and datetime ascending
        data.sort_values(by=['symbol_id', 'datetime'], ascending=[True, True], inplace=True)

        # Define the columns for which we need to calculate % change and delta
        columns_to_calculate = [
            'open', 'high', 'low', 'close', 'volume_24h', 'market_cap', 
            'circulating_supply', 'sentiment', 'contributors_active', 
            'contributors_created', 'posts_active', 'posts_created', 
            'interactions', 'social_dominance', 'galaxy_score', 
            'volatility', 'alt_rank', 'spam'
        ]
        
        # Calculate percentage change and delta for each column
        latest_data = data.groupby('symbol_id').last().reset_index()
        
        for col in columns_to_calculate:
            # Create shifted columns directly within the loop
            shift_1h = data.groupby('symbol_id')[col].shift(1)
            shift_24h = data.groupby('symbol_id')[col].shift(2)
            
            # Calculate percentage change and delta for 1 hour and 24 hours
            latest_data[f'{col}_r_1h'] = (latest_data[col] - shift_1h) / shift_1h * 100
            latest_data[f'{col}_d_1h'] = latest_data[col] - shift_1h
            
            latest_data[f'{col}_r_24h'] = (latest_data[col] - shift_24h) / shift_24h * 100
            latest_data[f'{col}_d_24h'] = latest_data[col] - shift_24h
        
        # Drop any intermediate columns if needed (in this case, no intermediate columns were created)
        
        return latest_data

    except Exception as e:
        print(f"Error reading market_data_summary_1_24 table: {e}")
        return pd.DataFrame()


def read_symbol_data(symbol_id) -> pd.DataFrame:
    """
    Reads full symbol_data 

    Returns:
    pd.DataFrame: DataFrame with full symbol data 
    """
    engine = create_engine(connection_string)
    try:
        with engine.connect() as connection:
                # Read the data from the materialized view
                query = f"""            
                    select 
                        ld.symbol_id,
                        s.symbol as symbol_ticker,
                        s.name as symbol_name,
                        ld.datetime,
                        ld.time_unix,
                        ld.open,
                        ld.high,
                        ld.low,
                        ld.close,
                        ld.volume_24h,
                        ld.market_cap,
                        ld.circulating_supply,
                        ld.sentiment,
                        ld.contributors_active,
                        ld.contributors_created,
                        ld.posts_active,
                        ld.posts_created,
                        ld.interactions,
                        ld.social_dominance,
                        ld.galaxy_score,
                        ld.volatility,
                        ld.alt_rank,
                        ld.spam
                    from landing.lunar_data ld
                    left join public.symbols s on ld.symbol_id = s.id
                    where ld.symbol_id = {symbol_id}
                    order by datetime asc
                    """
                data = pd.read_sql_query(query, connection)

        return data
    
    except Exception as e:
        print(f"Error read_symbol_data: {e}")
        return pd.DataFrame()





# Example usage:
#data = read_market_data_summary_1_24()
#print(data)  # Display the first few rows of the DataFrame
#print(data.columns)


#symbol_data = read_symbol_data(1)
#print(symbol_data.head())
