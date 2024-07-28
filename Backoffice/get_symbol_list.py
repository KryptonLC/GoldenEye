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