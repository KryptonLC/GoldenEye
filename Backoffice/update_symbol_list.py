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
