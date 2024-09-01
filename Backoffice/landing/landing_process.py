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