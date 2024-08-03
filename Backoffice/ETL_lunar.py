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

