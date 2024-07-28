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