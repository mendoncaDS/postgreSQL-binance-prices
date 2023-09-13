
import os
import pytz
import logging
import threading

import vectorbt as vbt

from flask import Flask
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

app = Flask(__name__)

logging.basicConfig(level=logging.INFO)

def run_in_background():
    # This will run your main function in the background
    main()

@app.route('/')
def home():
    # Trigger your main task in a separate thread
    threading.Thread(target=run_in_background).start()
    return "Task started", 200


def main():
    logging.info(f"-----*----------*----------*-----")
    logging.info(f"Starting task.")
    load_dotenv()
    database_password = os.environ.get('database_password')
    server_address = os.environ.get('server_address')
    logging.info(f"Loaded dotenv values.")
    connection_string = f"postgresql://postgres:{database_password}@{server_address}:5432/postgres"
    global engine
    engine = create_engine(connection_string)
    logging.info(f"Connected to database.")

    try:
        
        symbol_freqs = unique_symbol_freqs()
        logging.info(f"Got the symbols-freqs: {symbol_freqs}.")

    except Exception as e:
        logging.error(f"PYTHON ERROR: {e}")


    for symbol_freq in symbol_freqs:
        logging.info(f"Trying to update {symbol_freq[0]}, {symbol_freq[1]}.")
        try:
            update_database(symbol_freq[0], symbol_freq[1])
        except Exception as e:
            logging.error(f"PYTHON ERROR: {e}")
    
    logging.info(f"Task finished.")
    #print("Task finished.")

def check_symbol_freq(symbol, freq):
    
    sql_query = text("SELECT COUNT(*) FROM market_data WHERE symbol = :symbol AND frequency = :freq")
    
    # Using engine.connect()
    with engine.begin() as connection:
        result = connection.execute(sql_query, {'symbol': symbol,'freq': freq})
        count = result.scalar()  # Get the first column of the first row (should be the COUNT)
    
    if count==0:
        return False
    else:
        return True

def unique_symbol_freqs():
    with engine.connect() as connection:
        query = text(f"SELECT DISTINCT symbol, frequency FROM market_data")
        result  = connection.execute(query)
        unique_combinations = result.fetchall()
    return unique_combinations

def write_to_database(data, symbol, freq):

    dataIn = data.copy()

    dataIn.columns = ['open', 'high', 'low', 'close', 'volume']

    dataIn['frequency'] = freq
    dataIn['symbol'] = symbol

    dataIn.index.name = 'timestamp'
    
    with engine.begin() as connection:
        dataIn.to_sql('market_data', connection, if_exists='append', index=True)
      
def update_database(symbol, freq):

    if not check_symbol_freq(symbol, freq):
        print("New Symbol and Frequency")
        downloadedData = vbt.BinanceData.download(
            symbols = symbol,
            interval = freq
            ).get(["Open","High","Low","Close","Volume"])
        
        downloadedData = downloadedData.iloc[:-1]
        write_to_database(downloadedData, symbol, freq)
    else:
        print("Existing Symbol and Frequency")
        sql_query = text("""
        SELECT timestamp
        FROM market_data
        WHERE frequency = :freq AND symbol = :symbol
        ORDER BY timestamp DESC
        LIMIT 1;
        """)

        values = {'freq': freq, 'symbol': symbol}

        # Execute the query and fetch the result
        with engine.begin() as connection:
            result = connection.execute(sql_query, values)
            last_timestamp = result.scalar()
        last_timestamp = pytz.utc.localize(last_timestamp)
        

        downloadedData = vbt.BinanceData.download(
            symbols = symbol,
            start = last_timestamp,
            interval = freq
            ).get(["Open","High","Low","Close","Volume"])

        if downloadedData.shape[0] > 2:
            downloadedData = downloadedData.iloc[1:-1]
            write_to_database(downloadedData, symbol, freq)
        else:
            print("Data is up to date.")

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8080)