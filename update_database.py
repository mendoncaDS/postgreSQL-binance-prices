
import os
import pytz
import logging
import datetime
import threading

import pandas as pd
import vectorbt as vbt

from flask import Flask
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

from run_bots import run_bots

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
    logging.info(f"Starting Routine.")

    load_dotenv()
    database_password = os.environ.get('password')
    server_address = os.environ.get('serverip')
    logging.info(f"Loaded dotenv values.")

    connection_string = f"postgresql://postgres:{database_password}@{server_address}:5432/postgres"
    global engine
    engine = create_engine(connection_string)
    logging.info(f"Connected to database.")

    try:
        logging.info(f"Updating database...")
        update_database()
        logging.info(f"Updated database successfully.")
        logging.info(f"----------")
    except Exception as e:
        logging.error(f"An error occurred while updating database: {e}")

    try:
        logging.info(f"Updating bots...")
        run_bots()
        logging.info(f"Updated bots successfully.")
        logging.info(f"----------")
    except Exception as e:
        logging.error(f"An error occurred while updating bots: {e}")

    logging.info(f"Routine Finished.")



def unique_symbol_freqs():
    with engine.connect() as connection:
        query = text(f"SELECT DISTINCT symbol, frequency FROM market_data")
        result  = connection.execute(query)
        unique_combinations = result.fetchall()
    return unique_combinations

def update_database():
    all_data = []

    # Get the current timestamp at the beginning of the function
    end_timestamp = datetime.datetime.now(pytz.utc)

    symbol_freqs = unique_symbol_freqs()

    # Fetch the last_timestamps for all symbol_freq combinations
    sql_query = text("""
    SELECT symbol, frequency, MAX(timestamp) as last_timestamp
    FROM market_data
    GROUP BY symbol, frequency;
    """)

    with engine.begin() as connection:
        result = connection.execute(sql_query)
        last_timestamps = {(row.symbol, row.frequency): row.last_timestamp for row in result}

    for symbol, freq in symbol_freqs:
        print(f"symbol: {symbol}, freq: {freq}")

        last_timestamp = last_timestamps.get((symbol, freq))
        if last_timestamp:
            last_timestamp = pytz.utc.localize(last_timestamp)

        downloadedData = vbt.BinanceData.download(
            symbols=symbol,
            start=last_timestamp,
            end=end_timestamp,  # Using the end_timestamp here
            interval=freq
        ).get(["Open", "High", "Low", "Close", "Volume"])

        if downloadedData.shape[0] > 2:
            downloadedData = downloadedData.iloc[1:-1]
            downloadedData.columns = ['open', 'high', 'low', 'close', 'volume']
            downloadedData['symbol'] = symbol
            downloadedData['frequency'] = freq
            downloadedData.index.name = 'timestamp'
        else:
            print("Data is up to date.")
            continue  # Skip to the next iteration

        all_data.append(downloadedData)

    # Concatenate all the data frames
    all_data_df = pd.concat(all_data)

    with engine.begin() as connection:
        all_data_df.to_sql('market_data', connection, if_exists='append', index=True)

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8080)