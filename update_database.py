
import os
import pytz
import logging
import threading

import pandas as pd
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
    database_password = os.environ.get('password')
    server_address = os.environ.get('serverip')
    logging.info(f"Loaded dotenv values.")

    connection_string = f"postgresql://postgres:{database_password}@{server_address}:5432/postgres"
    global engine
    engine = create_engine(connection_string)
    logging.info(f"Connected to database.")

    try:
        update_database()
    except Exception as e:
        logging.error(f"PYTHON ERROR: {e}")
    
    logging.info(f"Task finished.")
    #print("Task finished.")

def unique_symbol_freqs():
    with engine.connect() as connection:
        query = text(f"SELECT DISTINCT symbol, frequency FROM market_data")
        result  = connection.execute(query)
        unique_combinations = result.fetchall()
    return unique_combinations


def update_database():

    all_data = []

    symbol_freqs = unique_symbol_freqs()

    for symbol, freq in symbol_freqs:
        print(f"symbol: {symbol}, freq: {freq}")
        sql_query = text("""
        SELECT timestamp
        FROM market_data
        WHERE frequency = :freq AND symbol = :symbol
        ORDER BY timestamp DESC
        LIMIT 1;
        """)

        values = {'freq': freq, 'symbol': symbol}

        with engine.begin() as connection:
            result = connection.execute(sql_query, values)
            last_timestamp = result.scalar()
        last_timestamp = pytz.utc.localize(last_timestamp)

        downloadedData = vbt.BinanceData.download(
            symbols=symbol,
            start=last_timestamp,
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