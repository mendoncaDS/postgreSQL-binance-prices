
import os
import pytz
import logging
import datetime
import threading

import pandas as pd
import vectorbt as vbt
from run_bots import run_bots

from flask import Flask
from dotenv import load_dotenv
from sqlalchemy import create_engine, text




def unique_symbol_freqs(engine):
    with engine.connect() as connection:
        query = text(f"SELECT DISTINCT symbol FROM market_data")
        result  = connection.execute(query)
        unique_combinations = [row[0] for row in result.fetchall()]
    return unique_combinations


def update_database():

    load_dotenv()
    database_password = os.environ.get('password')
    server_address = os.environ.get('serverip')
    logging.info(f"Loaded dotenv values.")
    connection_string = f"postgresql://postgres:{database_password}@{server_address}:5432/postgres"
    global engine
    engine = create_engine(connection_string)
    logging.info(f"Connected to database.")

    all_data = []

    # Get the current timestamp at the beginning of the function
    end_timestamp = datetime.datetime.now(pytz.utc)

    symbol_freqs = unique_symbol_freqs(engine)

    # Fetch the last_timestamps for all symbol_freq combinations
    sql_query = text("""
    SELECT symbol, MAX(timestamp) as last_timestamp
    FROM market_data
    GROUP BY symbol;
    """)

    with engine.begin() as connection:
        result = connection.execute(sql_query)
        last_timestamps = {row.symbol: row.last_timestamp for row in result}
    
    print(symbol_freqs)

    for symbol in symbol_freqs:
        print(f"symbol: {symbol}")

        last_timestamp = last_timestamps.get((symbol))
        if last_timestamp:
            last_timestamp = pytz.utc.localize(last_timestamp)

        downloadedData = vbt.BinanceData.download(
            symbols=symbol,
            start=last_timestamp,
            end=end_timestamp,  # Using the end_timestamp here
            interval="1m"
        ).get(["Open", "High", "Low", "Close", "Volume"])

        if downloadedData.shape[0] > 2:
            downloadedData = downloadedData.iloc[1:-1]
            downloadedData.columns = ['open', 'high', 'low', 'close', 'volume']
            downloadedData['symbol'] = symbol
            downloadedData.index.name = 'timestamp'
        else:
            print("Data is up to date.")
            continue  # Skip to the next iteration

        #all_data.append(downloadedData)

        with engine.begin() as connection:
            downloadedData.to_sql('market_data', connection, if_exists='append', index=True)

    # Concatenate all the data frames
    #all_data_df = pd.concat(all_data)

    #with engine.begin() as connection:
        #all_data_df.to_sql('market_data', connection, if_exists='append', index=True)
