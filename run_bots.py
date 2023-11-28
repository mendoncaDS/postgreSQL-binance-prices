
import os
import json
import pytz
import logging
import requests

import pandas as pd
import pandas_ta as ta

from dotenv import load_dotenv
from datetime import datetime, timedelta
from sqlalchemy import create_engine, text

def run_bots():

    load_dotenv()
    database_password = os.environ.get('password')
    server_address = os.environ.get('serverip')
    github_token = os.environ.get('githubtoken')
    connection_string = f"postgresql://postgres:{database_password}@{server_address}:5432/postgres"
    engine = create_engine(connection_string, echo=False)

    data_dict = {}

    def load_symbol_data(symbol, begin_date):
        print("Starting the data loading process...")

        # check if data_dict contains the symbol

        if symbol in data_dict:
            return data_dict[symbol]

        # Define the earliest date we want data from if no local data is available
        initial_date = pd.Timestamp(begin_date).tz_localize('UTC')

        # Define the timestamp for tomorrow at 1 AM
        tomorrow_1_am = pd.Timestamp(datetime.now().replace(hour=1, minute=0, second=0, microsecond=0) + timedelta(days=1)).tz_localize('UTC')

        # Step 2: Check for the most recent data available in the database
        with engine.connect() as connection:
            fetch_data_query = text("""
                SELECT timestamp, open, high, low, close, volume 
                FROM market_data 
                WHERE symbol = :symbol AND frequency = '1m' AND timestamp >= :data_to_fetch_from AND timestamp < :tomorrow_1_am
            """)

            new_data = pd.read_sql_query(fetch_data_query, connection, params={"symbol": symbol, "data_to_fetch_from": initial_date, "tomorrow_1_am": tomorrow_1_am})

        # Step 4: Process the fetched data
    
        if not new_data.empty:
            new_data.set_index('timestamp', inplace=True)
            new_data.index = new_data.index.tz_localize('UTC')  # Ensure timestamps are timezone-aware

            if not new_data.index.is_monotonic_increasing:
                new_data.sort_index(inplace=True)
            
            final_df = new_data
            data_dict[symbol] = final_df

            return final_df

        else:
            print("No new data fetched.")
            return None




    headers = {
        'Authorization': f'token {github_token}',
        'Accept': 'application/vnd.github.v3.raw'
    }

    zurfer_bots_config_url = 'https://api.github.com/repos/mendoncaDS/zlema_bots_config/contents/bots_config.json?ref=main'

    zurfer_bots_response = requests.get(zurfer_bots_config_url, headers=headers)
    if zurfer_bots_response.ok:
        zurfer_bots_list = json.loads(zurfer_bots_response.text)
    else:
        # Handle errors here
        print('Could not fetch the file:', zurfer_bots_response.status_code)




    def get_zurfer_bots_data(bot):

        begin_date = bot["bt_begin_date"]
        actual_begin_date = pd.Timestamp(begin_date) - pd.Timedelta(hours=4*max(bot["first_zlema_period"], bot["second_zlema_period"]))
        data = load_symbol_data(bot["symbol"], actual_begin_date)

        bt_data = data[["open","close"]].copy()
        all_dates = pd.date_range(start=bt_data.index.min(), end=bt_data.index.max(), freq="1T")
        bt_data = bt_data.reindex(all_dates, method='ffill')

        hourly_bt_data = bt_data.resample("1H").agg({"open": "first", "close": "last"})
        hourly_bt_data["first_zlema"] = ta.zlma(hourly_bt_data["close"], length=bot["first_zlema_period"])
        hourly_bt_data["second_zlema"] = ta.zlma(hourly_bt_data["close"], length=bot["second_zlema_period"])
        hourly_bt_data = hourly_bt_data.dropna()
        
        if bot["condition"] == 0:
            hourly_bt_data["position"] = (hourly_bt_data["close"] < hourly_bt_data["first_zlema"]) & (hourly_bt_data["close"] < hourly_bt_data["second_zlema"])
        elif bot["condition"] == 1:
            hourly_bt_data["position"] = (hourly_bt_data["close"] < hourly_bt_data["first_zlema"]) & (hourly_bt_data["close"] > hourly_bt_data["second_zlema"])
        elif bot["condition"] == 2:
            hourly_bt_data["position"] = (hourly_bt_data["close"] > hourly_bt_data["first_zlema"]) & (hourly_bt_data["close"] > hourly_bt_data["second_zlema"])
        
        hourly_bt_data["position"] = hourly_bt_data["position"].shift(1)
        hourly_bt_data = hourly_bt_data.dropna()
        hourly_bt_data["position"] = hourly_bt_data["position"].astype(int)

        processed_data = hourly_bt_data[["position"]]
        processed_data = processed_data[begin_date:]
        
        processed_data["bot_name"] = bot["bot_name"]
        processed_data["symbol"] = bot["symbol"]
        processed_data.index.name = "timestamp"

        processed_data.reset_index(inplace=True)
        
        return processed_data



    for zurfer_bot in zurfer_bots_list:

        bot_data = get_zurfer_bots_data(zurfer_bot)

        sql_query = text(
            f"SELECT MAX(timestamp) as most_recent_timestamp "
            f"FROM bots "
            f"WHERE bot_name = :bot_name AND symbol = :symbol"
        )

        with engine.connect() as connection:
            result = connection.execute(sql_query, {"bot_name": zurfer_bot["bot_name"], "symbol": zurfer_bot["symbol"]})
            most_recent_timestamp = result.scalar()

        if most_recent_timestamp is not None:
            most_recent_timestamp = most_recent_timestamp.replace(tzinfo=pytz.utc)
            print(f"most_recent_timestamp: {most_recent_timestamp}")
            bot_data = bot_data[bot_data["timestamp"] > most_recent_timestamp]
        
        if not bot_data.empty:
            bot_data.to_sql('bots', con=engine, if_exists='append', index=False)



if __name__ == "__main__":
    run_bots()

