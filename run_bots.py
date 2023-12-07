
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

        # Define the timestamp for tomorrow at 1 AM
        tomorrow_1_am = pd.Timestamp(datetime.now().replace(hour=1, minute=0, second=0, microsecond=0) + timedelta(days=1)).tz_localize('UTC')

        # Determine the date range for fetching data
        if symbol not in data_dict:
            # If no data is available, fetch from begin_date to tomorrow 1 AM
            data_to_fetch_from = pd.Timestamp(begin_date).tz_localize('UTC')
            fetch_up_to = tomorrow_1_am
        else:
            if data_dict[symbol].index.min() <= pd.Timestamp(begin_date).tz_localize('UTC'):
                return data_dict[symbol]
            # If data is available but not enough, fetch from begin_date to the oldest data available
            data_to_fetch_from = pd.Timestamp(begin_date).tz_localize('UTC')
            fetch_up_to = data_dict[symbol].index.min()

        # Fetch data from the database
        with engine.connect() as connection:
            fetch_data_query = text("""
                SELECT timestamp, open, close 
                FROM market_data 
                WHERE symbol = :symbol AND timestamp >= :data_to_fetch_from AND timestamp < :fetch_up_to
            """)

            new_data = pd.read_sql_query(fetch_data_query, connection, params={"symbol": symbol, "data_to_fetch_from": data_to_fetch_from, "fetch_up_to": fetch_up_to})

        # Process the fetched data
        if not new_data.empty:
            new_data.set_index('timestamp', inplace=True)
            new_data.index = new_data.index.tz_localize('UTC')  # Ensure timestamps are timezone-aware

            # Combine new data with existing data if available and remove duplicates
            final_df = pd.concat([data_dict.get(symbol, pd.DataFrame()), new_data])

            if not final_df.index.is_monotonic_increasing:
                final_df.sort_index(inplace=True)

            data_dict[symbol] = final_df

            return final_df
        else:
            print("No new data fetched.")
            return data_dict.get(symbol, None)


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


    def get_zzurfer_bots_data(bot, data):
        zurfer_bot_1 = {
            "first_zlema_period":bot["first_zlema_period"],
            "second_zlema_period":bot["second_zlema_period"],
            "condition":bot["condition"],
        }
        zurfer_1 = get_zurfer_bots_data(zurfer_bot_1, data)

        zurfer_bot_2 = {
            "first_zlema_period":bot["first_zlema_period_2"],
            "second_zlema_period":bot["second_zlema_period_2"],
            "condition":bot["condition_2"],
        }
        zurfer_2 = get_zurfer_bots_data(zurfer_bot_2, data)

        zzurfer = pd.DataFrame()

        zzurfer["position"] = zurfer_1["position"] | zurfer_2["position"]
        zzurfer["open"] = zurfer_1["open"]

        return zzurfer

    def get_zurfer_bots_data(bot, data):
    
        bt_data = data[["open","close"]].copy()
        all_dates = pd.date_range(start=bt_data.index.min(), end=bt_data.index.max(), freq="1T")
        bt_data = bt_data.reindex(all_dates, method='ffill')

        hourly_bt_data = bt_data.resample("1H").agg({"open": "first", "close": "last"})
        hourly_bt_data["first_zlema"] = ta.zlma(hourly_bt_data["close"], length=bot["first_zlema_period"])
        hourly_bt_data["second_zlema"] = ta.zlma(hourly_bt_data["close"], length=bot["second_zlema_period"])
        hourly_bt_data = hourly_bt_data.dropna()
        
        # Individual condition vectors
        price_above_first_zlema = hourly_bt_data["close"] > hourly_bt_data["first_zlema"]
        price_above_second_zlema = hourly_bt_data["close"] > hourly_bt_data["second_zlema"]
        first_zlema_above_price = hourly_bt_data["first_zlema"] > hourly_bt_data["close"]
        second_zlema_above_price = hourly_bt_data["second_zlema"] > hourly_bt_data["close"]
        second_zlema_above_first = hourly_bt_data["second_zlema"] > hourly_bt_data["first_zlema"]
        first_zlema_above_second = hourly_bt_data["first_zlema"] > hourly_bt_data["second_zlema"]

        # Combined conditions
        if bot["condition"] == 0:
            hourly_bt_data["position"] = first_zlema_above_price & second_zlema_above_price
        elif bot["condition"] == 1:
            hourly_bt_data["position"] = price_above_first_zlema & price_above_second_zlema
        elif bot["condition"] == 2:  # P, 1, 2
            hourly_bt_data["position"] = first_zlema_above_price & second_zlema_above_first
        elif bot["condition"] == 3:  # 1, P, 2
            hourly_bt_data["position"] = price_above_first_zlema & second_zlema_above_price
        elif bot["condition"] == 4:  # 1, 2, P
            hourly_bt_data["position"] = price_above_second_zlema & second_zlema_above_first
        elif bot["condition"] == 5:  # P, 2, 1
            hourly_bt_data["position"] = second_zlema_above_price & first_zlema_above_second
        elif bot["condition"] == 6:  # 2, P, 1
            hourly_bt_data["position"] = price_above_second_zlema & first_zlema_above_price
        elif bot["condition"] == 7:  # 2, 1, P
            hourly_bt_data["position"] = price_above_first_zlema & first_zlema_above_second
        elif bot["condition"] == 8:  # 1, 2 (No Price Position Specified)
            hourly_bt_data["position"] = second_zlema_above_first
        elif bot["condition"] == 9:  # 2, 1 (No Price Position Specified)
            hourly_bt_data["position"] = first_zlema_above_second

        hourly_bt_data["position"] = hourly_bt_data["position"].shift(1).astype("bool")

        return hourly_bt_data[["open","position"]]

    bot_data_to_push = pd.DataFrame()

    for zurfer_bot in zurfer_bots_list:
        logging.info(f"Processing {zurfer_bot['bot_name']}")
        print(f"Processing {zurfer_bot['bot_name']}")
        # Check the database for the most recent timestamp
        sql_query = text(
            f"SELECT MAX(timestamp) as most_recent_timestamp "
            f"FROM bots "
            f"WHERE bot_name = :bot_name AND symbol = :symbol"
        )

        with engine.connect() as connection:
            result = connection.execute(sql_query, {"bot_name": zurfer_bot["bot_name"], "symbol": zurfer_bot["symbol"]})
            most_recent_timestamp = result.scalar()

        # Calculate actual_begin_date based on the most recent timestamp
        if most_recent_timestamp is not None:
            datetime_hourly_comparison = datetime.utcnow().strftime("%Y-%m-%d %H:00:00")
            datetime_hourly_comparison = datetime.strptime(datetime_hourly_comparison, "%Y-%m-%d %H:00:00")

            # check if bot's most recent timestamp (in hour precision) is equal to the current utc-0 timezone timestamp in hour precision
            if most_recent_timestamp == datetime_hourly_comparison:

                logging.info(f"Bot {zurfer_bot['bot_name']} is up to date")
                print(f"Bot {zurfer_bot['bot_name']} is up to date")
                continue
            else:
                logging.info(f"Bot {zurfer_bot['bot_name']} is not up to date")
                print(f"Bot {zurfer_bot['bot_name']} is not up to date")
                actual_begin_date = most_recent_timestamp - pd.Timedelta(hours=4*max(zurfer_bot["first_zlema_period"], zurfer_bot["second_zlema_period"]))

            actual_begin_date = most_recent_timestamp - pd.Timedelta(hours=4*max(zurfer_bot["first_zlema_period"], zurfer_bot["second_zlema_period"]))
        else:
            actual_begin_date = pd.Timestamp(zurfer_bot["bt_begin_date"]) - pd.Timedelta(hours=4*max(zurfer_bot["first_zlema_period"], zurfer_bot["second_zlema_period"]))

        # Load and filter symbol data
        data = load_symbol_data(zurfer_bot["symbol"], actual_begin_date)

        # Process the data
        if zurfer_bot["strategy"] == "zurfer":
            msg = "Processing zurfer strategy"
            logging.info(msg)
            print(msg)
            bot_data = get_zurfer_bots_data(zurfer_bot, data)
        if zurfer_bot["strategy"] == "zzurfer":
            msg = "Processing zzurfer strategy"
            logging.info(msg)
            print(msg)
            bot_data = get_zzurfer_bots_data(zurfer_bot, data)

        bot_data = bot_data.dropna()
        bot_data["position"] = bot_data["position"].astype(int)

        bot_data = bot_data[["position"]].copy()
        
        bot_data["bot_name"] = zurfer_bot["bot_name"]
        bot_data["symbol"] = zurfer_bot["symbol"]
        bot_data.index.name = "timestamp"

        bot_data.reset_index(inplace=True)

        if most_recent_timestamp is not None:
            most_recent_timestamp = pytz.utc.localize(most_recent_timestamp)
            bot_data = bot_data[bot_data["timestamp"] > most_recent_timestamp]
        
        bot_data_to_push = pd.concat([bot_data_to_push, bot_data], ignore_index=True)

    # Update database if there is new data
    if not bot_data_to_push.empty:
        bot_data_to_push.to_sql('bots', con=engine, if_exists='append', index=False)



if __name__ == "__main__":
    run_bots()

