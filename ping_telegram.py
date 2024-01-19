
import os
import pytz
import telebot
import logging
import datetime

import pandas as pd

from dotenv import load_dotenv
from sqlalchemy import create_engine, text


def send_message(bot, message):
    bot.send_message(chat_id="@NGUSTRAT", text=message)

def ping_telegram():

    load_dotenv()
    database_password = os.environ.get('password')
    server_address = os.environ.get('serverip')
    telegramtoken = os.environ.get('telegramtoken')
    logging.info(f"Loaded dotenv values.")
    connection_string = f"postgresql://postgres:{database_password}@{server_address}:5432/postgres"
    global engine
    engine = create_engine(connection_string)
    logging.info(f"Connected to database.")
    bot = telebot.TeleBot(telegramtoken)

    end_timestamp = datetime.datetime.now(pytz.utc)
    start_timestamp = end_timestamp - datetime.timedelta(hours=2)

    sql_query = text("""
        SELECT *
        FROM bots
        WHERE timestamp >= :start_timestamp;
        """)

    with engine.begin() as connection:
        result = connection.execute(sql_query, {"start_timestamp": start_timestamp})

    result = pd.DataFrame(result.fetchall())


    for i in result["bot_name"].unique():
        last_positions = result[result["bot_name"]==i]["position"]
        if last_positions.iloc[1] != last_positions.iloc[0]:
            if last_positions.iloc[1]:
                msg = f"{i} just bought!"
            else:
                msg = f"{i} just sold!"
            send_message(bot, msg)

