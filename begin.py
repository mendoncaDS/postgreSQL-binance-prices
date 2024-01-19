
import logging
from flask import Flask
from dotenv import load_dotenv
import threading
import os
from sqlalchemy import create_engine
from run_bots import run_bots
from update_database import update_database
from ping_telegram import ping_telegram


app = Flask(__name__)

logging.basicConfig(level=logging.INFO)


@app.route('/')
def home():
    # Trigger your main task in a separate thread
    threading.Thread(target=main).start()
    return "Task started", 200


def main():

    logging.info(f"-----*----------*----------*-----")
    logging.info(f"Starting task.")



    try:
        logging.info(f"Trying to update database...")
        update_database()
    except Exception as e:
        logging.error(f"PYTHON ERROR: {e}")
    
    logging.info(f"Database updated.")



    try:
        logging.info(f"Trying to update bots...")
        run_bots()
    except Exception as e:
        logging.error(f"PYTHON ERROR: {e}")
    
    logging.info(f"Bots updated.")



    try:
        logging.info(f"Trying ping telegram...")
        ping_telegram()
    except Exception as e:
        logging.error(f"PYTHON ERROR: {e}")
    
    logging.info(f"Telegram pinged.")



    logging.info(f"Task finished.")

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8080)