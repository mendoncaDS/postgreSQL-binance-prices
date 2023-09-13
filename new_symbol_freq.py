import os
import argparse

import vectorbt as vbt

from dotenv import load_dotenv
from sqlalchemy import create_engine, text


load_dotenv()
database_password = os.environ.get('database_password')
server_address = os.environ.get('server_address')
connection_string = f"postgresql://postgres:{database_password}@{server_address}:5432/postgres"
engine = create_engine(connection_string,echo=True)


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



def write_to_database(data, symbol, freq):

    dataIn = data.copy()

    dataIn.columns = ['open', 'high', 'low', 'close', 'volume']

    dataIn['frequency'] = freq
    dataIn['symbol'] = symbol

    dataIn.index.name = 'timestamp'
    
    with engine.begin() as connection:
        dataIn.to_sql('market_data', connection, if_exists='append', index=True)
      



def main(symbol, freq):
    if not check_symbol_freq(symbol, freq):

        print(f"symbol {symbol}, frequency {freq} not found. inserting...")

        data = vbt.BinanceData.download(
            symbols=symbol,
            interval=freq
            ).get(["Open","High","Low","Close","Volume"])
        data = data.iloc[:-1,:]

        write_to_database(data, symbol, freq)
    else:
        print(f"symbol {symbol} and frequency {freq} already in database")




if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="My script description")
    
    parser.add_argument('-symbol', required=True, help='First argument')
    parser.add_argument('-freq', required=False, help='Second argument', default="default_value")
    
    args = parser.parse_args()
    
    main(args.symbol, args.freq)
