
import os

from dotenv import load_dotenv
from sqlalchemy.ext.declarative import declarative_base

from sqlalchemy import create_engine, Column, String, Float, DateTime, PrimaryKeyConstraint


load_dotenv()
database_password = os.environ.get('database_password')
server_address = os.environ.get('server_address')

connection_string = f"postgresql://postgres:{database_password}@{server_address}:5432/postgres"
engine = create_engine(connection_string,echo=True)



Base = declarative_base()

class MarketData(Base):

    __tablename__ = 'market_data'
    
    timestamp = Column(DateTime, nullable=False)
    symbol = Column(String, nullable=False)
    frequency = Column(String, nullable=False)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)
    
    __table_args__ = (
        PrimaryKeyConstraint('timestamp', 'symbol', 'frequency'),
    )


# Create the table
Base.metadata.create_all(engine)
