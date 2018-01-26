import os

from sqlalchemy.engine.url import URL

COINBASE_KEY = os.environ['COINBASE_KEY']
COINBASE_SECRET = os.environ['COINBASE_SECRET']
COINBASE_EXCHANGE_API_KEY = os.environ['COINBASE_EXCHANGE_API_KEY']
COINBASE_EXCHANGE_API_SECRET = os.environ['COINBASE_EXCHANGE_API_SECRET']
COINBASE_EXCHANGE_API_PASSPHRASE = os.environ['COINBASE_EXCHANGE_API_PASSPHRASE']

URI = URL(drivername='postgresql+psycopg2',
          username=os.environ['PGUSER'],
          password=os.environ['PGPASSWORD'],
          host=os.environ['PGHOST'],
          port=os.environ['PGPORT'],
          database=os.environ['CBTOOLS_PGDATABASE'])
