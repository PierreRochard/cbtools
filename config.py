import os

from sqlalchemy.engine.url import URL

COINBASE_KEY = os.environ['COINBASE_KEY']
COINBASE_SECRET = os.environ['COINBASE_SECRET']
GDAX_API_KEY = os.environ['GDAX_API_KEY']
GDAX_API_SECRET = os.environ['GDAX_API_SECRET']
GDAX_API_PASSPHRASE = os.environ['GDAX_API_PASSPHRASE']

URI = URL(drivername='postgresql+psycopg2',
          username=os.environ['PGUSER'],
          password=os.environ['PGPASSWORD'],
          host=os.environ['PGHOST'],
          port=os.environ['PGPORT'],
          database=os.environ['CBTOOLS_PGDATABASE'])
