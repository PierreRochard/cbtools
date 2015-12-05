

Create a config.py file in the current directory with the following parameters:
<code>
COINBASE_KEY = ''
COINBASE_SECRET = ''

PG_USERNAME = 'cbtools'
PG_PASSWORD = 'PASSWORD'
PG_HOST = 'localhost'
PG_PORT = 5432
PG_DB = 'cbtools'

URI = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(PG_USERNAME, PG_PASSWORD, PG_HOST, PG_PORT, PG_DB)
</code>