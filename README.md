1. Install PostgreSQL

2. Create a database in PostgreSQL

3. Create a config.py file in the current directory with the following parameters:

```
# I would advise setting the key permissions restrictively, only select ones with 'read' and 'limits'
COINBASE_KEY = ''
COINBASE_SECRET = ''

# Creating a database user with permissions restricted to your cbtools database would be wise
PG_USERNAME = 'cbtools'
PG_PASSWORD = 'PASSWORD'
PG_HOST = 'localhost'
PG_PORT = 5432
PG_DB = 'cbtools'

URI = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(PG_USERNAME, PG_PASSWORD, PG_HOST, PG_PORT, PG_DB)
```