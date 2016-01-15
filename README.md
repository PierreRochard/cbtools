# cbtools

cbtools allows you to download your Coinbase wallet data into a relational SQL database for further analysis.


If you need help with any of these steps please feel free to open an issue, I'd be glad to clarify and help get you set up.

1. Install PostgreSQL

2. Create a database in PostgreSQL

3. Clone the cbtools repository

4. Create a config.py file in the current directory with the following parameters:

```
# I would advise setting the key permissions restrictively, only select ones with 'read' and 'limits'
COINBASE_KEY = ''
COINBASE_SECRET = ''
COINBASE_EXCHANGE_API_KEY = ''
COINBASE_EXCHANGE_API_SECRET = ''
COINBASE_EXCHANGE_API_PASSPHRASE = ''

# Creating a database user with permissions restricted to your cbtools database would be wise
USER = 'cbtools'
PASSWORD = 'PASSWORD'
HOST = 'localhost'
PORT = 5432
DB = 'cbtools'

URI = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(USER, PASSWORD, HOST, PORT, DB)
```

5. Install the psycopg2, sqlalchemy, and coinbase Python packages

```
pip install -r requirements.txt
```

6. Run the scripts

```
python cbtools/models.py
python cbtools/main.py
```