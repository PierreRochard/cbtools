# cbtools

cbtools allows you to download your Coinbase wallet data into a relational SQL database for further analysis.


If you need help with any of these steps please feel free to open an issue, I'd be glad to clarify and help get you set up.

1. Install PostgreSQL

2. Create a database in PostgreSQL

3. Clone the cbtools repository

4. Get API keys from GDAX and Coinbase, add the environment variables mentioned in config.py

5. Install the psycopg2, sqlalchemy, and coinbase Python packages

```
pip install -r requirements.txt
```

6. Run the scripts

```
python cbtools/models.py
python cbtools/main.py
```