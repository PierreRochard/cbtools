from coinbase.wallet.client import Client


def update_database(api_key, api_secret):
    client = Client(api_key, api_secret)
    client.get_user()


if __name__ == '__main__':
    from config import COINBASE_KEY, COINBASE_SECRET
    update_database(COINBASE_KEY, COINBASE_SECRET)