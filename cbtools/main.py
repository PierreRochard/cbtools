from pprint import pformat

from coinbase.wallet.client import Client


def update_user(user):
    pass


def update_accounts(accounts):
    pass


def update_addresses(account_id, addresses):
    pass


def update_transactions(account_id, transactions):
    pass


def update_exchanges(account_id, exchanges):
    pass


def update_database(api_key, api_secret):
    client = Client(api_key, api_secret)

    current_user = client.get_current_user()
    update_user(current_user)
    # print(pformat(current_user))

    accounts = client.get_accounts()['data']
    update_accounts(accounts)
    # print(pformat(accounts))

    for account in accounts:
        addresses = client.get_addresses(account['id'])['data']
        update_addresses(account['id'], addresses)
        # print(pformat(addresses))

        transactions = client.get_transactions(account['id'])['data']
        update_transactions(account['id'], transactions)

        buys = client.get_buys(account['id'])['data']
        update_exchanges(account['id'], buys)

        sells = client.get_sells(account['id'])['data']
        update_exchanges(account['id'], sells)

        # deposits = client.get_deposits()

if __name__ == '__main__':
    from config import COINBASE_KEY, COINBASE_SECRET
    update_database(COINBASE_KEY, COINBASE_SECRET)