import argparse
from pprint import pformat

import requests
from coinbase.wallet.client import Client

from cbtools import insert
from cbtools.insert import update_entry, update_hold, update_exchange_order


def update_wallet_data(wallet_client):
    current_user = wallet_client.get_current_user()
    insert.update_user(current_user, wallet_client)

    wallet_accounts = wallet_client.get_accounts()['data']
    for wallet_account in wallet_accounts:
        insert.update_account(wallet_account, user_id=current_user['id'])

    payment_methods = wallet_client.get_payment_methods()
    for payment_method in payment_methods['data']:
        insert.update_payment_method(wallet_client, payment_method)

    for wallet_account in wallet_accounts:
        for end_point, function in [('get_buys', 'update_exchange'),
                                    ('get_sells', 'update_exchange'),
                                    ('get_deposits', 'update_exchange'),
                                    ('get_withdrawals', 'update_exchange'),
                                    ('get_addresses', 'update_address'),
                                    ('get_transactions', 'update_transaction')]:
            response = getattr(wallet_client, end_point)(wallet_account['id'])
            while response.pagination['next_uri']:
                data = response['data']
                for datum in data:
                    globals()[function](wallet_client, wallet_account['id'], datum)
                starting_after = response.pagination['next_uri'].split('=')[-1]
                response = getattr(wallet_client, end_point)(wallet_account['id'], starting_after=starting_after)
            else:
                data = response['data']
                for datum in data:
                    globals()[function](wallet_client, wallet_account['id'], datum)


def update_exchange_data(auth, url):
    exchange_accounts = requests.get(url + 'accounts', auth=auth).json()
    for exchange_account in exchange_accounts:
        insert.update_account(exchange_account, exchange_account=True)
        for end_point, function in [
                                    # ('ledger', 'update_entry'),
                                    # ('holds', 'update_hold'),
                                    ('orders', 'update_exchange_order')
                                    ]:
            if end_point == 'orders':
                # params = {'status': 'all'}
                params = {'status': ['open', 'pending', 'done']}
                end_point_url = url + 'orders'
            else:
                params = {}
                end_point_url = url + 'accounts/' + exchange_account['id'] + '/' + end_point
            response = requests.get(end_point_url, auth=auth, params=params)
            while 'CB-AFTER' in response.headers:
                data = response.json()
                for datum in data:
                    globals()[function](exchange_account['id'], datum)
                starting_after = response.headers['CB-AFTER']
                params['after'] = starting_after
                response = requests.get(end_point_url, params=params, auth=auth)
            else:
                data = response.json()
                for datum in data:
                    globals()[function](exchange_account['id'], datum)


if __name__ == '__main__':
    ARGS = argparse.ArgumentParser()
    ARGS.add_argument('--w', action='store_true', dest='wallet',
                      default=False, help='Download Coinbase Wallet Data')
    ARGS.add_argument('--e', action='store_true', dest='exchange',
                      default=False, help='Download Coinbase Exchange Data')
    args = ARGS.parse_args()
    if args.wallet:
        from config import COINBASE_KEY, COINBASE_SECRET

        coinbase_wallet_client = Client(COINBASE_KEY, COINBASE_SECRET)
        update_wallet_data(coinbase_wallet_client)
    if args.exchange:
        from config import (COINBASE_EXCHANGE_API_KEY, COINBASE_EXCHANGE_API_SECRET, COINBASE_EXCHANGE_API_PASSPHRASE)
        from cbtools.utilities import CoinbaseExchangeAuthentication

        exchange_api_url = 'https://api.exchange.coinbase.com/'
        exchange_auth = CoinbaseExchangeAuthentication(COINBASE_EXCHANGE_API_KEY, COINBASE_EXCHANGE_API_SECRET,
                                                       COINBASE_EXCHANGE_API_PASSPHRASE)

        update_exchange_data(exchange_auth, exchange_api_url)
