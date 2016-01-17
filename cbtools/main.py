import argparse
import os

import json
import requests
from coinbase.wallet.client import Client

from cbtools.utilities import denest_json


def get_wallet_data(wallet_client):
    denested_json_file = 'denested_wallet_jsons.json'
    if not os.path.exists(denested_json_file):
        denested_jsons = []
        current_user = wallet_client.get_current_user()
        denested_jsons += denest_json(current_user.json_doc)

        wallet_accounts = wallet_client.get_accounts()
        denested_jsons += denest_json(wallet_accounts)

        payment_methods = wallet_client.get_payment_methods()
        denested_jsons += denest_json(payment_methods)

        for wallet_account in wallet_accounts['data']:
            for end_point, function in [('get_buys', 'update_exchange'),
                                        ('get_sells', 'update_exchange'),
                                        ('get_deposits', 'update_exchange'),
                                        ('get_withdrawals', 'update_exchange'),
                                        ('get_addresses', 'update_address'),
                                        ('get_transactions', 'update_transaction')]:
                response = getattr(wallet_client, end_point)(wallet_account['id'])
                denested_jsons += denest_json(response, account_id=wallet_account['id'])
                while response.pagination['next_uri']:
                    starting_after = response.pagination['next_uri'].split('=')[-1]
                    response = getattr(wallet_client, end_point)(wallet_account['id'], starting_after=starting_after)
                    denested_jsons += denest_json(response, account_id=wallet_account['id'])
        with open(denested_json_file, 'w') as json_file:
            json.dump(denested_jsons, json_file, indent=4, sort_keys=True)
    else:
        with open(denested_json_file, 'r') as json_file:
            denested_jsons = json.load(json_file)
    return denested_jsons


def get_exchange_data(auth, url):
    denested_json_file = 'denested_exchange_jsons.json'
    if not os.path.exists(denested_json_file):
        denested_jsons = []
        exchange_accounts = requests.get(url + 'accounts', auth=auth).json()
        denested_jsons += denest_json(exchange_accounts, resource='exchange_account')
        for exchange_account in exchange_accounts:
            for end_point, function in [('ledger', 'update_entry'),
                                        ('holds', 'update_hold'),
                                        ('orders', 'update_exchange_order'),
                                        ('fills', 'update_fill')]:
                params = {}
                if end_point == 'orders':
                    # params['status'] = 'all'
                    params['status'] = ['open', 'pending', 'done']
                    end_point_url = url + 'orders'
                elif end_point == 'fills':
                    end_point_url = url + 'fills'
                else:
                    end_point_url = url + 'accounts/' + exchange_account['id'] + '/' + end_point
                if end_point.endswith('s'):
                    resource = end_point[:-1]
                else:
                    resource = end_point
                response = requests.get(end_point_url, auth=auth, params=params)
                denested_jsons += denest_json(response.json(), account_id=exchange_account['id'], resource=resource)
                while 'CB-AFTER' in response.headers:
                    starting_after = response.headers['CB-AFTER']
                    params['after'] = starting_after
                    response = requests.get(end_point_url, params=params, auth=auth)
                    denested_jsons += denest_json(response.json(), account_id=exchange_account['id'], resource=resource)
                with open(denested_json_file, 'w') as json_file:
                    json.dump(denested_jsons, json_file, indent=4, sort_keys=True)
    else:
        with open(denested_json_file, 'r') as json_file:
            denested_jsons = json.load(json_file)
    return denested_jsons

if __name__ == '__main__':
    ARGS = argparse.ArgumentParser()
    ARGS.add_argument('--w', action='store_true', dest='wallet',
                      default=False, help='Download Coinbase Wallet Data')
    ARGS.add_argument('--e', action='store_true', dest='exchange',
                      default=False, help='Download Coinbase Exchange Data')
    args = ARGS.parse_args()
    json_docs = []
    if args.wallet:
        from config import COINBASE_KEY, COINBASE_SECRET

        coinbase_wallet_client = Client(COINBASE_KEY, COINBASE_SECRET)
        json_docs += get_wallet_data(coinbase_wallet_client)
    if args.exchange:
        from config import (COINBASE_EXCHANGE_API_KEY, COINBASE_EXCHANGE_API_SECRET, COINBASE_EXCHANGE_API_PASSPHRASE)
        from cbtools.utilities import CoinbaseExchangeAuthentication

        exchange_api_url = 'https://api.exchange.coinbase.com/'
        exchange_auth = CoinbaseExchangeAuthentication(COINBASE_EXCHANGE_API_KEY, COINBASE_EXCHANGE_API_SECRET,
                                                       COINBASE_EXCHANGE_API_PASSPHRASE)

        json_docs += get_exchange_data(exchange_auth, exchange_api_url)
