import argparse
import os
from pprint import pformat

import json
import requests
from coinbase.wallet.client import Client

from cbtools import insert
from cbtools.insert import update_entry, update_hold, update_exchange_order, update_fill
from cbtools.models import Accounts, Users


def denest_json(json_document, account_id=None, resource=None):
    if 'data' in json_document:
        json_document = json_document['data']
    if not isinstance(json_document, list):
        json_documents = [json_document]
    else:
        json_documents = json_document
    new_json_documents = []
    for json_document in json_documents:
        new_json_document = {}
        for key, value in json_document.items():
            if key == 'json_doc':
                continue
            elif isinstance(value, dict):
                for nested_key, nested_value in value.items():
                    if 'json_doc' in nested_key:
                        continue
                    elif isinstance(nested_value, list) and key == 'limits':
                        for limit in nested_value:
                            new_limit = {}
                            for limit_key, limit_value in limit.items():
                                if limit_key == 'json_doc':
                                    continue
                                elif isinstance(limit_value, dict):
                                    for nested_limit_key, nested_limit_value in limit_value.items():
                                        if nested_limit_key == 'json_doc':
                                            continue
                                        elif nested_limit_key == 'amount':
                                            new_limit[limit_key] = nested_limit_value
                                        else:
                                            new_key = limit_key + '_' + nested_limit_key
                                            new_limit[new_key] = nested_limit_value
                                else:
                                    new_limit[limit_key] = limit_value
                                new_limit['resource'] = 'limit'
                                new_limit['type'] = nested_key
                                new_limit['payment_method_id'] = json_document['id']
                            new_json_documents += [new_limit]
                    elif isinstance(nested_value, str) or isinstance(nested_value, bool) or nested_value is None:
                        if nested_key == 'amount':
                            new_json_document[key] = nested_value
                        else:
                            new_key = key + '_' + nested_key
                            new_json_document[new_key] = nested_value
                    else:
                        print(type(nested_value))
                        raise Exception()
            elif isinstance(value, list) and json_document['resource'] in ['buy', 'sell', 'deposit', 'withdrawal']:
                for fee in value:
                    new_fee = {}
                    for fee_key, fee_value in fee.items():
                        if isinstance(fee_value, dict):
                            for nested_fee_key, nested_fee_value in fee_value.items():
                                if nested_fee_key == 'amount':
                                    new_fee[fee_key] = nested_fee_value
                                else:
                                    new_key = fee_key + '_' + nested_fee_key
                                    new_fee[new_key] = nested_fee_value
                        elif isinstance(fee_value, str) or isinstance(fee_value, bool) or fee_value is None:
                            new_fee[fee_key] = fee_value
                        else:
                            print(type(fee_value))
                            raise Exception()
                    new_fee['resource'] = 'fee'
                    new_fee['source_id'] = json_document['id']
                    new_json_documents += [new_fee]
            elif isinstance(value, str) or isinstance(value, bool) or isinstance(value, int) or value is None:
                new_json_document[key] = json_document[key]
            else:
                print(pformat(json_document))
                print(type(value))
                raise Exception()
        if resource:
            new_json_document['resource'] = resource
        if account_id:
            new_json_document['account_id'] = account_id
        new_json_documents += [new_json_document]
    print(pformat(new_json_documents))
    if new_json_documents:
        return new_json_documents
    else:
        return []


def update_wallet_data(wallet_client):
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
    stats = {}
    for doc in denested_jsons:
        if doc['resource'] not in ['buy', 'sell', 'deposit', 'withdrawal']:
            if doc['resource'] in stats:
                stats[doc['resource']]['count'] += 1
            else:
                stats[doc['resource']] = {'count': 1}
            # for key, value in doc.items():
            #     if key in stats[doc['resource']]:
            #         lengths = [len(str(item)) for item in stats[doc['resource']][key]]
            #         if doc[key] not in stats[doc['resource']][key] and len(str(doc[key])) not in lengths:
            #             stats[doc['resource']][key] += [doc[key]]
            #     else:
            #         stats[doc['resource']][key] = [doc[key]]
        else:
            if 'exchange' in stats:
                stats['exchange']['count'] += 1
            else:
                stats['exchange'] = {'count': 1}
            # for key, value in doc.items():
            #     if key in stats['exchange']:
            #         lengths = [len(str(item)) for item in stats['exchange'][key]]
            #         if doc[key] not in stats['exchange'][key] and len(str(doc[key])) not in lengths:
            #             stats['exchange'][key] += [doc[key]]
            #     else:
            #         stats['exchange'][key] = [doc[key]]
    with open('wallet_json_stats.json', 'w') as json_file:
        json.dump(stats, json_file, indent=4, sort_keys=True)
    print(pformat(stats))


def update_exchange_data(auth, url):
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

    stats = {}
    for doc in denested_jsons:
        if doc['resource'] in stats:
            stats[doc['resource']]['count'] += 1
        else:
            stats[doc['resource']] = {'count': 1}
        for key, value in doc.items():
            if key in stats[doc['resource']]:
                lengths = [len(str(item)) for item in stats[doc['resource']][key]]
                if doc[key] not in stats[doc['resource']][key] and len(str(doc[key])) not in lengths:
                    stats[doc['resource']][key] += [doc[key]]
            else:
                stats[doc['resource']][key] = [doc[key]]

    with open('exchange_json_stats.json', 'w') as json_file:
        json.dump(stats, json_file, indent=4, sort_keys=True)
    print(pformat(stats))


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
