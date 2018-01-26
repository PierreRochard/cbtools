import argparse
import os

import json
from collections import OrderedDict
from datetime import datetime
from decimal import Decimal

from coinbase.wallet.client import Client
from dateutil.tz import tzlocal
import requests
from sqlalchemy import inspect
from sqlalchemy.exc import IntegrityError, ProgrammingError
from sqlalchemy.orm.exc import FlushError

from cbtools.models import session, ReconciliationExceptions
from cbtools.utilities import denest_json
from cbtools import models, db_logger


def get_wallet_data(wallet_client, refresh):
    denested_json_file = tmp_directory + 'denested_wallet_jsons.json'
    if not os.path.exists(denested_json_file) or refresh:
        denested_jsons = []
        current_user = wallet_client.get_current_user()
        denested_jsons += denest_json(current_user)

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


def get_exchange_data(auth, url, refresh):
    denested_json_file = tmp_directory + 'denested_exchange_jsons.json'
    if not os.path.exists(denested_json_file) or refresh:
        denested_jsons = []
        exchange_accounts = requests.get(url + 'accounts', auth=auth).json()
        denested_jsons += denest_json(exchange_accounts, resource='exchange_account')
        for exchange_account in exchange_accounts:
            for end_point, function in [('ledger', 'update_entry'),
                                        ('holds', 'update_hold'),
                                        ('orders', 'update_exchange_order'),
                                        ('fills', 'update_fill')]:
                if end_point.endswith('s'):
                    resource = end_point[:-1]
                else:
                    resource = end_point
                params = {}
                if end_point == 'orders':
                    # params['status'] = 'all'
                    params['status'] = ['open', 'pending', 'done']
                    end_point_url = url + 'orders'
                    resource = 'exchange_order'
                elif end_point == 'fills':
                    end_point_url = url + 'fills'
                else:
                    end_point_url = url + 'accounts/' + exchange_account['id'] + '/' + end_point
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


resource_to_model = OrderedDict([('user', models.Users),
                                ('account', models.Accounts),
                                ('exchange_account', models.ExchangeAccounts),
                                ('payment_method', models.PaymentMethods),
                                ('address', models.Addresses),
                                ('fill', [models.Fills, ('trade_id', 'created_at', 'account_id')]),
                                ('hold', models.Holds),
                                ('ledger', models.Entries),
                                ('limit', [models.Limits, ('payment_method_id', 'type', 'period_in_days')]),
                                ('order', models.Orders),
                                ('exchange_order', models.ExchangeOrders),
                                ('buy', models.Exchanges),
                                ('deposit', models.Exchanges),
                                ('sell', models.Exchanges),
                                ('withdrawal', models.Exchanges),
                                ('transaction', models.Transactions),
                                ('fee', [models.Fees, ('source_id', 'fee_type')])])


def insert(document):
    if isinstance(resource_to_model[document['resource']], list):
        Model = resource_to_model[document['resource']][0]
        query_keys = resource_to_model[document['resource']][1]
    else:
        Model = resource_to_model[document['resource']]
        query_keys = None
    new_record = Model()
    for key in document:
        if hasattr(new_record, key):
            if isinstance(document[key], dict):
                setattr(new_record, key, json.loads(str(document[key])))
            else:
                setattr(new_record, key, document[key])
        else:
            db_logger.error('{0} is missing from {1} table'.format(key, Model.__tablename__))
            continue
    try:
        session.add(new_record)
        session.commit()
    except (IntegrityError, FlushError):
        session.rollback()
        if not query_keys:
            old_record = session.query(Model).filter(Model.id == new_record.id).one()
        elif len(query_keys) == 2:
            old_record = (session.query(Model)
                          .filter(getattr(Model, query_keys[0]) == getattr(new_record, query_keys[0]))
                          .filter(getattr(Model, query_keys[1]) == getattr(new_record, query_keys[1])).one())
        elif len(query_keys) == 3:
            old_record = (session.query(Model)
                          .filter(getattr(Model, query_keys[0]) == getattr(new_record, query_keys[0]))
                          .filter(getattr(Model, query_keys[1]) == getattr(new_record, query_keys[1]))
                          .filter(getattr(Model, query_keys[2]) == getattr(new_record, query_keys[2])).one())
        for column in inspect(Model).attrs:
            if column.key == 'id':
                continue
            old_version = getattr(old_record, column.key)
            new_version = getattr(new_record, column.key)
            if old_version == new_version:
                continue
            elif isinstance(old_version, datetime):
                try:
                    new_version_datetime = datetime.strptime(new_version, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=tzlocal())
                except ValueError:
                    new_version_datetime = (datetime.strptime(new_version, '%Y-%m-%dT%H:%M:%S.%fZ')
                                            .replace(tzinfo=tzlocal()))
                if new_version_datetime == old_version:
                    continue
            elif isinstance(old_version, Decimal):
                new_version_decimal = Decimal(new_version)
                if new_version_decimal == old_version:
                    continue
            elif isinstance(old_version, int):
                print(old_version)
                print(new_version)
                print(type(old_version))
                new_version_int = int(new_version)
                if new_version_int == old_version:
                    continue
            elif str(old_version) != str(new_version):
                new_exception = ReconciliationExceptions()
                new_exception.table_name = Model.__tablename__
                new_exception.record_id = old_record.id
                new_exception.column_name = column.key
                new_exception.old_version = old_version
                new_exception.new_version = new_version
                new_exception.old_version_type = type(old_version)
                new_exception.new_version_type = type(new_version)
                session.add(new_exception)
                try:
                    session.commit()
                except IntegrityError:
                    session.rollback()
                    db_logger.error('Commit New Reconciliation Exception IntegrityError')
                except ProgrammingError:
                    session.rollback()
                    db_logger.error('Commit New Reconciliation Exception ProgrammingError')
    except ProgrammingError:
        session.rollback()
        db_logger.error('Add {0} ProgrammingError'.format(Model.__tablename__))


if __name__ == '__main__':
    ARGS = argparse.ArgumentParser()
    ARGS.add_argument('--w', action='store_true', dest='wallet',
                      default=True, help='Load Coinbase Wallet Data into the database')
    ARGS.add_argument('--e', action='store_true', dest='exchange',
                      default=True, help='Load Coinbase Exchange Data into the database')
    ARGS.add_argument('--r', action='store_true', dest='refresh', default=False, help='Refresh the data')
    args = ARGS.parse_args()
    tmp_directory = 'tmp/'
    if not os.path.exists(tmp_directory):
        os.mkdir(tmp_directory)
    json_docs = []
    if args.wallet:
        from config import COINBASE_KEY, COINBASE_SECRET

        coinbase_wallet_client = Client(COINBASE_KEY, COINBASE_SECRET)
        json_docs += get_wallet_data(coinbase_wallet_client, args.refresh)
    if args.exchange:
        from config import (COINBASE_EXCHANGE_API_KEY, COINBASE_EXCHANGE_API_SECRET, COINBASE_EXCHANGE_API_PASSPHRASE)
        from cbtools.utilities import CoinbaseExchangeAuthentication

        exchange_api_url = 'https://api.gdax.com/'
        exchange_auth = CoinbaseExchangeAuthentication(COINBASE_EXCHANGE_API_KEY, COINBASE_EXCHANGE_API_SECRET,
                                                       COINBASE_EXCHANGE_API_PASSPHRASE)

        json_docs += get_exchange_data(exchange_auth, exchange_api_url, args.refresh)

    stats = {}
    for doc in json_docs:
        if doc['resource'] in stats:
            stats[doc['resource']] += 1
        else:
            stats[doc['resource']] = 1

    order_of_documents = [key for key in resource_to_model]

    json_docs = sorted(json_docs, key=lambda i: order_of_documents.index(i['resource']))

    for doc in json_docs:
        insert(doc)
