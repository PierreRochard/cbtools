from decimal import Decimal
import json
from pprint import pformat

from coinbase.wallet.client import Client
from dateutil.parser import parse
from dateutil.tz import tzlocal
from sqlalchemy import inspect
from sqlalchemy.exc import IntegrityError, ProgrammingError

from cbtools import db_logger
from cbtools.models import (session, Users, Accounts, Addresses, Transactions,
                            Exchanges, PaymentMethods, ReconciliationExceptions)
from cbtools.utilities import dict_compare


def update_user(user):
    new_user = Users()
    new_user.document = json.loads(str(user))
    new_user.country_code = user['country']['code']
    new_user.country_name = user['country']['name']
    new_user.created_at = parse(user.pop('created_at')).astimezone(tzlocal())
    for key in ['country', 'resource', 'resource_path']:
        del user[key]
    for key in user:
        if hasattr(new_user, key):
            if isinstance(user[key], dict):
                setattr(new_user, key, json.loads(str(user[key])))
            else:
                setattr(new_user, key, user[key])
        else:
            db_logger.error('{0} is missing from Users table, see {1}'.format(key, user['id']))
            continue
    session.add(new_user)
    try:
        session.commit()
    except IntegrityError:
        session.rollback()
        existing_user = session.query(Users).filter(Users.id == new_user.id).one()
        for column in inspect(Users).attrs:
            cbtools_version = getattr(existing_user, column.key)
            service_version = getattr(new_user, column.key)
            is_dict = isinstance(cbtools_version, dict) and isinstance(service_version, dict)
            if is_dict and cbtools_version != service_version:
                added, removed, modified, same = dict_compare(service_version, cbtools_version)
                if not added and not removed and not modified:
                    continue
                else:
                    new_exception = ReconciliationExceptions()
                    new_exception.table_name = 'Users'
                    new_exception.record_id = existing_user.id
                    new_exception.column_name = column.key
                    new_exception.cbtools_version = cbtools_version
                    new_exception.service_version = service_version
                    new_exception.json_doc = True
                    session.add(new_exception)
                    try:
                        session.commit()
                    except IntegrityError:
                        session.rollback()
                        db_logger.warn('Commit New Reconciliation Exception IntegrityError')
                    except ProgrammingError:
                        session.rollback()
                        db_logger.error('Commit New Reconciliation Exception ProgrammingError')
            elif cbtools_version == service_version:
                continue
            elif str(cbtools_version) != str(service_version):
                new_exception = ReconciliationExceptions()
                new_exception.table_name = 'Users'
                new_exception.record_id = existing_user.id
                new_exception.column_name = column.key
                new_exception.cbtools_version = cbtools_version
                new_exception.service_version = service_version
                new_exception.json_doc = False
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
        db_logger.error('Add User ProgrammingError')


def update_account(account):
    new_account = Accounts()
    new_account.document = json.loads(str(account))
    new_account.balance = Decimal(account['balance']['amount'])
    new_account.balance_currency = account['balance']['currency']
    new_account.native_balance = Decimal(account['native_balance']['amount'])
    new_account.native_balance_currency = account['native_balance']['currency']
    new_account.account_type = account.pop('type')
    for timestamp in ['created_at', 'updated_at']:
        setattr(new_account, timestamp, parse(account.pop(timestamp)).astimezone(tzlocal()))
    for key in ['balance', 'native_balance', 'resource', 'resource_path']:
        del account[key]
    for key in account:
        if hasattr(new_account, key):
            if isinstance(account[key], dict):
                setattr(new_account, key, json.loads(str(account[key])))
            else:
                setattr(new_account, key, account[key])
        else:
            db_logger.error('{0} is missing from Accounts table, see {1}'.format(key, account['id']))
            continue
    session.add(new_account)
    try:
        session.commit()
    except IntegrityError:
        session.rollback()
        existing_account = session.query(Accounts).filter(Accounts.id == new_account.id).one()
        for column in inspect(Accounts).attrs:
            cbtools_version = getattr(existing_account, column.key)
            service_version = getattr(new_account, column.key)
            is_dict = isinstance(cbtools_version, dict) and isinstance(service_version, dict)
            if is_dict and cbtools_version != service_version:
                added, removed, modified, same = dict_compare(service_version, cbtools_version)
                if not added and not removed and not modified:
                    continue
                else:
                    new_exception = ReconciliationExceptions()
                    new_exception.table_name = 'Accounts'
                    new_exception.record_id = existing_account.id
                    new_exception.column_name = column.key
                    new_exception.cbtools_version = cbtools_version
                    new_exception.service_version = service_version
                    new_exception.json_doc = True
                    session.add(new_exception)
                    try:
                        session.commit()
                    except IntegrityError:
                        session.rollback()
                        db_logger.warn('Commit New Reconciliation Exception IntegrityError')
                    except ProgrammingError:
                        session.rollback()
                        db_logger.error('Commit New Reconciliation Exception ProgrammingError')
            elif cbtools_version == service_version:
                continue
            elif str(cbtools_version) != str(service_version):
                new_exception = ReconciliationExceptions()
                new_exception.table_name = 'Accounts'
                new_exception.record_id = existing_account.id
                new_exception.column_name = column.key
                new_exception.cbtools_version = cbtools_version
                new_exception.service_version = service_version
                new_exception.json_doc = False
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
        db_logger.error('Add Account ProgrammingError')


def update_address(account_id, address):
    new_address = Addresses()
    new_address.document = json.loads(str(address))
    new_address.account_id = account_id
    for timestamp in ['created_at', 'updated_at']:
        setattr(new_address, timestamp, parse(address.pop(timestamp)).astimezone(tzlocal()))
    for key in ['callback_url', 'resource', 'resource_path']:
        del address[key]
    for key in address:
        if hasattr(new_address, key):
            if isinstance(address[key], dict):
                setattr(new_address, key, json.loads(str(address[key])))
            else:
                setattr(new_address, key, address[key])
        else:
            db_logger.error('{0} is missing from Addresses table, see {1}'.format(key, address['id']))
            continue
    session.add(new_address)
    try:
        session.commit()
    except IntegrityError:
        session.rollback()
        existing_address = session.query(Addresses).filter(Addresses.id == new_address.id).one()
        for column in inspect(Addresses).attrs:
            cbtools_version = getattr(existing_address, column.key)
            service_version = getattr(new_address, column.key)
            is_dict = isinstance(cbtools_version, dict) and isinstance(service_version, dict)
            if is_dict and cbtools_version != service_version:
                added, removed, modified, same = dict_compare(service_version, cbtools_version)
                if not added and not removed and not modified:
                    continue
                else:
                    new_exception = ReconciliationExceptions()
                    new_exception.table_name = 'Addresses'
                    new_exception.record_id = existing_address.id
                    new_exception.column_name = column.key
                    new_exception.cbtools_version = cbtools_version
                    new_exception.service_version = service_version
                    new_exception.json_doc = True
                    session.add(new_exception)
                    try:
                        session.commit()
                    except IntegrityError:
                        session.rollback()
                        db_logger.warn('Commit New Reconciliation Exception IntegrityError')
                    except ProgrammingError:
                        session.rollback()
                        db_logger.error('Commit New Reconciliation Exception ProgrammingError')
            elif cbtools_version == service_version:
                continue
            elif str(cbtools_version) != str(service_version):
                new_exception = ReconciliationExceptions()
                new_exception.table_name = 'Addresses'
                new_exception.record_id = existing_address.id
                new_exception.column_name = column.key
                new_exception.cbtools_version = cbtools_version
                new_exception.service_version = service_version
                new_exception.json_doc = False
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
        db_logger.error('Add Address ProgrammingError')


def update_transaction(account_id, transaction):
    if 'application' in transaction:
        from config import COINBASE_KEY, COINBASE_SECRET
        client = Client(COINBASE_KEY, COINBASE_SECRET)
        transaction = client.get_transaction(account_id, transaction['id'])
    new_transaction = Transactions()
    new_transaction.document = json.loads(str(transaction))
    new_transaction.account_id = account_id
    new_transaction.amount = Decimal(transaction['amount']['amount'])
    new_transaction.amount_currency = transaction['amount']['currency']
    new_transaction.native_amount = Decimal(transaction['native_amount']['amount'])
    new_transaction.native_amount_currency = transaction['native_amount']['currency']
    new_transaction.transaction_type = transaction.pop('type')
    if 'network' in transaction:
        new_transaction.network_status = transaction['network']['status']
        if 'hash' in transaction['network']:
            new_transaction.network_hash = transaction['network']['hash']
        del transaction['network']
    if 'to' in transaction:
        new_transaction.to_resource = transaction['to']['resource']
        if new_transaction.to_resource == 'bitcoin_address':
            new_transaction.to_address = transaction['to']['address']
        elif new_transaction.to_resource == 'user':
            new_transaction.to_user_id = transaction['to']['id']
        elif new_transaction.to_resource == 'email':
            new_transaction.to_email = transaction['to']['email']
        else:
            print(pformat(transaction['to']))
            raise Exception
        del transaction['to']
    if 'from' in transaction:
        new_transaction.from_resource = transaction['from']['resource']
        if new_transaction.from_resource == 'bitcoin_network':
            pass
        elif new_transaction.from_resource == 'user':
            new_transaction.from_user_id = transaction['from']['id']
        else:
            print(pformat(transaction['from']))
            raise Exception
        del transaction['from']
    if 'address' in transaction:
        new_transaction.address = transaction['address']['id']
        del transaction['address']
    for timestamp in ['created_at', 'updated_at']:
        setattr(new_transaction, timestamp, parse(transaction.pop(timestamp)).astimezone(tzlocal()))
    for key in ['resource', 'resource_path', 'amount', 'native_amount']:
        del transaction[key]
    if 'fiat_deposit' in transaction:
        new_transaction.exchange_id = transaction['fiat_deposit']['id']
        del transaction['fiat_deposit']
    elif 'fiat_withdrawal' in transaction:
        new_transaction.exchange_id = transaction['fiat_withdrawal']['id']
        del transaction['fiat_withdrawal']
    elif 'buy' in transaction:
        new_transaction.exchange_id = transaction['buy']['id']
        del transaction['buy']
    elif 'sell' in transaction:
        new_transaction.exchange_id = transaction['sell']['id']
        del transaction['sell']
    if 'order' in transaction:
        new_transaction.order_id = transaction['order']['id']
        del transaction['order']
    if 'application' in transaction:
        new_transaction.application_id = transaction['application']['id']
        del transaction['application']
    if 'idem' in transaction:
        del transaction['idem']
    for key in transaction:
        if hasattr(new_transaction, key):
            if isinstance(transaction[key], dict):
                setattr(new_transaction, key, json.loads(str(transaction[key])))
            else:
                setattr(new_transaction, key, transaction[key])
        else:
            db_logger.error('{0} is missing from Transactions table, see {1}'.format(key, transaction['id']))
            continue
    session.add(new_transaction)
    try:
        session.commit()
    except IntegrityError:
        session.rollback()
        existing_transaction = session.query(Transactions).filter(Transactions.id == new_transaction.id).one()
        for column in inspect(Transactions).attrs:
            cbtools_version = getattr(existing_transaction, column.key)
            service_version = getattr(new_transaction, column.key)
            is_dict = isinstance(cbtools_version, dict) and isinstance(service_version, dict)
            if is_dict and cbtools_version != service_version:
                added, removed, modified, same = dict_compare(service_version, cbtools_version)
                if not added and not removed and not modified:
                    continue
                else:
                    new_exception = ReconciliationExceptions()
                    new_exception.table_name = 'Transactions'
                    new_exception.record_id = existing_transaction.id
                    new_exception.column_name = column.key
                    new_exception.cbtools_version = cbtools_version
                    new_exception.service_version = service_version
                    new_exception.json_doc = True
                    session.add(new_exception)
                    try:
                        session.commit()
                    except IntegrityError:
                        session.rollback()
                        db_logger.warn('Commit New Reconciliation Exception IntegrityError')
                    except ProgrammingError:
                        session.rollback()
                        db_logger.error('Commit New Reconciliation Exception ProgrammingError')
            elif cbtools_version == service_version:
                continue
            elif str(cbtools_version) != str(service_version):
                new_exception = ReconciliationExceptions()
                new_exception.table_name = 'Transactions'
                new_exception.record_id = existing_transaction.id
                new_exception.column_name = column.key
                new_exception.cbtools_version = cbtools_version
                new_exception.service_version = service_version
                new_exception.json_doc = False
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
        db_logger.error('Add Transaction ProgrammingError')
        print(pformat(transaction))
        raise


def update_exchanges(account_id, exchange_type, exchanges):
    pass


def update_payment_methods(payment_methods):
    pass


def update_database(api_key, api_secret):

    client = Client(api_key, api_secret)
    current_user = client.get_current_user()
    update_user(current_user)

    accounts = client.get_accounts()['data']

    for account in accounts:
        update_account(account)
        addresses = client.get_addresses(account['id'])
        while addresses.pagination['next_uri']:
            addresses_data = addresses['data']
            for address in addresses_data:
                update_address(account['id'], address)
            starting_after = addresses.pagination['next_uri'].split('=')[-1]
            addresses = client.get_addresses(account['id'], starting_after=starting_after)
        else:
            addresses_data = addresses['data']
            for address in addresses_data:
                update_address(account['id'], address)

        transactions = client.get_transactions(account['id'])
        while transactions.pagination['next_uri']:
            transactions_data = transactions['data']
            for transaction in transactions_data:
                update_transaction(account['id'], transaction)
            starting_after = transactions.pagination['next_uri'].split('=')[-1]
            transactions = client.get_transactions(account['id'], starting_after=starting_after)
        else:
            transactions_data = transactions['data']
            for transaction in transactions_data:
                update_transaction(account['id'], transaction)
        continue



        buys = client.get_buys(account['id'])['data']
        update_exchanges(account['id'], 'buy', buys)

        sells = client.get_sells(account['id'])['data']
        update_exchanges(account['id'], 'sell', sells)

        deposits = client.get_deposits(account['id'])['data']
        update_exchanges(account['id'], 'deposit', deposits)

        withdrawals = client.get_withdrawals(account['id'])['data']
        update_exchanges(account['id'], 'withdrawal', withdrawals)

    payment_methods = client.get_payment_methods()
    update_payment_methods(payment_methods)


if __name__ == '__main__':
    from config import COINBASE_KEY, COINBASE_SECRET
    update_database(COINBASE_KEY, COINBASE_SECRET)
