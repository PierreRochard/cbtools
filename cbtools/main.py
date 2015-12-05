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
    del user['country']
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
    del account['balance']
    new_account.native_balance = Decimal(account['native_balance']['amount'])
    new_account.native_balance_currency = account['native_balance']['currency']
    del account['native_balance']
    new_account.created_at = parse(account.pop('created_at')).astimezone(tzlocal())
    new_account.updated_at = parse(account.pop('updated_at')).astimezone(tzlocal())
    new_account.account_type = account['type']
    del account['type']
    del account['resource']
    del account['resource_path']
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


def update_addresses(account_id, addresses):
    print(pformat(addresses))
    pass


def update_transactions(account_id, transactions):
    pass


def update_exchanges(account_id, exchange_type, exchanges):
    pass


def update_payment_methods(payment_methods):
    pass


def update_database(api_key, api_secret):
    client = Client(api_key, api_secret)

    current_user = client.get_current_user()
    update_user(current_user)
    # print(pformat(current_user))

    accounts = client.get_accounts()['data']

    # print(pformat(accounts))

    for account in accounts:
        update_account(account)
        addresses = client.get_addresses(account['id'])['data']
        update_addresses(account['id'], addresses)
        continue

        transactions = client.get_transactions(account['id'])['data']
        update_transactions(account['id'], transactions)

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
