import argparse
from decimal import Decimal
import json
from pprint import pformat

from coinbase.wallet.client import Client
from coinbase.wallet.error import NotFoundError
from dateutil.parser import parse
from dateutil.tz import tzlocal
from sqlalchemy import inspect
from sqlalchemy.exc import IntegrityError, ProgrammingError
from sqlalchemy.orm.exc import FlushError, NoResultFound
import requests

from cbtools import db_logger
from cbtools.models import (session, Users, Accounts, Addresses, Transactions,
                            Exchanges, Fees, PaymentMethods, Limits, ReconciliationExceptions, Entries)
from cbtools.utilities import dict_compare


def update_user(user, client):
    new_user = Users()
    new_user.document = json.loads(str(user))
    if 'country' in user:
        new_user.country_code = user['country']['code']
        new_user.country_name = user['country']['name']
        del user['country']
    if 'created_at' in user:
        new_user.created_at = parse(user.pop('created_at')).astimezone(tzlocal())
    for key in ['resource', 'resource_path']:
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
    except (IntegrityError, FlushError):
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


def update_account(account, user_id=None, exchange_account=False):
    new_account = Accounts()
    if exchange_account:
        new_account.document = account
        new_account.id = account['id']
        new_account.balance = account['balance']
        new_account.balance_currency = account['currency']
        new_account.hold = account['hold']
        new_account.hold_currency = account['currency']
        new_account.available = account['available']
        new_account.available_currency = account['currency']
    else:
        new_account.user_id = user_id
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
    except (IntegrityError, FlushError):
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
            try:
                numeric_inequality = float(cbtools_version) != float(service_version)
            except TypeError:
                numeric_inequality = True
            if str(cbtools_version) != str(service_version) and numeric_inequality:
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


def update_address(client, account_id, address):
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


def update_transaction(client, account_id, transaction):
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
            to_user = client.get_user(transaction['to']['id'])
            update_user(to_user, client)
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
            from_user = client.get_user(transaction['from']['id'])
            update_user(from_user, client)
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
    except (IntegrityError, FlushError):
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


def update_exchange(client, account_id, exchange):
    new_record = Exchanges()
    new_record.document = json.loads(str(exchange))
    new_record.account_id = account_id
    timestamps = ['created_at', 'updated_at']
    if 'payout_at' in exchange:
        timestamps += ['payout_at']
    for timestamp in timestamps:
        setattr(new_record, timestamp, parse(exchange.pop(timestamp)).astimezone(tzlocal()))
    new_record.exchange_type = exchange.pop('resource')
    if exchange['transaction']:
        new_record.transaction_id = exchange['transaction']['id']
    new_record.payment_method_id = exchange['payment_method']['id']
    try:
        session.query(PaymentMethods).filter(PaymentMethods.id == exchange['payment_method']['id']).one()
    except NoResultFound:
        try:
            payment_method = client.get_payment_method(exchange['payment_method']['id'])
            update_payment_method(client, payment_method)
        except NotFoundError:
            new_payment_method = PaymentMethods()
            new_payment_method.id = exchange['payment_method']['id']
            session.add(new_payment_method)
            session.commit()
    new_record.amount = exchange['amount']['amount']
    new_record.amount_currency = exchange['amount']['currency']
    if 'total' in exchange:
        new_record.total = exchange['total']['amount']
        new_record.total_currency = exchange['total']['currency']
        del exchange['total']
    new_record.subtotal = exchange['subtotal']['amount']
    new_record.subtotal_currency = exchange['subtotal']['currency']
    fees = exchange.pop('fees')
    for key in ['resource_path', 'transaction', 'payment_method', 'amount', 'subtotal']:
        del exchange[key]
    for key in exchange:
        if hasattr(new_record, key):
            if isinstance(exchange[key], dict):
                setattr(new_record, key, json.loads(str(exchange[key])))
            else:
                setattr(new_record, key, exchange[key])
        else:
            db_logger.error('{0} is missing from Exchanges table, see {1}'.format(key, exchange['id']))
            print(exchange)
            print(key)
            raise Exception
    session.add(new_record)
    try:
        session.commit()
    except IntegrityError:
        session.rollback()
        existing_exchange = session.query(Exchanges).filter(Exchanges.id == new_record.id).one()
        for column in inspect(Exchanges).attrs:
            cbtools_version = getattr(existing_exchange, column.key)
            service_version = getattr(new_record, column.key)
            is_dict = isinstance(cbtools_version, dict) and isinstance(service_version, dict)
            if is_dict and cbtools_version != service_version:
                added, removed, modified, same = dict_compare(service_version, cbtools_version)
                if not added and not removed and not modified:
                    continue
                else:
                    new_exception = ReconciliationExceptions()
                    new_exception.table_name = 'Exchanges'
                    new_exception.record_id = existing_exchange.id
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
                new_exception.table_name = 'Exchanges'
                new_exception.record_id = existing_exchange.id
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
        db_logger.error('Add Exchange ProgrammingError')
        print(pformat(exchange))
        raise

    for fee in fees:
        update_fee(client, exchange['id'], fee)


def update_fee(client, source_id, fee):
    new_record = Fees()
    new_record.document = json.loads(str(fee))
    new_record.source_id = source_id
    new_record.fee_type = fee['type']
    new_record.amount = fee['amount']['amount']
    new_record.amount_currency = fee['amount']['currency']
    session.add(new_record)
    try:
        session.commit()
    except IntegrityError:
        session.rollback()
        existing_fee = session.query(Fees).filter(Fees.source_id == new_record.source_id,
                                                  Fees.fee_type == new_record.fee_type).one()
        for column in inspect(Fees).attrs:
            if column.key == 'id':
                continue
            cbtools_version = getattr(existing_fee, column.key)
            service_version = getattr(new_record, column.key)
            is_dict = isinstance(cbtools_version, dict) and isinstance(service_version, dict)
            if is_dict and cbtools_version != service_version:
                added, removed, modified, same = dict_compare(service_version, cbtools_version)
                if not added and not removed and not modified:
                    continue
                else:
                    new_exception = ReconciliationExceptions()
                    new_exception.table_name = 'Fees'
                    new_exception.record_id = existing_fee.id
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
                new_exception.table_name = 'Fees'
                new_exception.record_id = existing_fee.id
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
        db_logger.error('Add Fee ProgrammingError')
        print(pformat(fee))
        raise


def update_payment_method(client, payment_method):
    new_record = PaymentMethods()
    new_record.method_type = payment_method.pop('type')
    new_record.document = json.loads(str(payment_method))
    timestamps = ['created_at', 'updated_at']
    for timestamp in timestamps:
        setattr(new_record, timestamp, parse(payment_method.pop(timestamp)).astimezone(tzlocal()))
    if 'fiat_account' in payment_method:
        new_record.fiat_account_id = payment_method['fiat_account']['id']
        del payment_method['fiat_account']
    if 'limits' in payment_method:
        limits = payment_method.pop('limits')
    else:
        limits = None
    for key in ['resource_path', 'resource']:
        del payment_method[key]
    for key in payment_method:
        if hasattr(new_record, key):
            if isinstance(payment_method[key], dict):
                setattr(new_record, key, json.loads(str(payment_method[key])))
            else:
                setattr(new_record, key, payment_method[key])
        else:
            db_logger.error('{0} is missing from Payment Methods table, see {1}'.format(key, payment_method['id']))
            print(payment_method)
            print(key)
            raise Exception
    session.add(new_record)
    try:
        session.commit()
    except IntegrityError:
        session.rollback()
        existing_payment_method = session.query(PaymentMethods).filter(PaymentMethods.id == new_record.id).one()
        for column in inspect(PaymentMethods).attrs:
            cbtools_version = getattr(existing_payment_method, column.key)
            service_version = getattr(new_record, column.key)
            is_dict = isinstance(cbtools_version, dict) and isinstance(service_version, dict)
            if is_dict and cbtools_version != service_version:
                added, removed, modified, same = dict_compare(service_version, cbtools_version)
                if not added and not removed and not modified:
                    continue
                else:
                    new_exception = ReconciliationExceptions()
                    new_exception.table_name = 'PaymentMethods'
                    new_exception.record_id = existing_payment_method.id
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
                new_exception.table_name = 'Payments'
                new_exception.record_id = existing_payment_method.id
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
        db_logger.error('Add Payment Method ProgrammingError')
        print(pformat(payment_method))
        raise
    if limits:
        update_limits(client, payment_method['id'], limits)


def update_limits(client, payment_method_id, limits):
    for limit_type in limits:
        for limit in limits[limit_type]:
            new_record = Limits()
            new_record.payment_method_id = payment_method_id
            new_record.limit_type = limit_type
            new_record.remaining = limit['remaining']['amount']
            new_record.remaining_currency = limit['remaining']['currency']
            del limit['remaining']
            new_record.total = limit['total']['amount']
            new_record.total_currency = limit['total']['currency']
            del limit['total']
            for key in limit:
                if hasattr(new_record, key):
                    if isinstance(limit[key], dict):
                        setattr(new_record, key, json.loads(str(limit[key])))
                    else:
                        setattr(new_record, key, limit[key])
                else:
                    db_logger.error('{0} is missing from Limits table, see {1}'.format(key, payment_method_id))
                    print(limit)
                    print(key)
                    raise Exception
            session.add(new_record)
            try:
                session.commit()
            except IntegrityError:
                session.rollback()
                existing_limit = (session.query(Limits)
                                  .filter(Limits.payment_method_id == new_record.payment_method_id,
                                          Limits.limit_type == new_record.limit_type,
                                          Limits.period_in_days == new_record.period_in_days).one())
                for column in inspect(Limits).attrs:
                    if column.key == 'id':
                        continue
                    cbtools_version = getattr(existing_limit, column.key)
                    service_version = getattr(new_record, column.key)
                    is_dict = isinstance(cbtools_version, dict) and isinstance(service_version, dict)
                    if is_dict and cbtools_version != service_version:
                        added, removed, modified, same = dict_compare(service_version, cbtools_version)
                        if not added and not removed and not modified:
                            continue
                        else:
                            new_exception = ReconciliationExceptions()
                            new_exception.table_name = 'Limits'
                            new_exception.record_id = existing_limit.id
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
                        new_exception.table_name = 'Limits'
                        new_exception.record_id = existing_limit.id
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
                db_logger.error('Add Limit ProgrammingError')
                print(pformat(limit))
                raise


def update_entry(entry, account_id):
    new_record = Entries()
    new_record.account_id = account_id
    if 'order_id' in entry['details']:
        new_record.order_id = entry['details']['order_id']
        new_record.product_id = entry['details']['product_id']
        new_record.trade_id = entry['details']['trade_id']
    elif 'transfer_id' in entry['details']:
        new_record.transfer_id = entry['details']['transfer_id']
        new_record.transfer_type = entry['details']['transfer_type']
    del entry['details']
    new_record.entry_type = entry.pop('type')
    new_record.created_at = parse(entry.pop('created_at')).astimezone(tzlocal())

    for key in entry:
        if hasattr(new_record, key):
            if isinstance(entry[key], dict):
                setattr(new_record, key, json.loads(str(entry[key])))
            else:
                setattr(new_record, key, entry[key])
        else:
            db_logger.error('{0} is missing from Entries table, see {1}'.format(key, entry['id']))
            print(entry)
            print(key)
            raise Exception
    session.add(new_record)
    try:
        session.commit()
    except (IntegrityError, FlushError):
        session.rollback()
        existing_entry = (session.query(Entries).filter(Entries.id == new_record.id,).one())
        for column in inspect(Entries).attrs:
            cbtools_version = getattr(existing_entry, column.key)
            service_version = getattr(new_record, column.key)
            is_dict = isinstance(cbtools_version, dict) and isinstance(service_version, dict)
            if is_dict and cbtools_version != service_version:
                added, removed, modified, same = dict_compare(service_version, cbtools_version)
                if not added and not removed and not modified:
                    continue
                else:
                    new_exception = ReconciliationExceptions()
                    new_exception.table_name = 'Entries'
                    new_exception.record_id = existing_entry.id
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
                new_exception.table_name = 'Entries'
                new_exception.record_id = existing_entry.id
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
        db_logger.error('Add Entry ProgrammingError')
        print(pformat(entry))
        raise
