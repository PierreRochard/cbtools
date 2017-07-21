import argparse
import logging
import traceback

from sqlalchemy import create_engine, func, UniqueConstraint, Boolean
from sqlalchemy import Column, DateTime, Integer, Numeric, String, ForeignKey
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from config import URI

engine = create_engine(URI)

session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
session.execute("CREATE SCHEMA IF NOT EXISTS cbtools;")
Base = declarative_base()
Base.query = session.query_property()


class Accounts(Base):
    __tablename__ = 'accounts'
    __table_args__ = {"schema": "cbtools"}

    balance_amount = Column(Numeric)
    balance_currency = Column(String)
    created_at = Column(DateTime(timezone=True))
    currency = Column(String)
    id = Column(String, primary_key=True)
    name = Column(String)
    native_balance_amount = Column(Numeric)
    native_balance_currency = Column(String)
    primary = Column(Boolean)
    resource = Column(String)
    resource_path = Column(String)
    type = Column(String)
    updated_at = Column(DateTime(timezone=True))


class Addresses(Base):
    __tablename__ = 'addresses'
    __table_args__ = {"schema": "cbtools"}

    account_id = Column(String, ForeignKey('cbtools.accounts.id'))
    address = Column(String)
    callback_url = Column(String)
    created_at = Column(DateTime(timezone=True))
    id = Column(String, primary_key=True)
    name = Column(String)
    resource = Column(String)
    resource_path = Column(String)
    updated_at = Column(DateTime(timezone=True))


class Exchanges(Base):
    __tablename__ = 'exchanges'
    __table_args__ = {"schema": "cbtools"}

    account_id = Column(String, ForeignKey('cbtools.accounts.id'))
    amount = Column(Numeric)
    amount_currency = Column(String)
    committed = Column(Boolean)
    created_at = Column(DateTime(timezone=True))
    id = Column(String, primary_key=True)
    instant = Column(Boolean)
    # payment_method_id = Column(String, ForeignKey('cbtools.payment_methods.id'))
    payment_method_id = Column(String)
    payment_method_resource = Column(String)
    payment_method_resource_path = Column(String)
    payout_at = Column(DateTime(timezone=True))
    resource = Column(String)
    resource_path = Column(String)
    status = Column(String)
    subtotal = Column(Numeric)
    subtotal_currency = Column(String)
    total = Column(Numeric)
    total_currency = Column(String)
    transaction = Column(String)
    transaction_id = Column(String)
    transaction_resource = Column(String)
    transaction_resource_path = Column(String)
    updated_at = Column(DateTime(timezone=True))


class Fees(Base):
    __tablename__ = 'fees'
    __table_args__ = (UniqueConstraint('source_id', 'fee_type', name='fees_unique_constraint'),
                      {"schema": "cbtools"})

    id = Column(Integer, primary_key=True)
    source_id = Column(String, ForeignKey('cbtools.exchanges.id'))
    fee_type = Column(String)
    amount = Column(Numeric)
    currency = Column(String)


class Limits(Base):
    __tablename__ = 'limits'
    __table_args__ = (UniqueConstraint('payment_method_id', 'type', 'period_in_days',
                                       name='limits_unique_constraint'),
                      {"schema": "cbtools"})
    id = Column(Integer, primary_key=True)

    payment_method_id = Column(String, ForeignKey('cbtools.payment_methods.id'))
    period_in_days = Column(Integer)
    remaining = Column(Numeric)
    remaining_currency = Column(String)
    resource = Column(String)
    total = Column(Numeric)
    total_currency = Column(String)
    type = Column(String)


# class Mispayments(Base):
#     pass


class Orders(Base):
    __tablename__ = 'orders'
    __table_args__ = {"schema": "cbtools"}

    id = Column(String, primary_key=True)
    code = Column(String)
    status = Column(String)
    order_type = Column(String)
    name = Column(String)
    description = Column(String)
    amount = Column(Numeric)
    amount_currency = Column(String)
    payout_amount = Column(Numeric)
    payout_amount_currency = Column(String)
    bitcoin_address = Column(String)
    bitcoin_amount = Column(Numeric)
    bitcoin_amount_currency = Column(String)
    bitcoin_uri = Column(String)
    receipt_url = Column(String)
    expires_at = Column(DateTime(timezone=True))
    mispaid_at = Column(DateTime(timezone=True))
    paid_at = Column(DateTime(timezone=True))
    refund_address = Column(String)
    transaction_id = Column(String, ForeignKey('cbtools.transactions.id'))


class PaymentMethods(Base):
    __tablename__ = 'payment_methods'
    __table_args__ = {"schema": "cbtools"}

    allow_buy = Column(Boolean)
    allow_deposit = Column(Boolean)
    allow_sell = Column(Boolean)
    allow_withdraw = Column(Boolean)
    created_at = Column(DateTime(timezone=True))
    currency = Column(String)
    fiat_account_id = Column(String, ForeignKey('cbtools.accounts.id'))
    fiat_account_resource = Column(String)
    fiat_account_resource_path = Column(String)
    id = Column(String, primary_key=True)
    name = Column(String)
    primary_buy = Column(Boolean)
    primary_sell = Column(Boolean)
    resource = Column(String)
    resource_path = Column(String)
    type = Column(String)
    updated_at = Column(DateTime(timezone=True))


# class Refunds(Base):
#     pass


class Transactions(Base):
    __tablename__ = 'transactions'
    __table_args__ = {"schema": "cbtools"}

    account_id = Column(String, ForeignKey('cbtools.accounts.id'))
    address_id = Column(String, ForeignKey('cbtools.addresses.id'))
    address_resource = Column(String)
    address_resource_path = Column(String)
    amount = Column(Numeric)
    amount_currency = Column(String)
    application_id = Column(String)
    application_resource = Column(String)
    buy_id = Column(String, ForeignKey('cbtools.exchanges.id'))
    buy_resource = Column(String)
    buy_resource_path = Column(String)
    created_at = Column(DateTime(timezone=True))
    description = Column(String)
    fiat_deposit_id = Column(String)
    fiat_deposit_resource = Column(String)
    fiat_deposit_resource_path = Column(String)
    fiat_withdrawal_id = Column(String)
    fiat_withdrawal_resource = Column(String)
    fiat_withdrawal_resource_path = Column(String)
    from_id = Column(String)
    from_resource = Column(String)
    from_resource_path = Column(String)
    id = Column(String, primary_key=True)
    idem = Column(String)
    instant_exchange = Column(Boolean)
    native_amount = Column(Numeric)
    native_amount_currency = Column(String)
    network_hash = Column(String)
    network_status = Column(String)
    order_id = Column(String)
    order_resource = Column(String)
    order_resource_path = Column(String)
    resource = Column(String)
    resource_path = Column(String)
    sell_id = Column(String)
    sell_resource = Column(String)
    sell_resource_path = Column(String)
    status = Column(String)
    to_address = Column(String)
    to_email = Column(String)
    to_id = Column(String)
    to_resource = Column(String)
    to_resource_path = Column(String)
    type = Column(String)
    updated_at = Column(DateTime(timezone=True))


class Users(Base):
    __tablename__ = 'users'
    __table_args__ = {"schema": "cbtools"}

    avatar_url = Column(String)
    bitcoin_unit = Column(String)
    country_code = Column(String)
    country_name = Column(String)
    created_at = Column(DateTime(timezone=True))
    email = Column(String)
    id = Column(String, primary_key=True)
    name = Column(String)
    native_currency = Column(String)
    profile_bio = Column(String)
    profile_location = Column(String)
    profile_url = Column(String)
    resource = Column(String)
    resource_path = Column(String)
    time_zone = Column(String)
    username = Column(String)

    # TODO add show authorization information


class ExchangeAccounts(Base):
    __tablename__ = 'exchange_accounts'
    __table_args__ = {"schema": "cbtools"}

    available = Column(Numeric)
    balance = Column(Numeric)
    currency = Column(String)
    hold = Column(Numeric)
    id = Column(String, primary_key=True)
    profile_id = Column(String)
    resource = Column(String)


class Fills(Base):
    __tablename__ = 'fills'
    __table_args__ = (UniqueConstraint('trade_id', 'created_at', 'account_id', name='fills_constraint'), {'schema': 'cbtools'})
    id = Column(Integer, primary_key=True)
    account_id = Column(String)
    created_at = Column(DateTime(timezone=True))
    fee = Column(Numeric)
    liquidity = Column(String)
    order_id = Column(String)
    price = Column(Numeric)
    product_id = Column(String)
    profile_id = Column(String)
    resource = Column(String)
    settled = Column(Boolean)
    side = Column(String)
    size = Column(Numeric)
    trade_id = Column(Integer)
    user_id = Column(String)


class Holds(Base):
    __tablename__ = 'holds'
    __table_args__ = {'schema': 'cbtools'}

    account_id = Column(String, ForeignKey('cbtools.exchange_accounts.id'))
    amount = Column(Numeric)
    created_at = Column(DateTime(timezone=True))
    id = Column(String, primary_key=True)
    ref = Column(String)
    resource = Column(String)
    type = Column(String)


class Entries(Base):
    __tablename__ = 'entries'
    __table_args__ = {'schema': 'cbtools'}

    account_id = Column(String, ForeignKey('cbtools.exchange_accounts.id'))
    amount = Column(Numeric)
    balance = Column(Numeric)
    created_at = Column(DateTime(timezone=True))
    entry_type = Column(String)
    id = Column(Numeric, primary_key=True)
    order_id = Column(String)
    product_id = Column(String)
    trade_id = Column(Integer)
    transfer_id = Column(String)
    transfer_type = Column(String)


class ExchangeOrders(Base):
    __tablename__ = 'exchange_orders'
    __table_args__ = {'schema': 'cbtools'}

    account_id = Column(String, ForeignKey('cbtools.exchange_accounts.id'))
    created_at = Column(DateTime(timezone=True))
    done_at = Column(DateTime(timezone=True))
    done_reason = Column(String)
    fill_fees = Column(Numeric)
    filled_size = Column(Numeric)
    funds = Column(Numeric)
    id = Column(String, primary_key=True)
    post_only = Column(Boolean)
    price = Column(Numeric)
    product_id = Column(String)
    resource = Column(String)
    reject_reason = Column(String)
    settled = Column(Boolean)
    side = Column(String)
    size = Column(Numeric)
    status = Column(String)
    stp = Column(String)
    time_in_force = Column(String)
    type = Column(String)


class ReconciliationExceptions(Base):
    __tablename__ = 'reconciliation_exceptions'
    __table_args__ = (UniqueConstraint('record_id', 'column_name', name='rec_exception_constraint'),
                      {"schema": "cbtools"})

    id = Column(Integer, primary_key=True)
    table_name = Column(String)
    record_id = Column(String)
    column_name = Column(String)
    old_version = Column(String)
    new_version = Column(String)
    old_version_type = Column(String)
    new_version_type = Column(String)
    resolved = Column(Boolean, default=False)
    resolution = Column(String, default=None)

    timestamp = Column(DateTime(timezone=True), default=func.now())
    resolved_timestamp = Column(DateTime(timezone=True))


class Log(Base):
    __tablename__ = 'logs'
    __table_args__ = {'schema': 'cbtools'}

    id = Column(Integer, primary_key=True)
    logger = Column(String)
    level = Column(String)
    trace = Column(String)
    msg = Column(String)
    request_remote_address = Column(String)
    user_agent = Column(String)
    platform = Column(String)
    browser = Column(String)
    version = Column(String)
    language = Column(String)
    created_at = Column(DateTime(timezone=True), default=func.now().op('AT TIME ZONE')('UTC'))

    def __unicode__(self):
        return self.__repr__()

    def __repr__(self):
        return '<Log: %s - %s>' % (self.created_at.strftime('%m/%d/%Y-%H:%M:%S'), self.msg[:50])


class SQLAlchemyLogHandler(logging.Handler):
    def emit(self, record):
        log = Log()
        log.logger = record.__dict__['name']
        log.level = record.__dict__['levelname']
        exc = record.__dict__['exc_info']
        if exc:
            log.trace = traceback.format_exc(exc)
        else:
            log.trace = None
        log.msg = record.__dict__['msg']

        session.add(log)
        session.commit()


if __name__ == '__main__':
    ARGS = argparse.ArgumentParser(description='Coinbase Exchange bot.')
    ARGS.add_argument('--d', action='store_true', dest='drop_tables', default=False, help='Drop tables')
    args = ARGS.parse_args()

    if args.drop_tables:
        Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
