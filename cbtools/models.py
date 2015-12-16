import argparse
import logging
import traceback

from sqlalchemy import (create_engine, func, UniqueConstraint, Boolean,
                        Column, DateTime, Integer, Numeric, String, ForeignKey)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import scoped_session, sessionmaker, relationship, backref
from sqlalchemy.ext.declarative import declarative_base

from config import URI


engine = create_engine(URI)

session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))
Base = declarative_base()
Base.query = session.query_property()


class ReconciliationExceptions(Base):
    __tablename__ = 'reconciliation_exceptions'
    __table_args__ = (UniqueConstraint('record_id', 'column_name', name='rec_exception_constraint'),
                      {"schema": "cbtools"})

    id = Column(Integer, primary_key=True)
    table_name = Column(String)
    record_id = Column(String)
    column_name = Column(String)
    cbtools_version = Column(String)
    coinbase_version = Column(String)
    json_doc = Column(Boolean)
    resolved = Column(Boolean, default=False)
    resolution = Column(String, default=None)

    timestamp = Column(DateTime(timezone=True), default=func.now())
    resolved_timestamp = Column(DateTime(timezone=True))


class Users(Base):
    __tablename__ = 'users'
    __table_args__ = {"schema": "cbtools"}

    id = Column(String, primary_key=True)
    name = Column(String)
    username = Column(String)
    profile_location = Column(String)
    profile_bio = Column(String)
    profile_url = Column(String)
    avatar_url = Column(String)
    time_zone = Column(String)
    native_currency = Column(String)
    bitcoin_unit = Column(String)
    country_code = Column(String)
    country_name = Column(String)
    created_at = Column(DateTime(timezone=True))
    email = Column(String)

    document = Column(JSONB)

    #TODO add show authorization information


class Accounts(Base):
    __tablename__ = 'accounts'
    __table_args__ = {"schema": "cbtools"}

    id = Column(String, primary_key=True)
    name = Column(String)
    primary = Column(Boolean)
    account_type = Column(String)
    currency = Column(String)
    balance = Column(Numeric)
    balance_currency = Column(String)
    native_balance = Column(Numeric)
    native_balance_currency = Column(String)
    hold = Column(Numeric)
    hold_currency = Column(String)
    available = Column(Numeric)
    available_currency = Column(String)
    created_at = Column(DateTime(timezone=True))
    updated_at = Column(DateTime(timezone=True))
    profile_id = Column(String)
    user_id = Column(String, ForeignKey('cbtools.users.id'))
    user = relationship("Users", backref=backref('accounts', order_by=id))

    document = Column(JSONB)


class Addresses(Base):
    __tablename__ = 'addresses'
    __table_args__ = {"schema": "cbtools"}

    id = Column(String, primary_key=True)
    account_id = Column(String, ForeignKey('cbtools.accounts.id'))
    account = relationship('Accounts', backref=backref('addresses', order_by=id))
    address = Column(String)
    name = Column(String)
    created_at = Column(DateTime(timezone=True))
    updated_at = Column(DateTime(timezone=True))

    document = Column(JSONB)


class Transactions(Base):
    __tablename__ = 'transactions'
    __table_args__ = {"schema": "cbtools"}

    id = Column(String, primary_key=True)
    transaction_type = Column(String)
    status = Column(String)
    amount = Column(Numeric)
    amount_currency = Column(String)
    native_amount = Column(Numeric)
    native_amount_currency = Column(String)
    description = Column(String)
    instant_exchange = Column(Boolean)
    created_at = Column(DateTime(timezone=True))
    updated_at = Column(DateTime(timezone=True))
    network_status = Column(String)
    network_hash = Column(String)
    to_resource = Column(String)
    to_address = Column(String)
    to_user_id = Column(String, ForeignKey('cbtools.users.id'))
    to_user = relationship('Users', backref=backref('transactions_sent', order_by=id),
                           foreign_keys=[to_user_id])
    to_email = Column(String)
    from_resource = Column(String)
    from_address = Column(String)
    from_user_id = Column(String, ForeignKey('cbtools.users.id'))
    from_user = relationship('Users', backref=backref('transactions_received', order_by=id),
                             foreign_keys=[from_user_id])
    address = Column(String, ForeignKey('cbtools.addresses.id'))
    application_id = Column(String)
    order_id = Column(String)

    exchange_id = Column(String, ForeignKey('cbtools.exchanges.id'))
    exchange = relationship('Exchanges', uselist=False, backref=backref('transaction', order_by=id))

    document = Column(JSONB)


class Exchanges(Base):
    __tablename__ = 'exchanges'
    __table_args__ = {"schema": "cbtools"}

    id = Column(String, primary_key=True)
    exchange_type = Column(String)
    status = Column(String)
    payment_method_id = Column(String, ForeignKey('cbtools.payment_methods.id'))
    payment_method = relationship('PaymentMethods', backref=backref('exchanges', order_by=id))
    transaction_id = Column(String)
    amount = Column(Numeric)
    amount_currency = Column(String)
    total = Column(Numeric)
    total_currency = Column(String)
    subtotal = Column(Numeric)
    subtotal_currency = Column(String)
    created_at = Column(DateTime(timezone=True))
    updated_at = Column(DateTime(timezone=True))
    committed = Column(Boolean)
    instant = Column(Boolean)
    payout_at = Column(DateTime(timezone=True))

    document = Column(JSONB)


class Fees(Base):
    __tablename__ = 'fees'
    __table_args__ = (UniqueConstraint('source_id', 'fee_type', name='fees_unique_constraint'),
                      {"schema": "cbtools"})

    id = Column(Integer, primary_key=True)
    source_id = Column(String, ForeignKey('cbtools.exchanges.id'))
    exchange = relationship('Exchanges', backref=backref('fees', order_by=id))
    fee_type = Column(String)
    amount = Column(Numeric)
    currency = Column(String)


class PaymentMethods(Base):
    __tablename__ = 'payment_methods'
    __table_args__ = {"schema": "cbtools"}

    id = Column(String, primary_key=True)
    method_type = Column(String)
    name = Column(String)
    currency = Column(String)
    primary_buy = Column(Boolean)
    primary_sell = Column(Boolean)
    allow_buy = Column(Boolean)
    allow_sell = Column(Boolean)
    allow_deposit = Column(Boolean)
    allow_withdraw = Column(Boolean)
    created_at = Column(DateTime(timezone=True))
    updated_at = Column(DateTime(timezone=True))
    fiat_account_id = Column(String, ForeignKey('cbtools.accounts.id'))
    fiat_account = relationship('Accounts', backref=backref('payment_methods', order_by=id))

    document = Column(JSONB)

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
    transaction = relationship('Transactions', uselist=False, backref=backref('order'))

# class Refunds(Base):
#     pass
#
# class Mispayments(Base):
#     pass


class Limits(Base):
    __tablename__ = 'limits'
    __table_args__ = (UniqueConstraint('payment_method_id', 'limit_type', 'period_in_days',
                                       name='limits_unique_constraint'),
                      {"schema": "cbtools"})
    id = Column(Integer, primary_key=True)
    payment_method_id = Column(String, ForeignKey('cbtools.payment_methods.id'))
    payment_method = relationship('PaymentMethods', backref=backref('limits', order_by=id))
    limit_type = Column(String)
    period_in_days = Column(Integer)
    remaining = Column(Numeric)
    remaining_currency = Column(String)
    total = Column(Numeric)
    total_currency = Column(String)


class Fills(Base):
    __tablename__ = 'fills'
    __table_args__ = {'schema': 'cbtools'}
    trade_id = Column(Integer, primary_key=True)
    product_id = Column(String)
    price = Column(Numeric)
    size = Column(Numeric)
    order_id = Column(String)
    created_at = Column(DateTime(timezone=True))
    liquidity = Column(String)
    fee = Column(Numeric)
    settled = Column(Boolean)
    side = Column(String)


class Entries(Base):
    __tablename__ = 'entries'
    __table_args__ = (UniqueConstraint('created_at', 'order_id',
                                       name='limits_unique_constraint'), {'schema': 'cbtools'})
    id = Column(Numeric, primary_key=True)
    amount = Column(Numeric)
    balance = Column(Numeric)
    created_at = Column(DateTime(timezone=True))
    order_id = Column(String)
    product_id = Column(String)
    trade_id = Column(Integer)
    transfer_id = Column(String)
    transfer_type = Column(String)
    entry_type = Column(String)

    account_id = Column(String, ForeignKey('cbtools.accounts.id'))
    account = relationship('Accounts', backref=backref('entries', order_by=created_at))


class Holds(Base):
    __tablename__ = 'holds'
    __table_args__ = {'schema': 'cbtools'}

    id = Column(String, primary_key=True)

    created_at = Column(DateTime(timezone=True))
    updated_at = Column(DateTime(timezone=True))
    amount = Column(Numeric)
    hold_type = Column(String)
    ref = Column(String)
    account_id = Column(String, ForeignKey('cbtools.accounts.id'))
    account = relationship('Accounts', backref=backref('holds', order_by=created_at))


class OpenOrders(Base):
    __tablename__ = 'open_orders'
    __table_args__ = {'schema': 'cbtools'}

    id = Column(String, primary_key=True)
    size = Column(Numeric)
    price = Column(Numeric)
    product_id = Column(String)
    status = Column(String)
    filled_size = Column(Numeric)
    fill_fees = Column(Numeric)
    settled = Column(Boolean)
    side = Column(String)
    created_at = Column(DateTime(timezone=True))


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
