import logging
import traceback

from sqlalchemy import (create_engine, func, UniqueConstraint, Boolean,
                        Column, DateTime, Integer, Numeric, String)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from config import URI


engine = create_engine(URI)

session = scoped_session(sessionmaker(autocommit=True, autoflush=True, bind=engine))
Base = declarative_base()
Base.query = session.query_property()


class ReconciliationExceptions(Base):
    __tablename__ = 'reconciliation_exceptions'
    __table_args__ = (UniqueConstraint('record_id', 'column_name', name='rec_exception_constraint'),)

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

    id = Column(String, primary_key=True)
    name = Column(String)
    primary = Column(Boolean)
    account_type = Column(String)
    currency = Column(String)
    balance = Column(Numeric)
    balance_currency = Column(String)
    native_balance = Column(Numeric)
    native_balance_currency = Column(String)
    created_at = Column(DateTime(timezone=True))
    updated_at = Column(DateTime(timezone=True))

    document = Column(JSONB)


class Addresses(Base):
    __tablename__ = 'addresses'

    id = Column(String, primary_key=True)
    account_id = Column(String)
    address = Column(String)
    name = Column(String)
    created_at = Column(DateTime(timezone=True))
    updated_at = Column(DateTime(timezone=True))

    document = Column(JSONB)


class Transactions(Base):
    __tablename__ = 'transactions'

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
    from_resource = Column(String)
    from_address = Column(String)
    address = Column(String)
    application = Column(String)

    document = Column(JSONB)


class Exchanges(Base):
    __tablename__ = 'exchanges'

    id = Column(String, primary_key=True)
    exchange_type = Column(String)
    status = Column(String)
    payment_method_id = Column(String)
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

    id = Column(Integer, primary_key=True)
    source_id = Column(String)
    fee_type = Column(String)
    amount = Column(Numeric)
    currency = Column(String)


class PaymentMethods(Base):
    __tablename__ = 'payment_methods'

    id = Column(String, primary_key=True)
    method_type = Column(String)
    name = Column(String)
    currency = Column(String)
    primary_buy = Column(Boolean)
    primary_sell = Column(Boolean)
    allow_buy = Column(Boolean)
    allow_sell = Column(Boolean)
    created_at = Column(DateTime(timezone=True))
    updated_at = Column(DateTime(timezone=True))

    document = Column(JSONB)


class Log(Base):
    __tablename__ = 'logs'
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
        return "<Log: %s - %s>" % (self.created_at.strftime('%m/%d/%Y-%H:%M:%S'), self.msg[:50])


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


if __name__ == "__main__":
    # Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
