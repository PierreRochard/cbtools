import logging
import traceback
from sqlalchemy import create_engine, func, UniqueConstraint, Boolean
from sqlalchemy.exc import DatabaseError
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, DateTime, Integer, Numeric, String

from config import DEV_URI


engine = create_engine(DEV_URI)

session = scoped_session(sessionmaker(autocommit=True, autoflush=True, bind=engine))
Base = declarative_base()
Base.query = session.query_property()


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


class Addresses(Base):
    __tablename__ = 'addresses'

    id = Column(String, primary_key=True)
    address = Column(String)
    name = Column(String)
    created_at = Column(DateTime(timezone=True))
    updated_at = Column(DateTime(timezone=True))


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


if __name__ == "__main__":
    # Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
