from pprint import pformat

import requests
from decimal import Decimal

from cbtools.utilities import CoinbaseExchangeAuthentication
from config import GDAX_API_KEY, GDAX_API_PASSPHRASE, GDAX_API_SECRET

if __name__ == '__main__':
    exchange_api_url = 'https://api.gdax.com/'
    exchange_auth = CoinbaseExchangeAuthentication(GDAX_API_KEY, GDAX_API_SECRET,
                                                       GDAX_API_PASSPHRASE)

    # requests.delete(exchange_api_url + 'orders', auth=exchange_auth)
    exchange_accounts = requests.get(exchange_api_url + 'accounts', auth=exchange_auth).json()
    for exchange_account in exchange_accounts:
        if exchange_account['currency'] != 'BTC':
            print(pformat(exchange_account))
            ticker = requests.get(exchange_api_url + 'products/BTC-USD/ticker').json()
            print(pformat(ticker))
            best_bid = Decimal(ticker['bid'])
            smallest_increment = Decimal('0.01') * best_bid
            balance_available = Decimal(exchange_account['available'])
            number_of_orders = int(balance_available/smallest_increment)
            print(number_of_orders)
            our_bid = best_bid - Decimal('0.50')
            for order_number in range(0, number_of_orders):
                order = {'size': '0.01',
                         'price': str(our_bid),
                         'side': 'buy',
                         'product_id': 'BTC-USD',
                         'post_only': True}
                requests.post(exchange_api_url + 'orders', json=order, auth=exchange_auth)
                our_bid -= Decimal('0.50')