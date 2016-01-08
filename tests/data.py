import json
import os

from coinbase.wallet.client import Client

from config import SANDBOX_KEY, SANDBOX_SECRET


def download_data(target_directory):
    sandbox_wallet_client = Client(SANDBOX_KEY, SANDBOX_SECRET,
                                   base_api_uri='https://api.sandbox.coinbase.com')

    for end_point, function in [('current_user', 'get_current_user'),
                                ('wallet_accounts', 'get_accounts'),
                                ('payment_methods', 'get_payment_methods')]:
        local_json_file = os.path.join(target_directory, '{0}.json'.format(end_point))
        if not os.path.exists(local_json_file):
            response = getattr(sandbox_wallet_client, function)()
            with open(local_json_file, 'w') as json_file:
                json.dump(response.json_doc, json_file, indent=4, sort_keys=True)


if __name__ == '__main__':
    tests_directory = os.path.dirname(os.path.realpath(__file__))
    data_directory = os.path.join(tests_directory, 'data')
    if not os.path.exists(data_directory):
        os.mkdir(data_directory)
    download_data(data_directory)
