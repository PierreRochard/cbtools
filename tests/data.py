import json
import os

from coinbase.wallet.client import Client

from config import SANDBOX_KEY, SANDBOX_SECRET


sandbox_wallet_client = Client(SANDBOX_KEY, SANDBOX_SECRET, base_api_uri='https://api.sandbox.coinbase.com')


def download_primary_end_points_json(target_directory):
    primary_end_points = [('current_user', 'get_current_user'),
                      ('payment_methods', 'get_payment_methods'),
                      ('wallet_accounts', 'get_accounts')]
    for primary_end_point, primary_function in primary_end_points:
        local_json_file = os.path.join(target_directory, '{0}.json'.format(primary_end_point))
        response = getattr(sandbox_wallet_client, primary_function)()
        with open(local_json_file, 'w') as json_file:
            json.dump(response.json_doc, json_file, indent=4, sort_keys=True)
        if primary_end_point == 'wallet_accounts':
            wallet_ids = [wallet['id'] for wallet in response['data']]
            [download_secondary_end_points_json(target_directory, account_id) for account_id in wallet_ids]


def download_secondary_end_points_json(target_directory, account_id):
    secondary_end_points = [('buys', 'get_buys'),
                            ('sells', 'get_sells'),
                            ('deposits', 'get_deposits'),
                            ('withdrawals', 'get_withdrawals'),
                            ('addresses', 'get_addresses'),
                            ('transactions', 'get_transactions')]

    for secondary_end_point, secondary_function in secondary_end_points:
        response = getattr(sandbox_wallet_client, secondary_function)(account_id)
        while response.pagination['next_uri']:
            first_id = response.json_doc['data'][0]['id']
            last_id = response.json_doc['data'][-1]['id']
            starting_after = response.pagination['next_uri'].split('=')[-1]
            local_json_file = os.path.join(target_directory, '{0} from {1} to {2}.json'.format(secondary_end_point,
                                                                                               first_id, last_id))
            with open(local_json_file, 'w') as json_file:
                json.dump(response.json_doc, json_file, indent=4, sort_keys=True)
            response = getattr(sandbox_wallet_client, secondary_function)(account_id, starting_after=starting_after)
        else:
            if response.json_doc['data']:
                first_id = response.json_doc['data'][0]['id']
                last_id = response.json_doc['data'][-1]['id']
                local_json_file = os.path.join(target_directory, '{0} from {1} to {2}.json'.format(secondary_end_point,
                                                                                                   first_id, last_id))
                with open(local_json_file, 'w') as json_file:
                    json.dump(response.json_doc, json_file, indent=4, sort_keys=True)


if __name__ == '__main__':
    tests_directory = os.path.dirname(os.path.realpath(__file__))
    data_directory = os.path.join(tests_directory, 'data')
    if not os.path.exists(data_directory):
        os.mkdir(data_directory)
    for old_json_file in os.listdir(data_directory):
        json_file_path = os.path.join(data_directory, old_json_file)
        os.remove(json_file_path)

    download_primary_end_points_json(data_directory)
