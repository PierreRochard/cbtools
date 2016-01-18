import time
import hmac
import hashlib
import base64

from requests.auth import AuthBase


def denest_json(json_document, account_id=None, resource=None):
    if 'data' in json_document:
        json_document = json_document['data']
    if not isinstance(json_document, list):
        json_documents = [json_document]
    else:
        json_documents = json_document
    new_json_documents = []
    for json_document in json_documents:
        new_json_document = {}
        for key, value in json_document.items():
            if key == 'json_doc':
                continue
            elif isinstance(value, dict):
                for nested_key, nested_value in value.items():
                    if 'json_doc' in nested_key:
                        continue
                    elif isinstance(nested_value, list) and key == 'limits':
                        for limit in nested_value:
                            new_limit = {}
                            for limit_key, limit_value in limit.items():
                                if limit_key == 'json_doc':
                                    continue
                                elif isinstance(limit_value, dict):
                                    for nested_limit_key, nested_limit_value in limit_value.items():
                                        if nested_limit_key == 'json_doc':
                                            continue
                                        elif nested_limit_key == 'amount':
                                            new_limit[limit_key] = nested_limit_value
                                        else:
                                            new_key = limit_key + '_' + nested_limit_key
                                            new_limit[new_key] = nested_limit_value
                                else:
                                    new_limit[limit_key] = limit_value
                                new_limit['resource'] = 'limit'
                                new_limit['type'] = nested_key
                                new_limit['payment_method_id'] = json_document['id']
                            new_json_documents += [new_limit]
                    elif isinstance(nested_value, str) or isinstance(nested_value, bool) or nested_value is None:
                        if nested_key == 'amount':
                            new_json_document[key] = nested_value
                        else:
                            new_key = key + '_' + nested_key
                            new_json_document[new_key] = nested_value
                    else:
                        print(type(nested_value))
                        raise Exception()
            elif isinstance(value, list) and json_document['resource'] in ['buy', 'sell', 'deposit', 'withdrawal']:
                for fee in value:
                    new_fee = {}
                    for fee_key, fee_value in fee.items():
                        if isinstance(fee_value, dict):
                            for nested_fee_key, nested_fee_value in fee_value.items():
                                if nested_fee_key == 'amount':
                                    new_fee[fee_key] = nested_fee_value
                                else:
                                    new_key = fee_key + '_' + nested_fee_key
                                    new_fee[new_key] = nested_fee_value
                        elif isinstance(fee_value, str) or isinstance(fee_value, bool) or fee_value is None:
                            new_fee[fee_key] = fee_value
                        else:
                            print(type(fee_value))
                            raise Exception()
                    new_fee['resource'] = 'fee'
                    new_fee['source_id'] = json_document['id']
                    new_json_documents += [new_fee]
            elif isinstance(value, str) or isinstance(value, bool) or isinstance(value, int) or value is None:
                new_json_document[key] = json_document[key]
            else:
                print(pformat(json_document))
                print(type(value))
                raise Exception()
        if resource:
            new_json_document['resource'] = resource
        if account_id:
            new_json_document['account_id'] = account_id
        new_json_documents += [new_json_document]
    if new_json_documents:
        return new_json_documents
    else:
        return []


class CoinbaseExchangeAuthentication(AuthBase):
    def __init__(self, api_key, secret_key, passphrase):
        self.api_key = api_key
        self.secret_key = secret_key
        self.passphrase = passphrase

    def __call__(self, request):
        timestamp = str(time.time())
        message = timestamp + request.method + request.path_url + (request.body or '')
        message = message.encode('utf-8')
        hmac_key = base64.b64decode(self.secret_key)
        signature = hmac.new(hmac_key, message, hashlib.sha256)
        signature_b64 = base64.b64encode(signature.digest())

        request.headers.update({
            'CB-ACCESS-SIGN': signature_b64,
            'CB-ACCESS-TIMESTAMP': timestamp,
            'CB-ACCESS-KEY': self.api_key,
            'CB-ACCESS-PASSPHRASE': self.passphrase,
        })
        return request

